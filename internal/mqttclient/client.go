package mqttclient

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	packetCONNECT    = 1
	packetCONNACK    = 2
	packetPUBLISH    = 3
	packetPUBACK     = 4
	packetSUBSCRIBE  = 8
	packetSUBACK     = 9
	packetPINGREQ    = 12
	packetPINGRESP   = 13
	packetDISCONNECT = 14
	maxPacketSize    = 64 << 20
)

var ErrNotConnected = errors.New("MQTT client is not connected")

type Config struct {
	BrokerURL          string
	ClientID           string
	Username           string
	Password           string
	KeepAlive          time.Duration
	ConnectTimeout     time.Duration
	PublishTimeout     time.Duration
	ReconnectMin       time.Duration
	ReconnectMax       time.Duration
	WillTopic          string
	WillPayload        []byte
	WillQoS            byte
	WillRetain         bool
	CAFile             string
	CertFile           string
	KeyFile            string
	ServerName         string
	InsecureSkipVerify bool
	OnMessage          MessageHandler
}

type Message struct {
	Topic   string
	Payload []byte
	QoS     byte
	Retain  bool
}

type MessageHandler func(Message)

type ackResult struct {
	err error
}

type Client struct {
	cfg    Config
	logger *slog.Logger
	conn   net.Conn
	reader *bufio.Reader

	writeMu   sync.Mutex
	stateMu   sync.RWMutex
	pendingMu sync.Mutex
	pending   map[uint16]chan ackResult
	subMu     sync.Mutex
	subacks   map[uint16]chan ackResult
	packetID  atomic.Uint32
	connected bool
	done      chan struct{}
	closeOnce sync.Once
	cancel    context.CancelFunc
}

func Dial(ctx context.Context, cfg Config, logger *slog.Logger) (*Client, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.ClientID == "" {
		return nil, errors.New("MQTT client ID is required")
	}
	if cfg.KeepAlive <= 0 {
		cfg.KeepAlive = 30 * time.Second
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = 10 * time.Second
	}
	if cfg.PublishTimeout <= 0 {
		cfg.PublishTimeout = 15 * time.Second
	}
	u, err := parseBrokerURL(cfg.BrokerURL)
	if err != nil {
		return nil, err
	}
	dialer := &net.Dialer{Timeout: cfg.ConnectTimeout, KeepAlive: cfg.KeepAlive}
	var conn net.Conn
	if isTLSScheme(u.Scheme) {
		tlsCfg, err := buildTLSConfig(cfg, u.Hostname())
		if err != nil {
			return nil, err
		}
		conn, err = tls.DialWithDialer(dialer, "tcp", u.Host, tlsCfg)
	} else {
		conn, err = dialer.DialContext(ctx, "tcp", u.Host)
	}
	if err != nil {
		return nil, fmt.Errorf("dial MQTT broker %s: %w", u.Host, err)
	}
	_ = conn.SetDeadline(time.Now().Add(cfg.ConnectTimeout))
	if _, err := conn.Write(encodeConnect(cfg)); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("write MQTT CONNECT: %w", err)
	}
	reader := bufio.NewReader(conn)
	packetType, _, body, err := readPacket(reader)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("read MQTT CONNACK: %w", err)
	}
	if packetType != packetCONNACK {
		_ = conn.Close()
		return nil, fmt.Errorf("expected CONNACK, got packet type %d", packetType)
	}
	if err := validateConnack(body); err != nil {
		_ = conn.Close()
		return nil, err
	}
	_ = conn.SetDeadline(time.Time{})

	clientCtx, cancel := context.WithCancel(context.Background())
	client := &Client{
		cfg: cfg, logger: logger, conn: conn, reader: reader,
		pending: make(map[uint16]chan ackResult), subacks: make(map[uint16]chan ackResult), connected: true,
		done: make(chan struct{}), cancel: cancel,
	}
	go client.readLoop(clientCtx)
	go client.pingLoop(clientCtx)
	return client, nil
}

func (c *Client) Subscribe(ctx context.Context, topic string, qos byte) error {
	if !c.IsConnected() {
		return ErrNotConnected
	}
	if topic == "" || len(topic) > 65535 {
		return errors.New("invalid MQTT subscription topic")
	}
	if qos > 1 {
		return fmt.Errorf("unsupported MQTT subscription QoS %d", qos)
	}
	packetID := c.nextPacketID()
	ack := make(chan ackResult, 1)
	c.subMu.Lock()
	c.subacks[packetID] = ack
	c.subMu.Unlock()
	defer func() {
		c.subMu.Lock()
		delete(c.subacks, packetID)
		c.subMu.Unlock()
	}()
	if err := c.writePacket(encodeSubscribe(topic, qos, packetID)); err != nil {
		c.fail(err)
		return err
	}
	timer := time.NewTimer(c.cfg.PublishTimeout)
	defer timer.Stop()
	select {
	case result := <-ack:
		return result.err
	case <-timer.C:
		return errors.New("MQTT SUBACK timeout")
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return ErrNotConnected
	}
}

func (c *Client) IsConnected() bool {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	return c.connected
}

func (c *Client) Done() <-chan struct{} { return c.done }

func (c *Client) Publish(ctx context.Context, topic string, qos byte, retain bool, payload []byte) error {
	if !c.IsConnected() {
		return ErrNotConnected
	}
	if qos > 1 {
		return fmt.Errorf("unsupported MQTT QoS %d", qos)
	}
	var packetID uint16
	var ack chan ackResult
	if qos == 1 {
		packetID = c.nextPacketID()
		ack = make(chan ackResult, 1)
		c.pendingMu.Lock()
		c.pending[packetID] = ack
		c.pendingMu.Unlock()
		defer func() {
			c.pendingMu.Lock()
			delete(c.pending, packetID)
			c.pendingMu.Unlock()
		}()
	}
	packet, err := encodePublish(topic, payload, qos, retain, packetID)
	if err != nil {
		return err
	}
	if err := c.writePacket(packet); err != nil {
		c.fail(err)
		return err
	}
	if qos == 0 {
		return nil
	}
	timeout := c.cfg.PublishTimeout
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case result := <-ack:
		return result.err
	case <-timer.C:
		return errors.New("MQTT PUBACK timeout")
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return ErrNotConnected
	}
}

func (c *Client) Disconnect(ctx context.Context) error {
	if !c.IsConnected() {
		c.fail(nil)
		return nil
	}
	_ = c.writePacket([]byte{packetDISCONNECT << 4, 2, 0, 0}) // MQTT v5 normal disconnect + empty properties.
	c.fail(nil)
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) readLoop(ctx context.Context) {
	for {
		packetType, flags, body, err := readPacket(c.reader)
		if err != nil {
			if ctx.Err() == nil {
				c.fail(fmt.Errorf("read MQTT packet: %w", err))
			}
			return
		}
		switch packetType {
		case packetPUBACK:
			if len(body) < 2 {
				c.fail(errors.New("short MQTT PUBACK"))
				return
			}
			id := binary.BigEndian.Uint16(body[:2])
			c.pendingMu.Lock()
			ack := c.pending[id]
			c.pendingMu.Unlock()
			if ack != nil {
				var ackErr error
				if len(body) >= 3 && body[2] >= 0x80 {
					ackErr = fmt.Errorf("MQTT PUBACK rejected with reason 0x%02x", body[2])
				}
				select {
				case ack <- ackResult{err: ackErr}:
				default:
				}
			}
		case packetPINGRESP:
			// Broker is alive.
		case packetSUBACK:
			id, reason, parseErr := parseSuback(body)
			if parseErr != nil {
				c.fail(parseErr)
				return
			}
			c.subMu.Lock()
			ack := c.subacks[id]
			c.subMu.Unlock()
			if ack != nil {
				var ackErr error
				if reason >= 0x80 {
					ackErr = fmt.Errorf("MQTT SUBACK rejected with reason 0x%02x", reason)
				}
				select {
				case ack <- ackResult{err: ackErr}:
				default:
				}
			}
		case packetDISCONNECT:
			c.fail(errors.New("MQTT broker disconnected"))
			return
		case packetPUBLISH:
			message, packetID, parseErr := parsePublish(flags, body)
			if parseErr != nil {
				c.fail(parseErr)
				return
			}
			if message.QoS == 1 {
				if err := c.writePacket(encodePuback(packetID)); err != nil {
					c.fail(err)
					return
				}
			}
			if c.cfg.OnMessage != nil {
				go c.cfg.OnMessage(message)
			}
		default:
		}
	}
}

func (c *Client) pingLoop(ctx context.Context) {
	interval := c.cfg.KeepAlive / 2
	if interval < 5*time.Second {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			_ = c.conn.SetReadDeadline(time.Now().Add(c.cfg.KeepAlive))
			if err := c.writePacket([]byte{packetPINGREQ << 4, 0}); err != nil {
				c.fail(err)
				return
			}
		case <-ctx.Done():
			return
		case <-c.done:
			return
		}
	}
}

func (c *Client) writePacket(packet []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if !c.IsConnected() {
		return ErrNotConnected
	}
	_ = c.conn.SetWriteDeadline(time.Now().Add(c.cfg.PublishTimeout))
	defer c.conn.SetWriteDeadline(time.Time{})
	written := 0
	for written < len(packet) {
		n, err := c.conn.Write(packet[written:])
		if err != nil {
			return fmt.Errorf("write MQTT packet: %w", err)
		}
		if n == 0 {
			return io.ErrUnexpectedEOF
		}
		written += n
	}
	return nil
}

func (c *Client) fail(err error) {
	c.closeOnce.Do(func() {
		c.stateMu.Lock()
		c.connected = false
		c.stateMu.Unlock()
		c.cancel()
		_ = c.conn.Close()
		c.pendingMu.Lock()
		for _, ack := range c.pending {
			select {
			case ack <- ackResult{err: ErrNotConnected}:
			default:
			}
		}
		c.pending = make(map[uint16]chan ackResult)
		c.pendingMu.Unlock()
		c.subMu.Lock()
		for _, ack := range c.subacks {
			select {
			case ack <- ackResult{err: ErrNotConnected}:
			default:
			}
		}
		c.subacks = make(map[uint16]chan ackResult)
		c.subMu.Unlock()
		if err != nil {
			c.logger.Debug("MQTT connection closed", "error", err)
		}
		close(c.done)
	})
}

func (c *Client) nextPacketID() uint16 {
	for {
		id := uint16(c.packetID.Add(1))
		if id != 0 {
			return id
		}
	}
}

func parseBrokerURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "mqtt" && scheme != "mqtts" && scheme != "tcp" && scheme != "tls" && scheme != "ssl" {
		return nil, fmt.Errorf("unsupported MQTT scheme %q", u.Scheme)
	}
	if u.Hostname() == "" {
		return nil, errors.New("MQTT broker URL requires a hostname")
	}
	if u.Port() == "" {
		port := "1883"
		if isTLSScheme(scheme) {
			port = "8883"
		}
		u.Host = net.JoinHostPort(u.Hostname(), port)
	}
	return u, nil
}

func isTLSScheme(scheme string) bool {
	switch strings.ToLower(scheme) {
	case "mqtts", "tls", "ssl":
		return true
	default:
		return false
	}
}

func buildTLSConfig(cfg Config, host string) (*tls.Config, error) {
	serverName := cfg.ServerName
	if serverName == "" {
		serverName = host
	}
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: serverName, InsecureSkipVerify: cfg.InsecureSkipVerify} // #nosec G402 -- explicit user opt-in.
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, err
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("MQTT CA file contained no certificates")
		}
		tlsCfg.RootCAs = pool
	}
	if cfg.CertFile != "" || cfg.KeyFile != "" {
		if cfg.CertFile == "" || cfg.KeyFile == "" {
			return nil, errors.New("both MQTT client certificate and key are required")
		}
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, err
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	return tlsCfg, nil
}

func encodeConnect(cfg Config) []byte {
	var vh []byte
	vh = appendUTF8(vh, "MQTT")
	vh = append(vh, 5)  // MQTT 5.0
	flags := byte(0x02) // Clean Start.
	if cfg.WillTopic != "" {
		flags |= 0x04 | ((cfg.WillQoS & 0x03) << 3)
		if cfg.WillRetain {
			flags |= 0x20
		}
	}
	if cfg.Password != "" {
		flags |= 0x40
	}
	if cfg.Username != "" {
		flags |= 0x80
	}
	vh = append(vh, flags)
	keepAliveSeconds := int(cfg.KeepAlive / time.Second)
	if keepAliveSeconds < 1 {
		keepAliveSeconds = 1
	}
	if keepAliveSeconds > 65535 {
		keepAliveSeconds = 65535
	}
	vh = binary.BigEndian.AppendUint16(vh, uint16(keepAliveSeconds))
	vh = append(vh, 0) // CONNECT properties.
	payload := appendUTF8(nil, cfg.ClientID)
	if cfg.WillTopic != "" {
		payload = append(payload, 0) // Will properties.
		payload = appendUTF8(payload, cfg.WillTopic)
		payload = appendBinary(payload, cfg.WillPayload)
	}
	if cfg.Username != "" {
		payload = appendUTF8(payload, cfg.Username)
	}
	if cfg.Password != "" {
		payload = appendBinary(payload, []byte(cfg.Password))
	}
	return makePacket(packetCONNECT<<4, append(vh, payload...))
}

func validateConnack(body []byte) error {
	if len(body) < 3 {
		return errors.New("short MQTT CONNACK")
	}
	if body[1] != 0 {
		return fmt.Errorf("MQTT CONNACK rejected with reason 0x%02x", body[1])
	}
	return nil
}

func encodePublish(topic string, payload []byte, qos byte, retain bool, packetID uint16) ([]byte, error) {
	if topic == "" || len(topic) > 65535 {
		return nil, errors.New("invalid MQTT topic")
	}
	body := appendUTF8(nil, topic)
	if qos == 1 {
		body = binary.BigEndian.AppendUint16(body, packetID)
	}
	body = append(body, 0) // PUBLISH properties.
	body = append(body, payload...)
	header := byte(packetPUBLISH<<4) | ((qos & 0x03) << 1)
	if retain {
		header |= 0x01
	}
	return makePacket(header, body), nil
}

func encodeSubscribe(topic string, qos byte, packetID uint16) []byte {
	body := binary.BigEndian.AppendUint16(nil, packetID)
	body = append(body, 0) // SUBSCRIBE properties.
	body = appendUTF8(body, topic)
	body = append(body, qos&0x03)
	return makePacket((packetSUBSCRIBE<<4)|0x02, body)
}

func encodePuback(packetID uint16) []byte {
	body := binary.BigEndian.AppendUint16(nil, packetID)
	return makePacket(packetPUBACK<<4, body)
}

func parseSuback(body []byte) (uint16, byte, error) {
	if len(body) < 4 {
		return 0, 0, errors.New("short MQTT SUBACK")
	}
	id := binary.BigEndian.Uint16(body[:2])
	propertiesLen, consumed, err := parseVarIntBytes(body[2:])
	if err != nil {
		return 0, 0, err
	}
	offset := 2 + consumed + propertiesLen
	if offset >= len(body) {
		return 0, 0, errors.New("MQTT SUBACK has no reason code")
	}
	return id, body[offset], nil
}

func parsePublish(flags byte, body []byte) (Message, uint16, error) {
	if len(body) < 3 {
		return Message{}, 0, errors.New("short MQTT PUBLISH")
	}
	topicLen := int(binary.BigEndian.Uint16(body[:2]))
	offset := 2
	if topicLen == 0 || offset+topicLen > len(body) {
		return Message{}, 0, errors.New("invalid MQTT PUBLISH topic")
	}
	topic := string(body[offset : offset+topicLen])
	offset += topicLen
	qos := (flags >> 1) & 0x03
	if qos > 1 {
		return Message{}, 0, fmt.Errorf("unsupported incoming MQTT QoS %d", qos)
	}
	var packetID uint16
	if qos == 1 {
		if offset+2 > len(body) {
			return Message{}, 0, errors.New("short MQTT PUBLISH packet identifier")
		}
		packetID = binary.BigEndian.Uint16(body[offset : offset+2])
		offset += 2
	}
	propertiesLen, consumed, err := parseVarIntBytes(body[offset:])
	if err != nil {
		return Message{}, 0, err
	}
	offset += consumed
	if propertiesLen < 0 || offset+propertiesLen > len(body) {
		return Message{}, 0, errors.New("invalid MQTT PUBLISH properties")
	}
	offset += propertiesLen
	return Message{Topic: topic, Payload: append([]byte(nil), body[offset:]...), QoS: qos, Retain: flags&0x01 != 0}, packetID, nil
}

func parseVarIntBytes(data []byte) (value int, consumed int, err error) {
	multiplier := 1
	for i := 0; i < 4; i++ {
		if i >= len(data) {
			return 0, 0, io.ErrUnexpectedEOF
		}
		encoded := data[i]
		value += int(encoded&0x7f) * multiplier
		consumed++
		if encoded&0x80 == 0 {
			return value, consumed, nil
		}
		multiplier *= 128
	}
	return 0, 0, errors.New("malformed MQTT variable integer")
}

func makePacket(header byte, body []byte) []byte {
	packet := []byte{header}
	packet = appendVarInt(packet, len(body))
	return append(packet, body...)
}

func appendUTF8(dst []byte, value string) []byte {
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(value)))
	return append(dst, value...)
}

func appendBinary(dst, value []byte) []byte {
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(value)))
	return append(dst, value...)
}

func appendVarInt(dst []byte, value int) []byte {
	for {
		encoded := byte(value % 128)
		value /= 128
		if value > 0 {
			encoded |= 0x80
		}
		dst = append(dst, encoded)
		if value == 0 {
			return dst
		}
	}
}

func readPacket(reader *bufio.Reader) (packetType, flags byte, body []byte, err error) {
	first, err := reader.ReadByte()
	if err != nil {
		return 0, 0, nil, err
	}
	remaining, err := readVarInt(reader)
	if err != nil {
		return 0, 0, nil, err
	}
	if remaining > maxPacketSize {
		return 0, 0, nil, fmt.Errorf("MQTT packet too large: %d", remaining)
	}
	body = make([]byte, remaining)
	if _, err := io.ReadFull(reader, body); err != nil {
		return 0, 0, nil, err
	}
	return first >> 4, first & 0x0f, body, nil
}

func readVarInt(reader *bufio.Reader) (int, error) {
	value, multiplier := 0, 1
	for i := 0; i < 4; i++ {
		encoded, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}
		value += int(encoded&0x7f) * multiplier
		if encoded&0x80 == 0 {
			return value, nil
		}
		multiplier *= 128
	}
	return 0, errors.New("malformed MQTT remaining length")
}
