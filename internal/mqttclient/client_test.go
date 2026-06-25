package mqttclient

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"
)

type receivedPublish struct {
	topic   string
	payload []byte
	qos     byte
	retain  bool
}

func TestDialAndPublishQoS1(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	received := make(chan receivedPublish, 1)
	serverErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		packetType, _, _, err := readPacket(reader)
		if err != nil {
			serverErr <- err
			return
		}
		if packetType != packetCONNECT {
			serverErr <- io.ErrUnexpectedEOF
			return
		}
		// MQTT 5 CONNACK: session flags, success reason, zero property length.
		if _, err := conn.Write([]byte{packetCONNACK << 4, 3, 0, 0, 0}); err != nil {
			serverErr <- err
			return
		}
		for {
			packetType, flags, body, err := readPacket(reader)
			if err != nil {
				serverErr <- err
				return
			}
			switch packetType {
			case packetPUBLISH:
				if len(body) < 5 {
					serverErr <- io.ErrUnexpectedEOF
					return
				}
				topicLen := int(binary.BigEndian.Uint16(body[:2]))
				offset := 2 + topicLen
				topic := string(body[2:offset])
				qos := (flags >> 1) & 0x03
				var packetID uint16
				if qos == 1 {
					packetID = binary.BigEndian.Uint16(body[offset : offset+2])
					offset += 2
				}
				// The implementation emits zero PUBLISH properties.
				offset++
				received <- receivedPublish{topic: topic, payload: append([]byte(nil), body[offset:]...), qos: qos, retain: flags&1 != 0}
				if qos == 1 {
					ack := []byte{packetPUBACK << 4, 2, byte(packetID >> 8), byte(packetID)}
					if _, err := conn.Write(ack); err != nil {
						serverErr <- err
						return
					}
				}
			case packetPINGREQ:
				if _, err := conn.Write([]byte{packetPINGRESP << 4, 0}); err != nil {
					serverErr <- err
					return
				}
			case packetDISCONNECT:
				serverErr <- nil
				return
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := Dial(ctx, Config{
		BrokerURL:      "mqtt://" + listener.Addr().String(),
		ClientID:       "test-client",
		KeepAlive:      10 * time.Second,
		ConnectTimeout: 2 * time.Second,
		PublishTimeout: 2 * time.Second,
		WillTopic:      "test/availability",
		WillPayload:    []byte("offline"),
		WillQoS:        1,
		WillRetain:     true,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Publish(ctx, "codexbar/test", 1, true, []byte(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-received:
		if msg.topic != "codexbar/test" || string(msg.payload) != `{"ok":true}` || msg.qos != 1 || !msg.retain {
			t.Fatalf("unexpected publish: %+v payload=%q", msg, msg.payload)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := client.Disconnect(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-serverErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestTopicEncodingRejectsEmptyTopic(t *testing.T) {
	if _, err := encodePublish("", nil, 1, false, 1); err == nil {
		t.Fatal("expected empty topic error")
	}
}
