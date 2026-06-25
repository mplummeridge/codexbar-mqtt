# codexbar-mqtt

A native, dependency-free macOS agent that publishes **raw CodexBar observations** to MQTT for central aggregation in Home Assistant.

It deliberately does not decide which machine “owns” an account and does not aggregate usage locally. One Mac can cycle through many Codex/Claude accounts; the Home Assistant-side consumer receives timestamped evidence and builds the account model centrally.

## What it collects

| Observation | Source | Default cadence | Semantics |
|---|---|---:|---|
| `serve/health` | `GET /health` | 30 s | Local CodexBar server health/version |
| `serve/usage` | `GET /usage` | 60 s | Enabled-provider usage snapshot |
| `serve/cost` | `GET /cost?provider=both` | 300 s | Machine-local Codex/Claude token ledger snapshot |
| `serve/usage` / `all` | `GET /usage?provider=all` | 30 min | Expensive provider discovery snapshot |
| `cli/active-account-probe` | `codexbar --provider P --format json` | 60 s | Current/default account evidence for account-transition modelling |
| `cli/account-catalogue` | `codexbar --provider P --all-accounts` | 30 min | All visible/configured accounts, not activity evidence |
| `cli/usage-status` | `codexbar ... --status` | 15 min | Provider status enrichment absent from `serve` |
| `cli/cost-horizon` | `codexbar cost --days N` | 60 min | 1/7/30/90-day machine-local history |
| `cli/config-validation` | `codexbar config validate` | 60 min | Local collector diagnostics |

Every observation contains the original JSON unchanged under `payload`, plus collection timing, source command/endpoint, semantic scope, machine identity, payload SHA-256, and a unique event ID.

## Why both HTTP and CLI probes?

CodexBar `serve` currently exposes only `/health`, `/usage`, and `/cost`, with `provider` as its only HTTP selector. It is efficient and keeps fetch sessions warm, but:

* `serve` omits provider status.
* `serve` has no `account`, `account-index`, `all-accounts`, `source`, `days`, or `refresh` query options.
* Codex `serve` intentionally returns all visible Codex accounts, so row presence is not proof that an account is active.

The separate CLI probes preserve those distinctions for the HA aggregator.

## Cost-attribution bracketing

Every periodic cost collection receives a correlation ID and is bracketed by account probes:

```text
active-account probe(s), phase=before-cost
GET /cost,              phase=cost
active-account probe(s), phase=after-cost
```

All observations share `collection.correlation_id`. HA can attribute a local cost delta only when the same account is observed on both sides. If an account changes or either bracket is missing, the delta must remain ambiguous/unattributed.

## Install

CodexBar’s CLI must be installed first:

```bash
# CodexBar → Preferences → Advanced → Install CLI
codexbar --version
```

From a release archive:

```bash
./scripts/install.sh
```

The installer creates:

```text
~/Library/Application Support/codexbar-mqtt/bin/codexbar-mqtt
~/Library/Application Support/codexbar-mqtt/config.json
~/Library/Application Support/codexbar-mqtt/mqtt-password
~/Library/LaunchAgents/dev.mmv3.codexbar-mqtt.plist
```

Edit `config.json`, write the broker password without a trailing newline, then restart:

```bash
printf '%s' 'YOUR_MQTT_PASSWORD' > "$HOME/Library/Application Support/codexbar-mqtt/mqtt-password"
chmod 600 "$HOME/Library/Application Support/codexbar-mqtt/mqtt-password"

launchctl kickstart -k "gui/$(id -u)/dev.mmv3.codexbar-mqtt"
```

Tailscale is used only for the outbound MQTT connection; CodexBar remains safely bound to local loopback.

## Commands

```bash
codexbar-mqtt init --config ~/Library/Application\ Support/codexbar-mqtt/config.json
codexbar-mqtt doctor --config ~/Library/Application\ Support/codexbar-mqtt/config.json
codexbar-mqtt once --config ~/Library/Application\ Support/codexbar-mqtt/config.json
codexbar-mqtt run --config ~/Library/Application\ Support/codexbar-mqtt/config.json
codexbar-mqtt schema
codexbar-mqtt version
```

`run` optionally supervises `codexbar serve` as the logged-in macOS user. This preserves access to the same config, browser cookies, and Keychain context as CodexBar.

## Home Assistant auto-discovery

Version 0.2 publishes a retained fleet beacon under the well-known topic:

```text
codexbar/discovery/v1/<fleet-id>/<machine-id>
```

The CodexBar Fleet Home Assistant integration uses this beacon to discover the
MQTT data prefix and create one config flow per fleet. No machine list or topic
prefix is required during normal setup. Brokers with restrictive ACLs must allow
the agent to publish to `codexbar/discovery/v1/#` as well as its configured data
prefix.

## MQTT topics

For machine `macbook-m4` and prefix `codexbar/v1`:

```text
codexbar/discovery/v1/db9cdd5da48dbaf5/macbook-m4
codexbar/v1/nodes/macbook-m4/availability
codexbar/v1/nodes/macbook-m4/meta
codexbar/v1/nodes/macbook-m4/heartbeat
codexbar/v1/events/macbook-m4/serve/usage
codexbar/v1/events/macbook-m4/cli/active-account-probe
codexbar/v1/nodes/macbook-m4/snapshots/serve/usage/enabled
codexbar/v1/nodes/macbook-m4/snapshots/cli/active-account-probe/claude
```

Events are non-retained evidence. Successful latest snapshots are retained. Availability uses an MQTT last will. See [`docs/topic-contract.md`](docs/topic-contract.md).

## Offline behaviour

Before publishing, every event/snapshot is atomically written to a bounded disk spool. On reconnection it is replayed FIFO with QoS 1. When limits are exceeded:

1. obsolete retained snapshots are coalesced by topic;
2. the oldest remaining messages are removed;
3. `heartbeat.spool.dropped` exposes the loss.

This preserves account transitions during short MQTT/Tailscale outages without allowing unbounded disk growth.

## Security

* No arbitrary remote-command MQTT topic exists.
* MQTT password can come from a `0600` file or environment variable; it need not live in JSON or the LaunchAgent.
* TLS, custom CA and mTLS are supported.
* `insecure_skip_verify` is explicit and defaults to false.
* CLI stderr is excluded by default because provider errors can contain sensitive context.
* The managed CodexBar listener remains loopback-only.

## Build

```bash
make test
make build
make release
```

The project uses only the Go standard library. Cross-compilation produces native Apple Silicon and Intel macOS binaries.
