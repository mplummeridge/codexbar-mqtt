# CodexBar MQTT Collector

Native macOS collector for publishing [CodexBar](https://github.com/steipete/CodexBar) observations to MQTT for the `ha-codexbar-fleet` Home Assistant integration.

This repository currently carries the MQTT/topic contract, LaunchAgent templates, installer scripts, and checksums for the generated `0.2.0` macOS archives. The generated tarballs still need to be uploaded as GitHub release assets because the current ChatGPT GitHub connector cannot upload local binary files or create releases by file path.

## Install

After uploading the generated release artifacts, download the appropriate archive from GitHub Releases:

```bash
tar -xzf codexbar-mqtt-0.2.0-darwin-arm64.tar.gz
cd codexbar-mqtt-0.2.0-darwin-arm64

MQTT_BROKER='mqtt://homeassistant.local:1883' \
MQTT_USERNAME='codexbar' \
MACHINE_ID='macbook-m4' \
./scripts/install.sh
```

Then store the MQTT password:

```bash
printf '%s' 'MQTT_PASSWORD' > \
  "$HOME/Library/Application Support/codexbar-mqtt/mqtt-password"
```

Validate:

```bash
"$HOME/Library/Application Support/codexbar-mqtt/bin/codexbar-mqtt" \
  doctor \
  --config "$HOME/Library/Application Support/codexbar-mqtt/config.json"
```

## MQTT ACLs

The collector publishes to:

```text
codexbar/discovery/v1/#
codexbar/v1/#
```

## Contract

See:

- [`docs/topic-contract.md`](docs/topic-contract.md)
- [`docs/home-assistant-aggregator-contract.md`](docs/home-assistant-aggregator-contract.md)

## Home Assistant

Install the HACS integration from:

```text
https://github.com/mplummeridge/ha-codexbar-fleet
```

## Status

This is an early private/prototype release. The collector is designed to emit raw observations; account inference and cost attribution belong to Home Assistant.
