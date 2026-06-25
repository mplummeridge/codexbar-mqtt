# Security

Report vulnerabilities privately to the repository owner.

The agent intentionally has no inbound command topic. It executes only statically defined CodexBar commands from local configuration. MQTT credentials should be supplied through `mqtt.password_file` or `mqtt.password_env`; avoid placing them directly in JSON. Keep TLS verification enabled outside a trusted Tailscale-only plaintext deployment.
