CodexBar Fleet 0.2.0
====================

1. Upgrade every Mac to codexbar-mqtt 0.2.0.
2. Ensure its MQTT ACL permits retained writes to:
     codexbar/discovery/v1/#
   as well as the configured data prefix (normally codexbar/v1/#).
3. Extract codexbar-fleet-ha-0.2.0-config-root.zip into Home Assistant /config,
   or extract the custom-component archive under /config/custom_components.
4. Restart Home Assistant.
5. Start or restart one Mac agent.
6. Open Settings -> Devices & services. Home Assistant should show
   "CodexBar Fleet discovered". Confirm it; no prefix or machine list is needed.

Legacy fallback
---------------
An existing 0.1 Home Assistant entry migrates automatically. A 0.1 Mac agent,
or a broker that blocks the discovery beacon, can still be configured through
Add integration -> CodexBar Fleet and the normal prefix codexbar/v1.

Verification
------------
On a Mac:
  codexbar-mqtt doctor --config "$HOME/Library/Application Support/codexbar-mqtt/config.json"

The doctor report should show mqtt_discovery OK and the retained topic:
  codexbar/discovery/v1/<fleet-id>/<machine-id>
