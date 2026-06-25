# MQTT topic and envelope contract

## Topic tree

```text
codexbar/discovery/v1/<fleet-id>/<machine>
<prefix>/nodes/<machine>/availability
<prefix>/nodes/<machine>/meta
<prefix>/nodes/<machine>/heartbeat
<prefix>/events/<machine>/<kind>
<prefix>/nodes/<machine>/snapshots/<kind>/<scope>
```

`kind` is a path such as `serve/usage`, `serve/cost`, `cli/active-account-probe`, or `agent/error`.

### Retention

| Topic | Retained | Purpose |
|---|---:|---|
| discovery | yes | well-known HA fleet discovery beacon; includes the configurable data prefix |
| availability | yes | `online` / LWT `offline` |
| meta | yes | machine, build and collector contract |
| heartbeat | yes | freshness, job health and spool depth |
| events | no | ordered evidence stream for the HA aggregator |
| snapshots | yes | latest successful observation for bootstrap/recovery |

## Observation envelope

```json
{
  "schema": "io.github.mplummeridge.codexbar_mqtt.observation.v1",
  "event_id": "...",
  "kind": "cli/active-account-probe",
  "snapshot_scope": "claude",
  "observed_at": "2026-06-25T12:00:01Z",
  "machine": {
    "id": "macbook-m4",
    "name": "Marvin MacBook M4",
    "hostname": "Marvins-MacBook-Pro.local",
    "tags": {"site": "home"}
  },
  "agent": {
    "version": "0.1.0",
    "commit": "...",
    "build_date": "..."
  },
  "collection": {
    "transport": "cli",
    "operation": "usage",
    "semantic_scope": "current_default_account_probe",
    "correlation_id": "cost-macbook-m4-...",
    "phase": "before-cost",
    "started_at": "...",
    "finished_at": "...",
    "command": ["/opt/homebrew/bin/codexbar", "--provider", "claude", "--format", "json", "--json-only"],
    "exit_code": 0,
    "duration_ms": 842,
    "content_type": "application/json",
    "success": true
  },
  "payload_sha256": "...",
  "payload": [
    {"provider": "claude", "account": "...", "usage": {}}
  ]
}
```

The `payload` is not normalised or pruned. New CodexBar fields therefore flow to HA without a Mac-agent release.

## Semantic scopes

| Scope | Interpretation |
|---|---|
| `machine_runtime_health` | local `codexbar serve` health |
| `serve_usage_snapshot` | normal `/usage`; may include multiple Codex accounts |
| `all_registered_provider_snapshot` | `/usage?provider=all`, provider discovery, not activity |
| `current_default_account_probe` | CLI without `--all-accounts`; strongest available activity evidence |
| `all_visible_or_configured_accounts` | CLI `--all-accounts`; catalogue only |
| `provider_status_enriched_usage` | usage plus provider status |
| `machine_local_cost_snapshot` | local cost ledger, never inherently account-labelled |
| `machine_local_cost_history` | local cost history for requested day horizon |
| `cost_attribution_account_bracket` | account observation immediately before/after cost sampling |
| `local_codexbar_config_validation` | local configuration diagnostics |

## Consumer rules

1. Deduplicate by `event_id`.
2. Process events by `collection.finished_at`, then `event_id`; broker arrival order is not an authority after spool replay.
3. Never infer active Codex account merely because a row appears in `serve/usage`.
4. Never sum account-global quota/dashboard snapshots across machines.
5. Never assign an entire local historical cost snapshot to the currently active account.
6. Attribute cost **deltas** only when bracketing evidence is unambiguous.
7. Treat retained snapshots as bootstrap state, not a complete event history.

## Home Assistant fleet discovery

Every node publishes a retained beacon to:

```text
codexbar/discovery/v1/<fleet-id>/<machine-id>
```

`fleet-id` is the first 64 bits of SHA-256 over the normalized MQTT data prefix.
Nodes sharing a data prefix therefore discover as one Home Assistant fleet, while
multiple prefixes remain separate config entries. The payload includes the exact
`fleet.topic_prefix`; consumers must validate that its hash matches `fleet.id`.

The broker ACL for an agent should allow retained writes to both its configured
data prefix and `codexbar/discovery/v1/#`. Failure to publish the discovery beacon
does not stop the observation data plane.
