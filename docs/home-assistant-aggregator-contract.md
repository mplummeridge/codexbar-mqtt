# Home Assistant aggregator contract

The Mac agent is an evidence producer. A Home Assistant integration/add-on should maintain projections keyed by machine, provider, account, and time.

## Evidence precedence

For account identity inside a CodexBar row:

```text
row.account
usage.identity.accountEmail
usage.accountEmail
usage.identity.accountOrganization
usage.accountOrganization
```

Rows without a strong identity must be scoped to the machine until stronger evidence appears. Do not merge separate `provider:default` identities globally.

## Projection classes

### Account quota

Source: `serve/usage`, `cli/active-account-probe`, and account catalogue rows.

Key: provider + strong account identity + rate-window ID.

Rule: newest provider `usage.updatedAt`, falling back to observation time. Never sum percentages.

### Account-global dashboards

Example: Codex `openaiDashboard.usageBreakdown`.

Rule: newest dashboard snapshot per account wins. Deduplicate by account/day/service. Never sum identical snapshots from several machines.

### Machine-local cost

Source: `serve/cost` and `cli/cost-horizon`.

Key: machine + provider + date + model. Keep the latest revision of each local ledger row.

### Inferred account cost

Compute a delta between consecutive revisions of the same machine/provider/date/model row. Join that delta to a cost-attribution correlation cycle:

```text
before-cost account == after-cost account -> inferred attribution
before-cost account != after-cost account -> ambiguous
missing/failed bracket                    -> unattributed
negative delta                            -> ledger reset/backfill, establish new baseline
```

### Active account timeline

`cli/active-account-probe` observations are the strongest activity evidence. `serve/usage` is weaker when it includes multiple visible accounts. The HA side should record confidence per interval.

## Agent discovery

The 0.2 agent publishes a retained discovery beacon to:

```text
codexbar/discovery/v1/<fleet-id>/<machine-id>
```

The Home Assistant custom integration subscribes to `codexbar/discovery/v1/+/+`,
validates the beacon's schema, contract major, topic-prefix hash and machine/topic identity,
then creates one pending config flow per fleet. This beacon is bootstrap
metadata only; all aggregation evidence remains under the advertised data
prefix.
