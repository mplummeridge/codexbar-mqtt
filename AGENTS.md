# Repository instructions

## Verification

Run before committing:

```bash
gofmt -w .
go test ./...
go vet ./...
make build
```

## Invariants

* Preserve raw CodexBar JSON under `Observation.payload`.
* Do not introduce machine-to-account bindings or local account aggregation.
* Account catalogue observations are not activity evidence.
* Cost payloads are machine-local and account-agnostic; only correlated deltas may be inferred centrally.
* Any new observation kind must document its semantic scope in `docs/topic-contract.md`.
* Do not add arbitrary command execution over MQTT.
* Keep the executable dependency-free and cross-compilable with `CGO_ENABLED=0`.
