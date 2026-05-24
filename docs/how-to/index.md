# How-to guides

Task-focused recipes. Each page is "how do I do X?", not "what
is X?". For the conceptual side, read the
[Concepts](../concepts/index.md) section.

## Operating a cluster

- [Run in production](run-in-production.md) — sizing, network
  surface, secrets, configuration reference, logging, shutdown.
- [Configure authentication](configure-authentication.md) — TOFU
  versus strict, provisioning the trust store, key rotation.
- [Observe a cluster](observe-cluster.md) — the metrics
  catalogue, wiring a Prometheus or OTLP exporter, alerting.
- [Troubleshoot](troubleshoot.md) — symptom-cause-fix tables for
  boot failures, authentication, cluster membership, GC.

## Specific tasks

- [Migrate connections](migrate-connections.md) — RFC 9000 §9
  path rebind via `migrate_peer/1,2`, including a small watchdog
  recipe.
- [Route through a relay](route-through-relay.md) — wiring an
  external relay or tunnel adapter for peers that cannot reach
  each other directly.
- [Run the tests](run-tests.md) — local CT and EUnit suites,
  the gated soak suite, the docker auth integration suite.
- [Partition state across nodes](partition-state.md) — route keys
  to owners with `place/1` and hand off on ownership events.
- [Schedule durable jobs](schedule-durable-jobs.md) — run work at a
  future time with `remind/3`, surviving the scheduling node's death.
- [Share replicated state](share-replicated-state.md) — a
  cluster-wide config/flags map with `mycelium_map`: put, get,
  subscribe, validate, tune GC.

## After how-tos

- [Concepts](../concepts/index.md) for the "why".
- [Reference](../reference/index.md) for the API surface and the
  full configuration list.
