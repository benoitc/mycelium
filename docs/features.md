# Feature stability and coverage

Mycelium follows semantic versioning for 0.x: minor bumps (0.x → 0.y) may
change documented public APIs; patch bumps (0.x.y → 0.x.y+1) are
non-breaking. See [README.md](../README.md#versioning-policy) for the
full policy. 1.0 is not yet on the roadmap.

Stability tiers used below:

| Tier         | Promise                                                                                                 |
|--------------|---------------------------------------------------------------------------------------------------------|
| supported    | The team avoids silently breaking it. Intentional breaks land with a CHANGELOG entry and deprecation.   |
| beta         | Works as documented but the shape may change across a 0.x minor bump.                                   |
| experimental | Anything may change. Use at your own risk; the next minor may rename, redesign, or remove it.           |

## Membership and overlay

| Feature                                       | Tier         | Coverage notes                                                  |
|-----------------------------------------------|--------------|-----------------------------------------------------------------|
| `mycelium:join/1`, `leave/0`                  | supported    | CT: `mycelium_hyparview_SUITE`, `mycelium_proto_dist_SUITE`     |
| `mycelium:active_view/0`, `passive_view/0`    | supported    | CT: `mycelium_hyparview_SUITE`                                  |
| HyParView shuffle and promote-from-passive    | supported    | CT: `mycelium_churn_SUITE`, soak `broadcast_burst`              |
| `mycelium:subscribe/0,1`, `unsubscribe/1`     | supported    | EUnit: `mycelium_hyparview_events_tests`                        |
| Plumtree gossip (`mycelium_plumtree`)         | supported    | CT: `mycelium_plumtree_SUITE`; soak `broadcast_burst`           |

## Distribution

| Feature                                       | Tier         | Coverage notes                                                  |
|-----------------------------------------------|--------------|-----------------------------------------------------------------|
| `-proto_dist mycelium` boot path              | supported    | CT: `mycelium_proto_dist_SUITE`                                 |
| `mycelium_dist` carrier defaults              | supported    | CT: `mycelium_dist_basic_SUITE`                                 |
| Ed25519 auth + TLS channel binding (v2)       | supported    | CT: `mycelium_dist_auth_SUITE`, `mycelium_proto_dist_SUITE`     |
| TOFU vs strict trust modes                    | supported    | CT: `mycelium_dist_auth_basic_SUITE`                            |
| Idle dist-channel GC (`mycelium_dist_gc`)     | supported    | CT case `gc_skips_live_streams`; EUnit `mycelium_dist_gc_tests` |
| `mycelium:migrate_peer/1,2`                   | beta         | EUnit: `mycelium_migrate_peer_tests`                            |

## Service registry

| Feature                                                   | Tier      | Coverage notes                                              |
|-----------------------------------------------------------|-----------|-------------------------------------------------------------|
| `register_service/1,2`, `unregister_service/1`            | supported | CT: `mycelium_registry_SUITE`                               |
| `lookup/1`, `lookup_local/1`, `list_services/0`           | supported | CT: `mycelium_registry_SUITE`                               |
| `whereis_service/1,2` with overlay fallback               | supported | CT: `mycelium_registry_SUITE`, `mycelium_router_SUITE`      |
| Via callbacks (`{via, mycelium, _}`)                      | supported | EUnit: `mycelium_via_tests`                                 |
| `global_register/1` proxy bridge                          | beta      | EUnit: `mycelium_registry_tests`                            |
| `get_proxy/1`                                             | beta      | EUnit: `mycelium_registry_tests`                            |
| Service events API (`subscribe_services/0,1`)             | beta      | CT: `mycelium_service_events_SUITE`                         |

## Singletons and leader election

| Feature                                                   | Tier      | Coverage notes                                                      |
|-----------------------------------------------------------|-----------|--------------------------------------------------------------------|
| `lead/1,2`, `resign/1`, `leader/1`, `is_leader/1`         | beta      | CT: `mycelium_leader_SUITE`, `mycelium_leader_e2e_SUITE`           |
| `{mycelium_leader, _, {elected, Fence} \| revoked}` msgs  | beta      | CT: `mycelium_leader_SUITE`                                        |
| HLC fencing token (`fence/1`)                             | beta      | CT: `mycelium_leader_e2e_SUITE` (`F2 > F1` across leader failover) |
| `peer_up`/`peer_down` re-election                         | beta      | CT: `mycelium_leader_e2e_SUITE`                                    |

## Sharded placement

| Feature                                                   | Tier      | Coverage notes                                                     |
|-----------------------------------------------------------|-----------|--------------------------------------------------------------------|
| `place/1`, `owners/2`, `is_owner/1`, `partition/1`        | beta      | CT: `mycelium_shard_SUITE`, `mycelium_shard_e2e_SUITE`            |
| `members/0` (lease-based live-node set)                   | beta      | CT: `mycelium_shard_SUITE`, `mycelium_shard_e2e_SUITE`            |
| `{mycelium_shard, {acquired \| released, P}}` events      | beta      | CT: `mycelium_shard_SUITE`, `mycelium_shard_e2e_SUITE`            |

## Durable reminders

| Feature                                                   | Tier      | Coverage notes                                                       |
|-----------------------------------------------------------|-----------|----------------------------------------------------------------------|
| `remind/3`, `remind_after/3`, `cancel_reminder/1`         | beta      | CT: `mycelium_reminder_SUITE`, `mycelium_reminder_e2e_SUITE`         |
| `subscribe_reminders/0,1`, `unsubscribe_reminders/1`      | beta      | CT: `mycelium_reminder_SUITE`                                       |
| `{mycelium_reminder, Key, Payload, Fence}` delivery       | beta      | CT: `mycelium_reminder_SUITE` (stable fence, no double-fire)        |
| Survivor fires after owner death                          | beta      | CT: `mycelium_reminder_e2e_SUITE` (kill owner before fire)          |
| Disk persistence (survives full-cluster restart)          | beta      | CT: `mycelium_reminder_e2e_SUITE` (`reminder_survives_full_cluster_restart`) |

## Replicated maps

| Feature                                                   | Tier      | Coverage notes                                                       |
|-----------------------------------------------------------|-----------|----------------------------------------------------------------------|
| `new_map/1,2`, `delete_map/1`                             | beta      | CT: `mycelium_map_SUITE`, `mycelium_map_e2e_SUITE`                  |
| `map_put/3`, `map_remove/2`, `map_get/2`, `map_keys/1`, `map_to_list/1` | beta | CT: `mycelium_map_SUITE`; convergence in `mycelium_map_e2e_SUITE` |
| `subscribe_map/1,2`, `unsubscribe_map/1,2`                | beta      | CT: `mycelium_map_SUITE` (events, DOWN cleanup)                     |
| `{mycelium_map, Name, {put \| remove, ...}}` events       | beta      | CT: `mycelium_map_SUITE`                                            |
| Late-join full-sync from peers                            | beta      | CT: `mycelium_map_e2e_SUITE` (map created after cluster formation) |
| Optional disk persistence (`persist => true`)             | beta      | CT: `mycelium_map_SUITE` (`persist_recovers_after_restart`), `mycelium_map_e2e_SUITE` (`persist_map_survives_full_cluster_restart`) |

## Streams

| Feature                                       | Tier         | Coverage notes                                                  |
|-----------------------------------------------|--------------|-----------------------------------------------------------------|
| `mycelium_streams` tagged multiplex           | supported    | EUnit: `mycelium_streams_tests`, prop suite                     |
| Reserved `<<"mycelium:", _>>` tag namespace   | supported    | (documented; future-proofs internal protocols)                  |

## CRDT and time

| Feature                                       | Tier         | Coverage notes                                                  |
|-----------------------------------------------|--------------|-----------------------------------------------------------------|
| `mycelium_ormap` (OR-Map CRDT)                | supported    | CT: `mycelium_ormap_SUITE`; prop suite                          |
| `mycelium_hlc` (Hybrid Logical Clock)         | supported    | CT: `mycelium_hlc_SUITE`; prop suite                            |
| `mycelium_replica` replication behaviour      | beta         | CT: the 4 consumer suites + `mycelium_map_e2e_SUITE`            |
| `mycelium_crdt_wire` safe gossip ingest       | supported    | EUnit: `mycelium_crdt_wire_tests`; CT: `mycelium_map_SUITE`     |
| `mycelium_replica_log` (WAL + snapshot store) | supported    | EUnit: `mycelium_replica_log_tests`                             |

## Operations

| Feature                                       | Tier         | Coverage notes                                                  |
|-----------------------------------------------|--------------|-----------------------------------------------------------------|
| `mycelium_rotate:rotate_cert/0,1`             | beta         | EUnit: `mycelium_rotate_tests`                                  |
| `mycelium_rotate:rotate_identity/0,1`         | beta         | EUnit: `mycelium_rotate_tests`                                  |
| `instrument` metrics                          | beta         | EUnit: `mycelium_metrics_tests`                                 |
| Discovery backends (file, DNS, static)        | supported    | EUnit: `mycelium_discovery_tests`                               |

## Auxiliary

| Feature                                       | Tier         | Coverage notes                                                  |
|-----------------------------------------------|--------------|-----------------------------------------------------------------|
| External relay adapter seam                   | experimental | Docs only; no committed adapter                                 |
| Soak suite (`MYCELIUM_CT_SOAK=1`)             | experimental | One active case; rest is scaffolding                            |
| Bench harness (`bench/run.sh`)                | experimental | Soft CI regression gate                                         |
| `mycelium:start_service_holder/1`             | experimental | Integration-test helper; may move out of `mycelium.erl`         |

## When a feature changes

* **supported → supported (refined):** CHANGELOG entry, no version bump required if the change is non-breaking.
* **supported → breaking change:** deprecation in one minor, removal at the earliest in the next minor. CHANGELOG entry on both.
* **beta → supported:** CHANGELOG entry on the minor that promotes it.
* **beta → breaking:** minor bump, CHANGELOG entry, no deprecation cycle required.
* **experimental:** changes land without ceremony but should still appear in CHANGELOG when they affect callers.
