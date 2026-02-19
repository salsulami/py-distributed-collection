# distributed-python-collections

Hazelcast-inspired distributed collections for Python services, with support for:

- distributed `map`, `list`, `queue`, and `topic`
- TCP cluster networking with static and multicast discovery
- consistency modes (`best_effort`, `quorum`, `all`, `linearizable`)
- leader election, heartbeats, and split-brain write protection
- optional TLS/mTLS + HMAC message authentication + sender ACLs
- durability (WAL + snapshot lifecycle)
- optional Redis centralized backend via a separate plugin package

---

## Table of contents

- [Architecture](#architecture)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Switching storage backends](#switching-storage-backends)
- [Operational endpoints](#operational-endpoints)
- [API reference](#api-reference)
- [Configuration reference](#configuration-reference)
- [Contributing](#contributing)
- [Repository layout](#repository-layout)
- [License](#license)

---

## Architecture

### High-level view

```text
                        +-----------------------------+
                        |      Python Application     |
                        |  ClusterNode + Collections  |
                        +--------------+--------------+
                                       |
                     +-----------------+-----------------+
                     |                                   |
          +----------v----------+            +-----------v-----------+
          |  Networking Layer   |            |   Storage Layer       |
          |  TCP protocol       |            | CollectionStore       |
          |  discovery          |            | - ClusterDataStore    |
          |  leader/heartbeat   |            | - RedisClusterData... |
          +----------+----------+            +-----------+-----------+
                     |                                   |
           +---------+-----------+                       |
           |  Peer cluster nodes |                       |
           +---------------------+                       |
                                                         |
                                      +------------------v------------------+
                                      | Durability / Observability          |
                                      | WAL, snapshots, health, metrics,    |
                                      | traces, scenario harnesses          |
                                      +-------------------------------------+
```

### Storage mode behavior

```text
Mode A: In-memory backend (default)
-----------------------------------
Node A local store <--> replication over TCP <--> Node B local store

Mode B: Redis backend (plugin)
------------------------------
Node A -> Redis <- Node B
Node-to-node collection replication for map/list/queue is skipped
(topic pub/sub still uses cluster messaging)
```

### Runtime responsibilities

- **ClusterNode**
  - membership, discovery, leader election, write routing
  - transport/auth/version checks
  - consistency commit strategy
  - observability and lifecycle orchestration
- **CollectionStore backends**
  - apply/read snapshot lifecycle for collection state
  - pluggable backend contract
- **Durability**
  - optional WAL replay and snapshot checkpointing
- **Security**
  - optional HMAC message signatures
  - optional TLS/mTLS transport
  - optional sender ACL

---

## Installation

### Core package

```bash
pip install distributed-python-collections
```

From source (repo root):

```bash
pip install -e .
```

### Redis backend plugin (optional, separate package)

```bash
pip install distributed-python-collections-redis
```

From source:

```bash
pip install -e redis_backend
```

---

## Quick start

### 1) Minimal in-memory node

```python
from distributed_collections import ClusterConfig, ClusterNode, NodeAddress

node = ClusterNode(
    ClusterConfig(
        cluster_name="orders",
        bind=NodeAddress("0.0.0.0", 5701),
        advertise_host="10.0.0.5",
    )
)
node.start()

users = node.get_map("users")
users.put("u-1", {"name": "Alice"})

node.stop()
```

### 2) Two nodes with static discovery

Create two nodes and set each node's seed list to include the other node address.
Call `start(join=True)` (default) or call `join_cluster()` explicitly.

### 3) Redis centralized storage backend

```python
from distributed_collections import ClusterConfig, create_node

node = create_node(
    ClusterConfig(cluster_name="orders"),
    backend="redis",
    redis_url="redis://127.0.0.1:6379/0",
    namespace="orders-cluster",
)
node.start()
```

In Redis mode, all nodes use shared Redis state for `map`/`list`/`queue`.

---

## Switching storage backends

Choose backend with one argument:

```python
from distributed_collections import ClusterConfig, create_node

config = ClusterConfig(cluster_name="orders")

node_memory = create_node(config, backend="memory")
node_redis = create_node(
    config,
    backend="redis",
    redis_url="redis://127.0.0.1:6379/0",
    namespace="orders",
)
```

Equivalent classmethod form:

```python
from distributed_collections import ClusterNode

node = ClusterNode.from_backend(config, backend="memory")
node = ClusterNode.from_backend(config, backend="redis", redis_url="redis://127.0.0.1:6379/0")
```

Backend discovery:

```python
from distributed_collections import available_backends
print(available_backends())  # e.g. ('memory', 'redis') or ('memory',)
```

---

## Operational endpoints

If `observability.enable_http=True`, node exposes:

- `GET /healthz` - JSON health state
- `GET /metrics` - Prometheus-style text metrics
- `GET /traces` - recent in-memory trace events

Node API equivalents:

- `node.health()`
- `node.metrics_text()`
- `node.recent_traces()`
- `node.stats()`

---

## API reference

## Core factories

- `create_store(backend="memory"|"redis", **backend_options) -> CollectionStore`
- `create_node(config, backend="memory"|"redis", **backend_options) -> ClusterNode`
- `available_backends() -> tuple[str, ...]`
- `StoreBackend` enum (`MEMORY`, `REDIS`)

## `ClusterNode` public API

- `ClusterNode.from_backend(config, backend="memory", **backend_options)`
- `ClusterNode(config, store=None)`
- `start(join=True)`
- `stop()`
- `close()`
- `is_running` (property)
- `stats()`
- `health()`
- `metrics_text()`
- `recent_traces()`
- `join_cluster()`
- `members()`
- `wait_for_next_join_window()`
- `get_map(name)`
- `get_list(name)`
- `get_queue(name)`
- `get_topic(name)`

## Distributed collection APIs

### `DistributedMap`

- `put(key, value)`
- `get(key, default=None)`
- `remove(key)`
- `clear()`
- `items_dict()`
- mapping protocol: `__getitem__`, `__setitem__`, `__delitem__`, iteration, `len()`

### `DistributedList`

- `append(value)`
- `extend(values)`
- `pop(index=None)`
- `remove(value)`
- `clear()`
- `values()`
- list-like protocol: `__getitem__`, `__setitem__`, iteration, `len()`

### `DistributedQueue`

- `offer(value)`
- `poll()`
- `peek()`
- `clear()`
- `values()`
- `size()` / `len()`

### `DistributedTopic`

- `subscribe(callback) -> subscription_id`
- `unsubscribe(subscription_id) -> bool`
- `publish(message)`

## Redis plugin API (`distributed_collections_redis`)

- `RedisStoreConfig(redis_url, namespace)`
- `RedisClusterDataStore(config=None, redis_client=None)`
- `RedisClusterNode(config, redis_url=..., namespace=..., redis_client=None)`

---

## Configuration reference

## Top-level `ClusterConfig`

| Field | Type | Default |
|---|---|---|
| `cluster_name` | `str` | `"default"` |
| `bind` | `NodeAddress` | `NodeAddress("0.0.0.0", 5701)` |
| `advertise_host` | `str` | `"127.0.0.1"` |
| `static_discovery` | `StaticDiscoveryConfig` | empty seeds |
| `multicast` | `MulticastDiscoveryConfig` | defaults below |
| `enabled_discovery` | `tuple[DiscoveryMode, ...]` | `(STATIC, MULTICAST)` |
| `socket_timeout_seconds` | `float` | `2.0` |
| `reconnect_interval_seconds` | `float` | `3.0` |
| `auto_sync_on_join` | `bool` | `True` |
| `security` | `SecurityConfig` | defaults below |
| `tls` | `TLSConfig` | defaults below |
| `acl` | `ACLConfig` | defaults below |
| `replication` | `ReplicationConfig` | defaults below |
| `persistence` | `PersistenceConfig` | defaults below |
| `wal` | `WriteAheadLogConfig` | defaults below |
| `consensus` | `ConsensusConfig` | defaults below |
| `consistency` | `ConsistencyConfig` | defaults below |
| `observability` | `ObservabilityConfig` | defaults below |
| `upgrade` | `UpgradeConfig` | defaults below |

## Basic types

### `NodeAddress`

| Field | Type | Notes |
|---|---|---|
| `host` | `str` | required, non-empty |
| `port` | `int` | `1..65535` |

### `DiscoveryMode`

- `STATIC`
- `MULTICAST`

### `ConsistencyMode`

- `BEST_EFFORT`
- `QUORUM`
- `ALL`
- `LINEARIZABLE`

## Nested config dataclasses

### `StaticDiscoveryConfig`

| Field | Type | Default |
|---|---|---|
| `seeds` | `list[NodeAddress]` | `[]` |

### `MulticastDiscoveryConfig`

| Field | Type | Default |
|---|---|---|
| `group` | `str` | `"239.11.11.11"` |
| `port` | `int` | `55300` |
| `timeout_seconds` | `float` | `1.5` |
| `discovery_attempts` | `int` | `2` |
| `ttl` | `int` | `1` |

### `SecurityConfig`

| Field | Type | Default |
|---|---|---|
| `shared_token` | `str \| None` | `None` |

### `TLSConfig`

| Field | Type | Default |
|---|---|---|
| `enabled` | `bool` | `False` |
| `certfile` | `str \| None` | `None` |
| `keyfile` | `str \| None` | `None` |
| `ca_file` | `str \| None` | `None` |
| `require_client_cert` | `bool` | `False` |
| `server_hostname` | `str \| None` | `None` |

### `ACLConfig`

| Field | Type | Default |
|---|---|---|
| `enabled` | `bool` | `False` |
| `default_allow` | `bool` | `True` |
| `sender_permissions` | `dict[str, tuple[str, ...]]` | `{}` |

### `ReplicationConfig`

| Field | Type | Default |
|---|---|---|
| `worker_threads` | `int` | `2` |
| `max_retries` | `int` | `4` |
| `initial_backoff_seconds` | `float` | `0.05` |
| `max_backoff_seconds` | `float` | `1.0` |
| `member_failure_threshold` | `int` | `5` |
| `queue_maxsize` | `int` | `10000` |
| `enqueue_timeout_seconds` | `float` | `0.05` |
| `drop_on_overflow` | `bool` | `False` |
| `batch_size` | `int` | `64` |

### `PersistenceConfig`

| Field | Type | Default |
|---|---|---|
| `enabled` | `bool` | `False` |
| `snapshot_path` | `str` | `"cluster_snapshot.json"` |
| `fsync` | `bool` | `True` |

### `WriteAheadLogConfig`

| Field | Type | Default |
|---|---|---|
| `enabled` | `bool` | `True` |
| `wal_path` | `str` | `"cluster_wal.log"` |
| `fsync_each_write` | `bool` | `False` |
| `checkpoint_interval_operations` | `int` | `500` |

### `ConsensusConfig`

| Field | Type | Default |
|---|---|---|
| `enabled` | `bool` | `True` |
| `election_timeout_seconds` | `float` | `2.0` |
| `heartbeat_interval_seconds` | `float` | `0.5` |
| `leader_lease_seconds` | `float` | `2.0` |
| `require_majority_for_writes` | `bool` | `True` |

### `ConsistencyConfig`

| Field | Type | Default |
|---|---|---|
| `mode` | `ConsistencyMode` | `LINEARIZABLE` |
| `write_timeout_seconds` | `float` | `3.0` |

### `ObservabilityConfig`

| Field | Type | Default |
|---|---|---|
| `enable_http` | `bool` | `False` |
| `host` | `str` | `"127.0.0.1"` |
| `port` | `int` | `8085` |
| `enable_tracing` | `bool` | `True` |
| `trace_history_size` | `int` | `2000` |

### `UpgradeConfig`

| Field | Type | Default |
|---|---|---|
| `min_compatible_protocol_version` | `int` | `1` |
| `max_compatible_protocol_version` | `int` | `1` |

---

## Contributing

### 1) Prerequisites

- Python 3.10+
- (Optional) Redis for plugin backend development

### 2) Setup local editable installs

Core only:

```bash
pip install -e .
```

Core + Redis plugin:

```bash
pip install -e .
pip install -e redis_backend
```

### 3) Development workflow

1. Create/change code under:
   - `distributed_collections/` (core)
   - `redis_backend/src/distributed_collections_redis/` (plugin)
2. Keep public APIs documented with clear docstrings.
3. Add or update integration/scenario coverage where behavior changes.

### 4) Existing validation suites

- Integration test:
  - `tests/integration/test_two_instance_replication.py`
- Scenario harnesses:
  - `tests/scenarios/chaos_runner.py`
  - `tests/scenarios/soak_runner.py`
  - `tests/scenarios/load_runner.py`

### 5) Contribution guidelines

- Keep backward compatibility for public APIs where possible.
- Prefer additive configuration changes over breaking defaults.
- Ensure backend-switching remains ergonomic (`backend="..."` path).
- Keep plugin dependencies optional (core should not require Redis).

---

## Repository layout

```text
distributed_collections/               # core runtime, protocol, config, primitives
redis_backend/                         # separate optional Redis plugin package
  src/distributed_collections_redis/
tests/integration/                     # two-instance integration harness
tests/scenarios/                       # chaos/soak/load scenario scripts
```

---

## License

See `LICENSE`.
