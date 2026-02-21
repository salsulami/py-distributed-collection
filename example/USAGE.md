# Example project usage

This directory is an installable project that demonstrates the core library end-to-end.

## Install

From repository root:

```bash
pip install -e .
pip install -e example
```

Or install FastAPI example dependencies directly:

```bash
pip install -r example/requirements.txt
```

Optional Redis backend:

```bash
pip install -e redis_backend
pip install -e "example[redis]"
```

## Run demo (all collection types)

Default (in-memory backend):

```bash
dpc-example
```

Redis centralized backend:

```bash
dpc-example --backend redis --redis-url redis://127.0.0.1:6379/0
```

The demo starts two nodes and showcases:

- Distributed map / hash map
- Distributed list
- Distributed queue
- Distributed topic

You can also customize ports and consistency mode:

```bash
dpc-example --port-a 5811 --port-b 5812 --consistency quorum
```

## Run FastAPI replication example across multiple instances

Install example package:

```bash
pip install -e example
```

Notes:

- If `DPC_API_PORT` is unset, the app auto-selects the first free API port starting from `8000`.
- If `DPC_BIND_PORT` is unset, the app auto-selects the first free cluster TCP port starting from `5711`.
- Default `DPC_DISCOVERY` is `both` (multicast + static fallback), using node IP for multicast identity.
- If `DPC_STATIC_SEEDS` is unset, default local fallback seeds are generated from ports `5711..5726`.
- App retries `join_cluster()` periodically so late-starting nodes can still be discovered.
- For deterministic static discovery, set `DPC_DISCOVERY=static` and provide explicit `DPC_STATIC_SEEDS`.

Start instance A (terminal 1):

```bash
DPC_API_PORT=8001 \
DPC_BIND_PORT=5711 \
DPC_DISCOVERY=static \
DPC_STATIC_SEEDS=127.0.0.1:5712 \
dpc-example-api
```

Start instance B (terminal 2):

```bash
DPC_API_PORT=8002 \
DPC_BIND_PORT=5712 \
DPC_DISCOVERY=static \
DPC_STATIC_SEEDS=127.0.0.1:5711 \
dpc-example-api
```

Write through instance A and read from instance B:

```bash
curl -X PUT "http://127.0.0.1:8001/map/users/u-1" \
  -H "content-type: application/json" \
  -d '{"value":{"name":"Alice","role":"admin"}}'

curl "http://127.0.0.1:8002/map/users"
```

Queue/list replication checks:

```bash
curl -X POST "http://127.0.0.1:8001/queue/jobs/offer" \
  -H "content-type: application/json" \
  -d '{"value":"job-1"}'

curl "http://127.0.0.1:8002/queue/jobs"

curl -X POST "http://127.0.0.1:8001/list/events/append" \
  -H "content-type: application/json" \
  -d '{"value":{"kind":"created","id":"u-1"}}'

curl "http://127.0.0.1:8002/list/events"
```

Topic broadcast check:

```bash
curl -X POST "http://127.0.0.1:8001/topic/alerts/publish" \
  -H "content-type: application/json" \
  -d '{"message":{"level":"info","message":"replication works"}}'

curl "http://127.0.0.1:8002/topic/alerts/messages"
```
