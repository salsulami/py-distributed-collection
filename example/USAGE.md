# Example project usage

This directory is an installable project that demonstrates the core library end-to-end.

## Install

From repository root:

```bash
pip install -e .
pip install -e example
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
