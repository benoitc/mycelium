# Tutorials

End-to-end walkthroughs. Each tutorial builds something real;
you should be able to follow it from a clean checkout and end up
with a running example.

## In this section

- [Hello, cluster](hello-cluster.md) — the minimal two-node
  walkthrough. If you have not done [Getting started](../overview/getting-started.md)
  yet, do that first; this tutorial assumes a working cluster.
- [Create an application](create-an-application.md) — build a
  minimal OTP app from scratch: a worker that registers a service
  and an API that discovers and calls it on any node. Runnable
  source under [`examples/quickstart`](../../examples/quickstart/README.md).
- [Distributed chat](distributed-chat.md) — a small chat
  application that uses the service registry, service events,
  and `gen_server` patterns over the mycelium dist channel.
  The full source is under [`examples/chat`](../../examples/chat/README.md).

## After the tutorials

- [Concepts](../concepts/index.md) explains *why* each piece
  works the way it does.
- [How-to guides](../how-to/index.md) cover the operational
  side: production, observability, troubleshooting.
- [Reference](../reference/index.md) is the API surface and the
  configuration list.
