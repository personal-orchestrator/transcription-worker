# JetStream consumer configuration

The durable consumer is created with explicit settings rather than NATS server defaults, which
are a poor fit for this workload — a Polish file takes two rate-limited Groq calls (English takes
one), far longer than the default 30s `ack_wait`. The values are constants in `app/main.py` and
`app/workers/transcription.py`:

| setting | value | why |
| --- | --- | --- |
| `ack_wait` | 300s | a **death-detection window**, not a duration budget — the heartbeat covers duration |
| `max_deliver` | 3 | a finite redelivery ceiling; the server default of `-1` retries forever |
| `max_ack_pending` | 1 | the worker drains messages serially at `replicas: 1` |
| progress interval | 20s | how often `msg.in_progress()` (`+WPI`) resets the ack timer while a file is in flight |

**Known limitation.** The heartbeat has no expiry, so a call that hangs rather than fails would
hold its message and — since `nats-py` dispatches a subscription's callbacks serially — block the
consumer, recoverable only by restarting the pod. Every Groq call here is bounded by the SDK's
60s-per-attempt default, so the exposure is small; `ai-worker` builds its client without a
timeout and is the one that carries this risk. Bounding the heartbeat is not the answer either
way: it would release the ack claim while the work continued and duplicate the message.
