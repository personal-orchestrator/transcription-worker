# Transcription Worker

Dedicated, isolated audio transcription worker for the personal orchestrator platform.

## Features
- Listens for `audio.ingested` NATS JetStream events.
- Performs generic transcription and translation (`Audio -> Text`).
  - *Current implementation*: Groq Whisper API (`whisper-large-v3`).
  - *Extensible design*: Pluggable `TranscriptionService` interface designed to support future local Whisper models (e.g. `faster-whisper` / local GPU execution).
- Persists raw and translated transcripts into daily `.jsonl` files on disk (`/data/transcriptions-raw` and `/data/transcriptions`).
- Emits `transcription.completed` events to NATS JetStream for downstream agent processing.
- Includes a reindexing service to automatically scan and process untranscribed audio files.

## JetStream consumer configuration

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

The interval is 20s rather than something sized against our own 300s because it also has to beat
the **30s server default** in force until the step below has been run.

**Known limitation.** The heartbeat has no expiry, so a call that hangs rather than fails holds
its message indefinitely. Capping it would not help: `nats-py` dispatches a subscription's
callbacks serially, so a hung call blocks the consumer at the client whatever the server does
with the ack — a cap would only release the claim while the work continued, producing a duplicate
when it finally returned. Recovering from a hang means restarting the pod.

### Applying this to an existing consumer

`nats-py`'s `js.subscribe()` only applies the config when the durable does not yet exist; an
existing one keeps whatever the server holds, and the code does not detect or repair that. The
heartbeat ships in code and applies either way, but the three consumer settings do not — on an
existing cluster they arrive only via:

```bash
nats consumer edit audio_events transcription-worker-consumer \
  --ack-wait=5m --max-pending=1
nats consumer edit audio_events transcription-worker-consumer --max-deliver=3
```

**Two commands, in that order, and check the backlog between them.** `--ack-wait` and
`--max-pending` stop the amplification without dropping anything. `--max-deliver=3` discards every
pending message already past three deliveries — against the measured backlog, all of them — as
each comes up for redelivery, so apply it only once the backlog has drained (`nats consumer info
audio_events transcription-worker-consumer`, watch `Unprocessed Messages` fall).

Use `edit` rather than delete-and-recreate so the consumer keeps its ack floor and does not replay
the stream from the start. `nats consumer info` is the source of truth for what is actually in
force; `Redelivered Messages` should stay near zero under normal load.
