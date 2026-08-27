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

**Known limitation.** The heartbeat has no expiry, so a call that hangs rather than fails would
hold its message and — since `nats-py` dispatches a subscription's callbacks serially — block the
consumer, recoverable only by restarting the pod. Every Groq call here is bounded by the SDK's
60s-per-attempt default, so the exposure is small; `ai-worker` builds its client without a
timeout and is the one that carries this risk. Bounding the heartbeat is not the answer either
way: it would release the ack claim while the work continued and duplicate the message.

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
message already past three deliveries as it comes up for redelivery — against the measured
backlog, all of them — so apply it only once that set has drained.

Check with `nats consumer info audio_events transcription-worker-consumer` and watch
**`Outstanding Acks`** (`num_ack_pending`) fall to zero. Watch that field, not `Unprocessed
Messages`: the latter is `num_pending`, the messages never delivered at all, and while the
consumer is saturated at `max_ack_pending=1000` it reads zero even with the whole backlog sitting
in the redelivery set. Reading it as "drained" is how you lose the backlog.

Use `edit` rather than delete-and-recreate so the consumer keeps its ack floor and does not replay
the stream from the start. `nats consumer info` is the source of truth for what is actually in
force; `Redelivered Messages` should stay near zero under normal load.
