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
are a poor fit for this workload — a single file takes two rate-limited Groq calls, far longer
than the default 30s `ack_wait`:

| setting | value | env var | why |
| --- | --- | --- | --- |
| `ack_wait` | 300s | `NATS_ACK_WAIT` | worst case is two Groq calls, each retried up to 5× behind the rate limiter |
| `max_deliver` | 3 | `NATS_MAX_DELIVER` | a finite redelivery ceiling; the server default of `-1` retries forever |
| `max_ack_pending` | 1 | `NATS_MAX_ACK_PENDING` | the worker drains messages serially at `replicas: 1` |
| progress interval | 60s | `NATS_PROGRESS_INTERVAL` | how often `msg.in_progress()` (`+WPI`) resets the ack timer while a file is in flight |

The heartbeat is what actually protects long files: `+WPI` resets `ack_wait` without counting as
a delivery, so a slow transcription stays on its first delivery instead of being handed out
again and re-transcribed. If the worker dies, the heartbeats stop and the message is redelivered
as normal.

**Applying this to an existing consumer.** `nats-py`'s `js.subscribe()` only applies the config
when the durable consumer does not yet exist; an existing one keeps whatever the server holds.
Consumers created before this change need a one-off:

```bash
nats consumer edit audio_events transcription-worker-consumer \
  --ack-wait=5m --max-deliver=3 --max-pending=1
```

Use `edit` rather than delete-and-recreate so the consumer keeps its ack floor and does not
replay the stream from the start. Verify with `nats consumer info audio_events
transcription-worker-consumer` — `Redelivered Messages` should stay near zero under normal load.
