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
than the default 30s `ack_wait`. The values are constants in `app/main.py`:

| setting | value | why |
| --- | --- | --- |
| `ack_wait` | 300s | a **death-detection window**, not a duration budget — see below |
| `max_deliver` | 3 | a finite redelivery ceiling; the server default of `-1` retries forever |
| `max_ack_pending` | 1 | the worker drains messages serially at `replicas: 1` |
| progress interval | 20s | how often `msg.in_progress()` (`+WPI`) resets the ack timer while a file is in flight |

**`ack_wait` is not sized to cover the work.** It cannot be: counting the Groq SDK's own retries
inside each `tenacity` attempt, a non-English file has a worst case on the order of half an hour.
`ack_wait` is how long the server waits after the last sign of life before assuming the worker
died. The heartbeat is what covers duration — `+WPI` resets the timer without counting as a
delivery, so a slow transcription stays on its first delivery instead of being handed out again
and re-transcribed. If the worker dies the beats stop, `ack_wait` expires, and the message is
redelivered, which is the behaviour you want.

The progress interval is 20s rather than something sized against our own 300s because it also has
to beat the **30s server default** that is in force until the step below has been run. A failed
beat is logged and retried on the next tick; one transient publish error during a reconnect must
not retire the protection for the rest of the file.

### Applying this to an existing consumer — run this first

`nats-py`'s `js.subscribe()` only applies the config when the durable consumer does not yet
exist; an existing one keeps whatever the server holds, and the code does not detect or repair
that. **On any cluster where the consumer already exists, this one-off command is the entire
fix** — the code change only governs consumers created from scratch afterwards.

```bash
nats consumer edit audio_events transcription-worker-consumer \
  --ack-wait=5m --max-deliver=3 --max-pending=1
```

Run it **before** deploying, not after. Deploying first leaves an interim window still running
`max_ack_pending=1000`, which is the setting responsible for most of the redelivery: the server
pushes a 1000-deep batch that the worker drains one at a time while every queued message's ack
timer runs down.

Use `edit` rather than delete-and-recreate so the consumer keeps its ack floor and does not
replay the stream from the start. Verify with `nats consumer info audio_events
transcription-worker-consumer` — the startup log deliberately does **not** print the effective
settings, because `js.subscribe()` cannot know them; `consumer info` is the only source of truth.
`Redelivered Messages` should stay near zero under normal load.
