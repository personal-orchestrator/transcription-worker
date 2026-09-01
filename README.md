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

The durable consumer overrides the NATS server defaults, which do not suit slow, rate-limited
transcription work. See
[documentation/jetstream-consumer-configuration.md](documentation/jetstream-consumer-configuration.md)
for the values, the reasoning, and the one-time steps for a consumer that already exists.
