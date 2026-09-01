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

The durable consumer sets `ack_wait`, `max_deliver` and `max_ack_pending` explicitly instead of
taking the NATS server defaults, and holds the ack timer open with an `msg.in_progress()`
heartbeat while a file is transcribed. The values, the reasoning behind them and the one-time
command that applies them to a consumer which already exists are in
[documentation/jetstream-consumer-configuration.md](documentation/jetstream-consumer-configuration.md).
