from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    storage_dir: str = "/data/recordings"
    transcriptions_raw_dir: str = "/data/transcriptions-raw"
    transcriptions_dir: str = "/data/transcriptions"
    nats_url: str = "nats://localhost:4222"
    nats_subject: str = "audio.ingested"
    nats_transcriptions_subject: str = "transcription.completed"
    groq_api_key: str
    groq_rate_limit_per_minute: int = 10

    # JetStream consumer tuning. The server defaults (ack_wait=30s, max_deliver=-1,
    # max_ack_pending=1000) are wrong for this workload: a single file needs two rate-limited
    # Groq calls, so the 30s ack timer expires while the message is still being worked on and
    # unlimited redelivery turns every expiry into another billed transcription.
    nats_ack_wait: float = 300.0
    nats_max_deliver: int = 3
    nats_max_ack_pending: int = 1
    # How often to send +WPI while a file is in flight. Must stay well below nats_ack_wait.
    nats_progress_interval: float = 60.0

    log_level: str = "INFO"
    reindex_poll_interval: int = 60
    
    model_config = SettingsConfigDict(env_file=".env.secrets", env_file_encoding="utf-8", extra="ignore")

settings = Settings()  # type: ignore[call-arg]
