import pytest
from app.services.transcription import TranscriptionResult

def test_transcription_result_to_dict():
    res = TranscriptionResult(text="Hello world", language="en")
    assert res.to_dict() == {"text": "Hello world", "language": "en"}
