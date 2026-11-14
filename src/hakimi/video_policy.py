"""Independent, server-owned limits for opt-in video-file analysis."""

from pydantic import BaseModel, ConfigDict, Field


class VideoWatchConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    enabled: bool = False
    max_seconds: int = Field(default=480, ge=30, le=480)
    daily_limit: int = Field(default=3, ge=0, le=20)

    @classmethod
    def from_settings(cls, settings):
        return cls(
            enabled=settings.enable_video_file_watch,
            max_seconds=settings.max_video_watch_seconds,
            daily_limit=settings.daily_video_watch_limit,
        )
