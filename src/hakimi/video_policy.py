"""Independent, server-owned limits for opt-in video-file analysis."""

from pydantic import BaseModel, ConfigDict, Field

MAX_VIDEO_SECONDS = 15 * 60


class VideoWatchConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    enabled: bool = False
    max_seconds: int = Field(default=MAX_VIDEO_SECONDS, ge=30, le=MAX_VIDEO_SECONDS)
    daily_limit: int = Field(default=3, ge=0, le=20)

    @classmethod
    def from_settings(cls, settings):
        return cls(
            enabled=settings.enable_video_file_watch,
            max_seconds=settings.max_video_watch_seconds,
            daily_limit=settings.daily_video_watch_limit,
        )
