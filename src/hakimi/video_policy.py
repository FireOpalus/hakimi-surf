"""Independent, server-owned limits for opt-in video-file analysis."""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

MAX_VIDEO_SECONDS = 15 * 60
VideoDailyLimit = Annotated[int, Field(ge=0, le=20)] | Literal["unlimited"]


class VideoWatchConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    enabled: bool = False
    max_seconds: int = Field(default=MAX_VIDEO_SECONDS, ge=30, le=MAX_VIDEO_SECONDS)
    daily_limit: VideoDailyLimit = 3

    @classmethod
    def from_settings(cls, settings):
        return cls(
            enabled=settings.enable_video_file_watch,
            max_seconds=settings.max_video_watch_seconds,
            daily_limit=settings.daily_video_watch_limit,
        )
