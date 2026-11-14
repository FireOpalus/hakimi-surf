from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class ImageRef(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    url: str = Field(default="", max_length=4096)
    # Opaque media id from an authenticated OneBot segment, never a local path.
    file: str = Field(default="", max_length=1024)
    file_size: int | None = Field(default=None, ge=0)
    sender_id: str = Field(default="", max_length=128)
    sender_nickname: str = Field(default="", max_length=80)
    origin: str = Field(default="", max_length=40)

    @property
    def identity(self) -> str:
        return self.file or self.url


class Message(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    platform: Literal["onebot", "qq"]
    bot_id: str = Field(min_length=1, max_length=64)
    event_id: str = Field(min_length=1, max_length=256)
    user_id: str = Field(min_length=1, max_length=128)
    kind: Literal["private", "group"]
    conversation_id: str = Field(min_length=1, max_length=128)
    text: str = Field(max_length=8000)
    addressed: bool
    created_at: float
    sender_nickname: str = Field(default="", max_length=80)
    group_name: str = Field(default="", max_length=100)
    context_expanded: bool = False
    # Only direct text may invoke commands; quoted/card/forward text never can.
    command_text: str | None = Field(default=None, max_length=8000)
    # Only genuine top-level OneBot at segments; never inferred from text or forwards.
    mentioned_ids: list[str] = Field(default_factory=list, max_length=32)
    # Reply ids come only from top-level protocol segments; authors are verified locally.
    reply_ids: list[str] = Field(default_factory=list, max_length=2)
    attention_reason: Literal["", "private", "mention", "alias", "bilibili", "reply"] = ""
    images: list[ImageRef] = Field(default_factory=list, max_length=2)
    forward_ids: list[str] = Field(default_factory=list, max_length=2)
    video_ids: list[str] = Field(default_factory=list, max_length=3)
    short_urls: list[str] = Field(default_factory=list, max_length=3)

    def session_parts(self, namespace: str) -> list[str]:
        return [namespace, self.platform, self.bot_id, self.kind, self.conversation_id]

    def scope_parts(self, namespace: str) -> list[str]:
        return [
            namespace,
            self.platform,
            self.bot_id,
            self.kind,
            self.conversation_id,
            self.user_id,
        ]

    def subject_parts(self, namespace: str) -> list[str]:
        return [namespace, self.platform, self.bot_id, self.user_id]


class Video(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    bvid: str = Field(pattern=r"^BV[1-9A-HJ-NP-Za-km-z]{10}$")
    mid: str = Field(pattern=r"^[1-9][0-9]{0,19}$")
    title: str = Field(max_length=200)
    description: str = Field(max_length=2000)
    author: str = Field(max_length=100)
    published_at: int

    @property
    def url(self) -> str:
        return f"https://www.bilibili.com/video/{self.bvid}"
