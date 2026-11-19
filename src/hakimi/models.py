from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

PlatformId = Annotated[str, Field(pattern=r"^[a-z][a-z0-9_]{0,47}$")]


class ImageRef(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    url: str = Field(default="", max_length=4096)
    # Opaque media id from an authenticated OneBot segment, never a local path.
    file: str = Field(default="", max_length=1024)
    file_size: int | None = Field(default=None, ge=0)
    sender_id: str = Field(default="", max_length=128)
    sender_nickname: str = Field(default="", max_length=80)
    origin: str = Field(default="", max_length=40)
    is_sticker: bool = False

    @property
    def identity(self) -> str:
        return self.file or self.url


class FileRef(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    name: str = Field(default="", max_length=255)
    file_id: str = Field(default="", max_length=1024)
    url: str = Field(default="", max_length=4096)
    file_size: int | None = Field(default=None, ge=0)
    busid: int | None = Field(default=None, ge=0)
    sender_id: str = Field(default="", max_length=128)
    sender_nickname: str = Field(default="", max_length=80)
    origin: str = Field(default="", max_length=40)

    @property
    def identity(self) -> str:
        return self.file_id or self.url or self.name


class Message(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    platform: PlatformId
    bot_id: str = Field(min_length=1, max_length=64)
    event_id: str = Field(min_length=1, max_length=256)
    user_id: str = Field(min_length=1, max_length=128)
    kind: Literal["private", "group"]
    conversation_id: str = Field(min_length=1, max_length=128)
    text: str = Field(max_length=24000)
    addressed: bool
    created_at: float
    sender_nickname: str = Field(default="", max_length=80)
    group_name: str = Field(default="", max_length=100)
    context_expanded: bool = False
    # Resolver diagnostics are data in C, never bracketed prose in the chat/history body.
    context_status: list[Literal["reply_unavailable", "forward_unavailable", "truncated"]] = Field(
        default_factory=list, max_length=3
    )
    # Only direct text may invoke commands; quoted/card/forward text never can.
    command_text: str | None = Field(default=None, max_length=24000)
    # Only genuine top-level OneBot at segments; never inferred from text or forwards.
    mentioned_ids: list[str] = Field(default_factory=list, max_length=32)
    # Reply ids come only from top-level protocol segments; authors are verified locally.
    reply_ids: list[str] = Field(default_factory=list, max_length=2)
    attention_reason: Literal[
        "", "private", "mention", "alias", "possible_alias", "bilibili", "reply", "command"
    ] = ""
    images: list[ImageRef] = Field(default_factory=list, max_length=2)
    files: list[FileRef] = Field(default_factory=list, max_length=2)
    forward_ids: list[str] = Field(default_factory=list, max_length=4)
    video_ids: list[str] = Field(default_factory=list, max_length=5)
    short_urls: list[str] = Field(default_factory=list, max_length=5)

    @property
    def has_possible_qq_alias(self) -> bool:
        return (
            self.platform == "onebot"
            and self.kind == "group"
            and self.attention_reason == "possible_alias"
        )

    @property
    def invites_group_review(self) -> bool:
        return self.addressed or self.has_possible_qq_alias

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
