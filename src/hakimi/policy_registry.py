"""Centralized Pydantic v2 policy schema registry and metadata exporter."""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel

from .admission_policy import AdmissionConfig
from .budget_policy import BudgetConfig
from .compression_policy import CompressionConfig
from .debounce_policy import DebounceConfig
from .limits_policy import (
    ContentToolsConfig,
    GenerationCapsConfig,
    ImageLimitsConfig,
    PatrolLimitConfig,
    RateLimitConfig,
)
from .rest_policy import RestHoursConfig
from .thinking_policy import ThinkingConfig
from .video_policy import VideoWatchConfig
from .web_search import WebSearchConfig


@dataclass(frozen=True)
class PolicySpec:
    """Declarative metadata linking a policy model to its runtime slot, lock, and route."""

    key: str
    model: type[BaseModel]
    route: str
    lock: str
    title: str
    category: str
    attr: str = ""
    simple_limit: bool = False
    partial_update: bool = False
    settings_factory: Callable[[Any], BaseModel] | None = None


def _image_limits_from_settings(settings: Any) -> ImageLimitsConfig:
    return ImageLimitsConfig(
        image_max_bytes=max(1_048_576, settings.image_max_bytes),
        image_max_pixels=settings.image_max_pixels,
        image_standard_max_edge=settings.image_standard_max_edge,
        image_tall_max_edge=settings.image_tall_max_edge,
    )


_SPECS: tuple[PolicySpec, ...] = (
    PolicySpec(
        key="rate_limits",
        model=RateLimitConfig,
        route="/rate-limits",
        lock="limits",
        title="速率与发送延迟限制",
        category="limits",
        attr="rate_limits",
        simple_limit=True,
    ),
    PolicySpec(
        key="generation_caps",
        model=GenerationCapsConfig,
        route="/generation-caps",
        lock="limits",
        title="模型输出长度上限",
        category="limits",
        attr="generation_caps",
        simple_limit=True,
        partial_update=True,
    ),
    PolicySpec(
        key="patrol_limits",
        model=PatrolLimitConfig,
        route="/patrol-limits",
        lock="limits",
        title="B 站巡逻限额与频率",
        category="limits",
        attr="patrol_limits",
        simple_limit=True,
    ),
    PolicySpec(
        key="image_limits",
        model=ImageLimitsConfig,
        route="/image-limits",
        lock="limits",
        title="入站图片像素与体积限制",
        category="media",
        attr="image_limits",
        simple_limit=True,
        settings_factory=_image_limits_from_settings,
    ),
    PolicySpec(
        key="content_tools",
        model=ContentToolsConfig,
        route="/content-tools",
        lock="limits",
        title="文件读取与富文本卡片策略",
        category="media",
        attr="content_tools",
        simple_limit=True,
    ),
    PolicySpec(
        key="budget",
        model=BudgetConfig,
        route="/budget",
        lock="budget",
        title="每日/月度预算限额",
        category="budget",
    ),
    PolicySpec(
        key="rest_hours",
        model=RestHoursConfig,
        route="/rest-hours",
        lock="rest_hours",
        title="作息与静默时段",
        category="life",
    ),
    PolicySpec(
        key="admission",
        model=AdmissionConfig,
        route="/admission",
        lock="admission",
        title="准入名单与 Bot 交互阈值",
        category="safety",
    ),
    PolicySpec(
        key="compression",
        model=CompressionConfig,
        route="/compression",
        lock="compression",
        title="上下文窗口与记忆压缩",
        category="memory",
    ),
    PolicySpec(
        key="debounce",
        model=DebounceConfig,
        route="/debounce",
        lock="debounce",
        title="群聊与提及防抖时间",
        category="limits",
    ),
    PolicySpec(
        key="thinking",
        model=ThinkingConfig,
        route="/thinking",
        lock="thinking",
        title="各场景推理深度",
        category="routing",
    ),
    PolicySpec(
        key="web_search",
        model=WebSearchConfig,
        route="/web-search",
        lock="web_search",
        title="联网搜索配额与开关",
        category="search",
    ),
    PolicySpec(
        key="video_watch",
        model=VideoWatchConfig,
        route="/video-watch",
        lock="video_watch",
        title="视频文件直读限制",
        category="media",
    ),
)

POLICY_REGISTRY: dict[str, PolicySpec] = {spec.key: spec for spec in _SPECS}
SIMPLE_LIMIT_SPECS: tuple[PolicySpec, ...] = tuple(spec for spec in _SPECS if spec.simple_limit)
SIMPLE_LIMIT_BY_KEY: dict[str, PolicySpec] = {spec.key: spec for spec in SIMPLE_LIMIT_SPECS}


def build_env_policy(spec: PolicySpec, settings: Any) -> BaseModel:
    """Build the validated environment-default policy model for a PolicySpec."""
    if spec.settings_factory is not None:
        return spec.settings_factory(settings)
    return spec.model.from_settings(settings)


def validate_registry_against_settings(settings: Any) -> dict[str, BaseModel]:
    """Instantiate and validate every registered policy against the given settings object."""
    return {key: build_env_policy(spec, settings) for key, spec in POLICY_REGISTRY.items()}


def export_policy_schemas(settings: Any | None = None) -> dict[str, Any]:
    """Export JSON Schemas and optional env defaults for all registered runtime policies."""
    policies: dict[str, Any] = {}
    for key, spec in POLICY_REGISTRY.items():
        entry: dict[str, Any] = {
            "key": spec.key,
            "title": spec.title,
            "category": spec.category,
            "route": f"/admin/api{spec.route}",
            "lock": spec.lock,
            "partial_update": spec.partial_update,
            "schema": spec.model.model_json_schema(),
        }
        if settings is not None:
            entry["defaults"] = build_env_policy(spec, settings).model_dump(mode="json")
        policies[key] = entry
    return {"version": 1, "policies": policies}
