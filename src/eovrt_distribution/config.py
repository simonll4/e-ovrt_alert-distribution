from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field

CooldownKey = Literal["condition_id", "source_id", "subject_key"]


class StrictConfigModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PolicyConfig(StrictConfigModel):
    cooldown_ms: float = Field(default=30000.0, ge=0, allow_inf_nan=False)
    key: list[CooldownKey] = Field(
        default_factory=lambda: ["condition_id", "source_id"], min_length=1
    )


class ChannelConfig(StrictConfigModel):
    mode: Literal["dry_run", "live"] = "dry_run"
    host: str = Field(default="127.0.0.1", min_length=1)
    port: int = Field(default=1883, ge=1, le=65535)
    topic_prefix: str = Field(default="eovrt/alerts", min_length=1, pattern=r"^[^#+\x00]+$")
    qos: Literal[1] = 1


class RetryConfig(StrictConfigModel):
    max_attempts: int = Field(default=3, ge=1)
    wait_ms: float = Field(default=500.0, ge=0, allow_inf_nan=False)


class DistributionConfig(StrictConfigModel):
    notification_policy: PolicyConfig = Field(default_factory=PolicyConfig)
    channel: ChannelConfig = Field(default_factory=ChannelConfig)
    retry: RetryConfig = Field(default_factory=RetryConfig)

    @classmethod
    def load(cls, path: Path | None) -> DistributionConfig:
        if path is None:
            return cls()
        data = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
        return cls.model_validate(data)
