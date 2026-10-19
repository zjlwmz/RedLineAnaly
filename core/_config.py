"""单文件版配置：只保留数据路径，避免依赖完整工程的 app 包。"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DATA = BASE_DIR.parent / "data"


@dataclass(frozen=True)
class Settings:
    data_dir: Path = Path(os.environ.get("REDLINE_DATA_DIR", str(DEFAULT_DATA)))
    #: 严重压盖阈值（并集压盖面积 / 输入面面积），按项目标准调整
    severe_ratio: float = float(os.environ.get("REDLINE_SEVERE_RATIO", "0.5"))
    #: 红线缓冲距离（米），规避省界裁剪导致的漏判
    buffer_m: float = float(os.environ.get("REDLINE_BUFFER_M", "50"))

    @property
    def redline_dir(self) -> Path:
        return self.data_dir / "redline"

    @property
    def province_pkg(self) -> Path:
        return self.data_dir / "province" / "provinces.gpkg"

    @property
    def china_outline(self) -> Path:
        return self.data_dir / "province" / "china_outline.gpkg"


settings = Settings()
