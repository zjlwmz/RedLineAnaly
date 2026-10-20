"""省份识别 + 按省拆分任务（Master 侧）。

调用 core.province.detect_provinces 做 bbox 预筛 + 实际几何相交二级判断，
返回涉及的省份列表（只取 match='geom' 的，bbox-only 的跳过）。
"""
from __future__ import annotations

import sys
from pathlib import Path

# 确保 core 包可被导入（config 锚定工程根目录）
_PKG_DIR = Path(__file__).resolve().parent.parent
if str(_PKG_DIR) not in sys.path:
    sys.path.insert(0, str(_PKG_DIR))

from osgeo import ogr  # noqa: E402

from core import province as province_mod  # noqa: E402

ogr.UseExceptions()


def split_provinces(input_geom: ogr.Geometry) -> list[dict]:
    """识别输入面跨了哪些省，返回可投递任务的省份列表。

    返回: [{"code": "110000", "name": "北京市"}, ...]  按面积占比降序
    """
    detect = province_mod.detect_provinces(input_geom)
    hits = [p for p in detect.provinces if p.match == "geom"]
    return [{"code": h.code, "name": h.name} for h in hits]
