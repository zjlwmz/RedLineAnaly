"""红线压盖计算入口（Slave 侧调用）。

包装 core/ 的计算核心：解析任务消息中的 WKT → 逐省 overlay_province → 回传结果 JSON。
这样 Slave 只需要调用 run_task()，不用关心 core 的 import 细节。
"""
from __future__ import annotations

import sys
from pathlib import Path

# 确保 core 包可被导入
_PKG_DIR = Path(__file__).resolve().parent.parent
if str(_PKG_DIR) not in sys.path:
    sys.path.insert(0, str(_PKG_DIR))

from osgeo import ogr  # noqa: E402

from core import geo as geo_mod  # noqa: E402
from core.overlay import overlay_province  # noqa: E402
from core.redline_index import RedlineStore  # noqa: E402

ogr.UseExceptions()


def run_task(task: dict) -> dict:
    """执行单省红线压盖计算。

    Args:
        task: 来自 MQ 的任务消息，必须包含：
          input_wkt, input_srid, province_code, province_name, input_area_m2, buffer_m

    Returns:
        可直接作为 Slave 回传结果的 dict
    """
    input_wkt = task["input_wkt"]
    input_srid = int(task.get("input_srid", 4490))
    province_code = task["province_code"]
    province_name = task.get("province_name", province_code)
    input_area_m2 = float(task.get("input_area_m2") or 0.0)
    buffer_m = float(task.get("buffer_m") or 50.0)

    # 解析 WKT → CGCS2000 经纬度
    input_geom = geo_mod.parse_geojson_geometry(input_wkt, input_srid)

    store = RedlineStore()
    return overlay_province(
        input_geom=input_geom,
        province_code=province_code,
        province_name=province_name,
        buffer_m=buffer_m,
        input_area_m2=input_area_m2,
        store=store,
    )
