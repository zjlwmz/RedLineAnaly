#!/usr/bin/env python3
"""最小可运行版本：直接算一个面压了多少红线。

    /usr/bin/python3 quickstart.py

和工程里的区别：不用 MQ、不起服务，同一个进程里串行跑完所有省。
调试算法、验证数据、给客户演示单个图斑，用这个最快。
"""
from __future__ import annotations

import json
import sys

from osgeo import ogr

if __package__ is None:
    # 脚本直接运行：python core/quickstart.py（__package__ 为 None）
    # 把父目录加入 sys.path，然后用绝对导入（core.xxx）
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from core import geo
    from core import province
    from core.overlay import overlay_province
    from core.redline_index import RedlineStore  # noqa: F401
else:
    # 作为包导入：from core.quickstart import analyze
    from . import geo
    from . import province
    from .overlay import overlay_province
    from .redline_index import RedlineStore

ogr.UseExceptions()


def analyze(input_geojson: dict, srid: int = 4326, buffer_m: float = 50.0, severe: float = 0.5) -> dict:
    # 1. 坐标统一 + 几何修复
    geom = geo.parse_geojson_geometry(input_geojson, srid)

    # 2. 面积（等积投影，不是 GetArea 的平方度）
    area = geo.area_m2(geom)
    check = geo.area_consistency(geom)   # 与椭球面积交叉校验

    # 3. 动态识别省份（bbox 预筛 → 实际几何相交）
    detect = province.detect_provinces(geom)
    hits = [p for p in detect.provinces if p.match == "geom"]

    # 4. 逐省叠加
    store = RedlineStore()
    prov_results = []
    for p in hits:
        prov_results.append(
            overlay_province(
                input_geom=geom,
                province_code=p.code,
                province_name=p.name,
                buffer_m=buffer_m,
                input_area_m2=area,
                store=store,
            )
        )

    # 5. 汇总：并集口径才是不重复的“真实压盖面积”
    patches: dict[str, dict] = {}
    for r in prov_results:
        for m in r["matches"]:
            item = patches.setdefault(m["patch_id"], {
                "patch_id": m["patch_id"],
                "redline_type": m["redline_type"],
                "overlap_area": 0.0,
            })
            item["overlap_area"] = round(item["overlap_area"] + m["overlap_area"], 2)

    union_area = _union_area([m for r in prov_results for m in r["matches"]])
    sum_area = round(sum(p["overlap_area"] for p in patches.values()), 2)

    if union_area <= 1e-6:
        severity, compliant = "CLEAR", True
    elif union_area / area >= severe:
        severity, compliant = "SEVERE", False
    else:
        severity, compliant = "OCCUPIED", False

    return {
        "is_compliant": compliant,
        "severity": severity,
        "input_area_m2": round(area, 2),
        "area_check": check,
        "provinces": [p.name for p in hits],
        "sum_overlap_area_m2": sum_area,                                  # 各图斑相加（可能 > 输入面）
        "union_overlap_area_m2": round(union_area, 2),                    # 真实压盖面积（推荐）
        "overlap_ratio": round(min(sum_area / area * 100, 100.0), 4),
        "overlap_ratio_union": round(union_area / area * 100, 4),
        "by_type": _group_by_type(list(patches.values()), area),
        "patches": sorted(patches.values(), key=lambda x: -x["overlap_area"]),
        "warnings": detect.warnings,
    }


def _union_area(matches: list[dict]) -> float:
    geoms = [ogr.CreateGeometryFromJson(json.dumps(m["geometry"])) for m in matches if m.get("geometry")]
    geoms = [g for g in geoms if g is not None and not g.IsEmpty()]
    if not geoms:
        return 0.0
    union = geoms[0]
    for g in geoms[1:]:
        union = union.Union(g)
    return geo.area_m2(union)


def _group_by_type(patches: list[dict], area: float) -> list[dict]:
    out: dict[str, dict] = {}
    for p in patches:
        b = out.setdefault(p["redline_type"], {"redline_type": p["redline_type"], "overlap_area": 0.0, "patch_count": 0})
        b["overlap_area"] = round(b["overlap_area"] + p["overlap_area"], 2)
        b["patch_count"] += 1
    for b in out.values():
        b["overlap_ratio"] = round(b["overlap_area"] / area * 100, 4)
    return sorted(out.values(), key=lambda x: -x["overlap_area"])


if __name__ == "__main__":
    # 北京东部山区，横跨京冀两省
    demo = {
        "type": "Polygon",
        "coordinates": [[[116.85, 40.50], [117.20, 40.50], [117.20, 40.75], [116.85, 40.75], [116.85, 40.50]]],
    }
    result = analyze(demo)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    sys.exit(0)
