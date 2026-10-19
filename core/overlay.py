"""面-面叠加计算核心。

与原方案代码示例的关键差异：
1. 不用 `GetArea()` 直接算 —— 在 EPSG:4490 下那是平方度。统一投到 Albers 等积算平方米。
2. 红线按米缓冲（省界处红线被裁剪，不缓冲会漏判骑省界的压盖）。
3. 同一图斑内多块重叠先 union 再求面积，避免「图斑自相交区域被重复累加」。
4. 顺带吐出压盖几何（WGS84 经纬度），前端可直接渲染。
"""
from __future__ import annotations

import time
from typing import Optional

from osgeo import ogr

import geo
from _config import settings
from redline_index import RedlineStore

ogr.UseExceptions()

#: 单个图斑重叠几何的坐标点上限，超过就简化后再回传（前端渲染足够用）
_MAX_OUTPUT_POINTS = 4000


def overlay_province(
    input_geom: ogr.Geometry,
    province_code: str,
    province_name: str,
    buffer_m: float,
    input_area_m2: float,
    store: Optional[RedlineStore] = None,
) -> dict:
    """计算输入面在**单个省份**内的红线压盖情况。"""
    t0 = time.perf_counter()
    store = store or RedlineStore()
    meta = store.layer_meta(province_code)
    warnings: list[str] = []

    if not meta["available"]:
        return {
            "province_code": province_code,
            "province_name": province_name,
            "status": "no_data",
            "patches_scanned": 0,
            "matches": [],
            "warnings": [f"缺少 {province_name} 的红线数据"],
            "elapsed_ms": round((time.perf_counter() - t0) * 1000, 2),
        }

    # 缓冲：将输入面外扩 buffer_m，用于「吃掉」省界裁剪导致的缝隙
    search_geom = geo.buffer_meters(input_geom, buffer_m) if buffer_m > 0 else input_geom
    bbox = search_geom.GetEnvelope()
    # 粗筛容差（度）：缓冲距离的粗略换算，多给 30% 余量
    buf_deg = (buffer_m / 111000.0) * 1.3 + 0.001

    matches: list[dict] = []
    scanned = 0
    for patch in store.iter_patches(province_code, bbox=bbox, buffer_deg=buf_deg):
        scanned += 1
        rl_geom = patch["geometry"]
        if not search_geom.Intersects(rl_geom):
            continue

        # 相交前把红线缓冲一下：只对候选图斑做，代价可控
        work_rl = geo.buffer_meters(rl_geom, buffer_m) if buffer_m > 0 else rl_geom
        if not input_geom.Intersects(work_rl):
            continue

        inter = input_geom.Intersection(work_rl)
        if inter is None or inter.IsEmpty():
            continue

        # 重叠可能是多块，先 union 自身再算面积
        inter = inter.Union(inter) if ogr.GT_Flatten(inter.GetGeometryType()) != ogr.wkbPolygon else inter
        overlap_area = geo.area_m2(inter)
        if overlap_area <= 1e-6:
            continue

        matches.append({
            "patch_id": patch["patch_id"],
            "redline_type": patch["redline_type"],
            "overlap_area": round(overlap_area, 2),
            "overlap_ratio": round(overlap_area / input_area_m2 * 100, 4) if input_area_m2 else 0.0,
            "geometry": _export_wgs84(inter),
        })

    # 该省内按类型汇总
    by_type: dict[str, dict] = {}
    for m in matches:
        b = by_type.setdefault(m["redline_type"], {"redline_type": m["redline_type"], "overlap_area": 0.0, "patch_count": 0})
        b["overlap_area"] += m["overlap_area"]
        b["patch_count"] += 1
    for b in by_type.values():
        b["overlap_area"] = round(b["overlap_area"], 2)
        b["overlap_ratio"] = round(b["overlap_area"] / input_area_m2 * 100, 4) if input_area_m2 else 0.0

    # 该省所有重叠区域的并集面积（避免同类型图斑互相重叠导致重复计数）
    union_area = 0.0
    if matches:
        union_geom = None
        for m in matches:
            g = ogr.CreateGeometryFromJson(_dumps(m["geometry"])) if m["geometry"] else None
            if g is None:
                continue
            union_geom = g if union_geom is None else union_geom.Union(g)
        if union_geom is not None:
            union_area = geo.area_m2(union_geom)

    if scanned and not matches:
        warnings.append("候选图斑均未相交（粗筛命中但精算为空）")

    return {
        "province_code": province_code,
        "province_name": province_name,
        "status": "success",
        "data_version": meta["version"],
        "patches_scanned": scanned,
        "patch_count": len(matches),
        "province_overlap_area": round(sum(m["overlap_area"] for m in matches), 2),
        "province_union_area": round(union_area, 2),
        "by_type": sorted(by_type.values(), key=lambda x: -x["overlap_area"]),
        "matches": matches,
        "warnings": warnings,
        "elapsed_ms": round((time.perf_counter() - t0) * 1000, 2),
    }


def _export_wgs84(inter: ogr.Geometry) -> Optional[dict]:
    """导出为 GeoJSON（WGS84/CGCS2000 经纬度），用于前端渲染。"""
    if inter.IsEmpty():
        return None
    pts = inter.GetGeometryRef(0).GetPointCount() if inter.GetGeometryType() == ogr.wkbPolygon else \
        sum(inter.GetGeometryRef(i).GetGeometryRef(0).GetPointCount() for i in range(inter.GetGeometryCount()))
    out = inter.Clone()
    if pts > _MAX_OUTPUT_POINTS:
        # 简化容差按面积开根号给，保证形状不失真太多
        tol = max((geo.area_m2(inter) ** 0.5) / 3000.0, 1e-5)
        simplified = out.SimplifyPreserveTopology(tol)
        if simplified is not None and not simplified.IsEmpty():
            out = simplified
    return _loads(out.ExportToJson())


def _loads(s: str):
    import json

    return json.loads(s)


def _dumps(o) -> str:
    import json

    return json.dumps(o)


def default_buffer_m() -> float:
    return settings.buffer_m
