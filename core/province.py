"""动态省份识别。

原方案只做「bbox 与省界相交」，有两个坑：
  a) bbox 跨省、但面实际只在一个省内 → 会多拆出 N 个空任务；
  b) 面贴着省界时，bbox 相交但面本身不相交 → 漏掉该省。
本实现：省份先按包围盒快速排除，再做**包围盒相交**和**实际几何相交**两级判定，
两者结果不一致时记录下来，便于排查数据（省界简化误差）问题。
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional

from osgeo import ogr

from . import geo
from ._config import settings

ogr.UseExceptions()


@dataclass
class ProvinceHit:
    code: str
    name: str
    match: str  # bbox（仅包围盒相交）/ geom（实际几何相交）
    area_ratio: float = 0.0


@dataclass
class DetectResult:
    provinces: list[ProvinceHit] = field(default_factory=list)
    outside_china: bool = False
    bbox: tuple[float, float, float, float] = (0, 0, 0, 0)
    elapsed_ms: float = 0.0
    warnings: list[str] = field(default_factory=list)


_cache: Optional[list[dict]] = None
_outline_cache: Optional[ogr.Geometry] = None


def _load_outline() -> ogr.Geometry:
    """国界几何。必须 Clone 后及时释放 DataSource，否则拿到的引用已经在析构队列里。"""
    global _outline_cache
    if _outline_cache is None:
        ds = ogr.Open(str(settings.china_outline), 0)
        feat = ds.GetLayer(0).GetNextFeature()
        _outline_cache = feat.GetGeometryRef().Clone()
        feat = None
        ds = None
    return _outline_cache


def load_provinces() -> list[dict]:
    global _cache
    if _cache is not None:
        return _cache
    path = settings.province_pkg
    if not path.exists():
        raise RuntimeError(f"缺少省级边界数据: {path}，请先执行 scripts/gen_data.py")
    ds = ogr.Open(str(path), 0)
    lyr = ds.GetLayer(0)
    items = []
    repaired = []
    for feat in lyr:
        g = feat.GetGeometryRef()
        if g is None:
            continue
        g = g.Clone()
        # 行政边界数据（尤其在线下载的）普遍存在自相交，直接用 GEOS 做 Intersection 会抛
        # TopologyException。加载时统一 MakeValid，一次修复、全局受益。
        if not g.IsValid():
            fixed = g.MakeValid()
            if fixed is not None and not fixed.IsEmpty():
                g = fixed
                repaired.append(str(feat.GetField("name")))
        items.append({
            "code": str(feat.GetField("code")),
            "name": str(feat.GetField("name")),
            "geom": g,
            "env": g.GetEnvelope(),
            "valid": g.IsValid(),
        })
    ds = None
    if repaired:
        print(f"[province] 已修复无效几何: {'、'.join(repaired)}", flush=True)
    _cache = items
    return items


def detect_provinces(geom: ogr.Geometry) -> DetectResult:
    t0 = time.perf_counter()
    result = DetectResult()
    env = geom.GetEnvelope()
    result.bbox = env
    env_geom = geo.envelope_polygon(geom)
    total_area = geo.area_m2(geom) or 1.0

    for p in load_provinces():
        pminx, pmaxx, pminy, pmaxy = p["env"]
        # 一级：包围盒相交（O(1)，全国 34 个省也毫无压力）
        if env[1] < pminx or env[0] > pmaxx or env[3] < pminy or env[2] > pmaxy:
            continue
        if not env_geom.Intersects(p["geom"]):
            continue
        # 二级：实际几何相交
        if geom.Intersects(p["geom"]):
            inter = geom.Intersection(p["geom"])
            ratio = geo.area_m2(inter) / total_area if not inter.IsEmpty() else 0.0
            result.provinces.append(ProvinceHit(p["code"], p["name"], "geom", round(ratio, 6)))
        else:
            result.provinces.append(ProvinceHit(p["code"], p["name"], "bbox", 0.0))

    hit_geom = [p for p in result.provinces if p.match == "geom"]
    if not hit_geom and result.provinces:
        result.warnings.append(
            "包围盒命中了省份但实际几何不相交（省份列表："
            + "、".join(p.name for p in result.provinces)
            + "），可能为省界数据简化误差或输入面贴边"
        )

    # 是否出了国界（仅提示，不阻断面）
    if settings.china_outline.exists():
        outline = _load_outline()
        if not outline.Intersects(geom):
            result.outside_china = True
            result.warnings.append("输入面完全不在国界范围内")
        else:
            d = geom.Difference(outline)
            if not d.IsEmpty() and geo.area_m2(d) / total_area > 0.02:
                result.outside_china = True
                result.warnings.append("输入面有超过 2% 的面积在国界之外，结果可能不完整")

    result.provinces.sort(key=lambda x: -x.area_ratio)
    result.elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)
    return result
