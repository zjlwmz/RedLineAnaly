#!/usr/bin/env python3
"""贪心裁片：把大面切成 ≤ max_size_km 的小片（按面积加权，近似按距离等分）。

真实的“用地报批”场景，用户上传的图斑（甚至一个县的全部报批地块）可能几千平方公里，
一次相交和一次 GeoJSON 提交都不合适。裁片后各片独立分析、独立汇总，
既能控住单次计算的内存，也能把边长限制在 50km 以内（经纬度转米的误差可控）。

这里用「经纬度网格 + 面积过滤」实现，够用且不依赖额外库；追求性能可换成
GEOS 的 `VoronoiDiagram` 或按人口/图幅网格切分。
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

from osgeo import ogr

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))

from app import geo  # noqa: E402

ogr.UseExceptions()

DD = 0.1     # 约 11km
OVER = 0.01  # 10% 重叠，防止裁片边界把图斑切丢


def split_geometry(geom: ogr.Geometry, tile_deg: float = DD, overlap: float = OVER) -> list[ogr.Geometry]:
    minx, maxx, miny, maxy = geom.GetEnvelope()
    tiles: list[ogr.Geometry] = []
    y = miny
    while y < maxy:
        x = minx
        while x < maxx:
            rect = _rect(x - overlap, y - overlap, x + tile_deg + overlap, y + tile_deg + overlap)
            inter = geom.Intersection(rect)
            if not inter.IsEmpty() and geo.area_m2(inter) > 1.0:
                tiles.append(inter)
            x += tile_deg
        y += tile_deg
    return tiles


def _rect(x0, y0, x1, y1) -> ogr.Geometry:
    ring = ogr.Geometry(ogr.wkbLinearRing)
    for x, yy in ((x0, y0), (x1, y0), (x1, y1), (x0, y1), (x0, y0)):
        ring.AddPoint_2D(x, yy)
    poly = ogr.Geometry(ogr.wkbPolygon)
    poly.AddGeometry(ring)
    return poly


def split_by_count(geom: ogr.Geometry, target_km2: float = 20000.0) -> list[ogr.Geometry]:
    """按目标面积估算需要的裁片数，再反推网格步长。"""
    area_km2 = geo.area_m2(geom) / 1e6
    if area_km2 <= target_km2:
        return [geom]
    n = max(1, math.ceil(math.sqrt(area_km2 / target_km2)))
    minx, maxx, miny, maxy = geom.GetEnvelope()
    step = max((maxx - minx), (maxy - miny)) / n
    return split_geometry(geom, tile_deg=step)


if __name__ == "__main__":
    g = geo.parse_geojson_geometry({
        "type": "Polygon",
        "coordinates": [[[114.0, 36.5], [117.5, 36.5], [117.5, 40.0], [114.0, 40.0], [114.0, 36.5]]]
    })
    tiles = split_by_count(g, 20000)
    print(f"原始 {geo.area_m2(g) / 1e6:.0f} km² → {len(tiles)} 片")
    s = sum(geo.area_m2(t) / 1e6 for t in tiles)
    print(f"裁片面积合计 {s:.0f} km²（重叠导致略大于原始，属预期）")
