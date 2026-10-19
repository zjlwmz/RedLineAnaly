"""几何与坐标工具。

设计要点（对应原方案的修正）：
1. 一切输入先统一到 CGCS2000 地理坐标（EPSG:4490），不假设用户传的是经纬度。
2. 面积一律用「等积投影」算，绝不用经纬度几何的 GetArea()——那是平方度。
   同时用 pyproj 的 Geod.geometry_area_perimeter 做椭球面积交叉校验（GDAL 3.8 Python API
   没有暴露 OGR_GEOM_AREA_ELLIPSOID，只能绕开）。
3. 按米缓冲：投到 Albers 等积投影再 Buffer，不能拿经纬度直接 Buffer(50)。
"""
from __future__ import annotations

import json
import math
from typing import Optional

from osgeo import ogr, osr
from pyproj import CRS, Geod, Transformer

ogr.UseExceptions()
osr.UseExceptions()

SRID_CGCS2000_GEO = 4490
SRID_WGS84 = 4326

#: CGCS2000 椭球（与 WGS84 差异在毫米级，工程上互认）
_GEOD = Geod(ellps="GRS80")


# --------------------------------------------------------------------------- #
# 坐标系
# --------------------------------------------------------------------------- #
def srs_from_epsg(code: int) -> osr.SpatialReference:
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(code)
    srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    return srs


def albers_srs(center_lon: float, center_lat: float) -> osr.SpatialReference:
    """以区域中心自定义 Albers 等积圆锥投影。

    固定用全国标准纬线（25N/47N）时，跨纬度大的面误差可达千分级；
    以实际区域中心建投影，误差可压到 0.01% 以内。
    """
    srs = osr.SpatialReference()
    srs.SetProjCS(f"Albers_China_{center_lon:.4f}_{center_lat:.4f}")
    srs.SetGeogCS("CGCS2000", "China_2000", "CGCS2000", 6378137.0, 298.257222101)
    sp1 = max(center_lat - 3.0, 0.0)
    sp2 = min(center_lat + 3.0, 89.0)
    srs.SetACEA(sp1, sp2, center_lat, center_lon, 0.0, 0.0)
    srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    return srs


def make_transformer(src_epsg: int, dst_epsg_or_srs) -> osr.CoordinateTransformation:
    dst = srs_from_epsg(dst_epsg_or_srs) if isinstance(dst_epsg_or_srs, int) else dst_epsg_or_srs
    return osr.CoordinateTransformation(srs_from_epsg(src_epsg), dst)


# --------------------------------------------------------------------------- #
# 解析
# --------------------------------------------------------------------------- #
def parse_geojson_geometry(obj, src_srid: int = SRID_WGS84) -> ogr.Geometry:
    """GeoJSON dict / GeoJSON 字符串 / WKT → OGR Geometry（已统一到 CGCS2000 地理坐标）。"""
    geom = None
    if isinstance(obj, dict):
        geom = ogr.CreateGeometryFromJson(json.dumps(obj))
    elif isinstance(obj, str):
        text = obj.strip()
        if text.startswith("{"):
            geom = ogr.CreateGeometryFromJson(text)
        else:
            geom = ogr.CreateGeometryFromWkt(text)
    if geom is None or geom.IsEmpty():
        raise ValueError("几何为空或无法解析")

    if ogr.GT_Flatten(geom.GetGeometryType()) not in (ogr.wkbPolygon, ogr.wkbMultiPolygon):
        raise ValueError("输入必须是面（Polygon / MultiPolygon）")

    if src_srid != SRID_CGCS2000_GEO:
        geom.Transform(make_transformer(src_srid, SRID_CGCS2000_GEO))

    if not geom.IsValid():
        # 自相交多边形做相交会得到垃圾结果，先修复
        fixed = geom.MakeValid()
        if fixed is None or fixed.IsEmpty():
            raise ValueError("输入几何无效且无法修复")
        geom = fixed
    return geom


# --------------------------------------------------------------------------- #
# 空间分析辅助
# --------------------------------------------------------------------------- #
def geometry_center(geom: ogr.Geometry) -> tuple[float, float]:
    minx, maxx, miny, maxy = geom.GetEnvelope()
    return (minx + maxx) / 2.0, (miny + maxy) / 2.0


def envelope_polygon(geom: ogr.Geometry) -> ogr.Geometry:
    minx, maxx, miny, maxy = geom.GetEnvelope()
    ring = ogr.Geometry(ogr.wkbLinearRing)
    for x, y in ((minx, miny), (maxx, miny), (maxx, maxy), (minx, maxy), (minx, miny)):
        ring.AddPoint_2D(x, y)
    poly = ogr.Geometry(ogr.wkbPolygon)
    poly.AddGeometry(ring)
    return poly


def to_albers(geom: ogr.Geometry, center: Optional[tuple[float, float]] = None) -> ogr.Geometry:
    """投影到以几何中心为基准的 Albers 等积坐标系（返回新几何）。"""
    lon, lat = center if center else geometry_center(geom)
    copy = geom.Clone()
    copy.Transform(make_transformer(SRID_CGCS2000_GEO, albers_srs(lon, lat)))
    return copy


def from_albers(geom: ogr.Geometry, center: tuple[float, float]) -> ogr.Geometry:
    copy = geom.Clone()
    dst = srs_from_epsg(SRID_CGCS2000_GEO)
    copy.Transform(osr.CoordinateTransformation(albers_srs(*center), dst))
    return copy


def area_m2(geom: ogr.Geometry, center: Optional[tuple[float, float]] = None) -> float:
    """等积投影下的面积（平方米）。"""
    if geom is None or geom.IsEmpty():
        return 0.0
    return float(to_albers(geom, center).GetArea())


def ellipsoid_area_m2(geom: ogr.Geometry) -> float:
    """椭球面真实面积（pyproj Geod，Karney 算法）。"""
    if geom is None or geom.IsEmpty():
        return 0.0
    gj = json.loads(geom.ExportToJson())
    return float(abs(_GEOD.geometry_area_perimeter(_shapely_like(gj))[0]))


def _shapely_like(geojson_geom: dict):
    """pyproj 的 geometry_area_perimeter 接受 shapely 几何；此处用 shapely 做中转。"""
    from shapely.geometry import shape

    return shape(geojson_geom)


def area_consistency(geom: ogr.Geometry) -> dict:
    """等积投影面积 vs 椭球面积。差异 > 0.5% 说明投影参数或数据基准有问题。"""
    a1 = area_m2(geom)
    a2 = ellipsoid_area_m2(geom)
    diff = abs(a1 - a2) / a2 * 100 if a2 > 0 else 0.0
    return {"albers_m2": a1, "ellipsoid_m2": a2, "diff_pct": round(diff, 4)}


def buffer_meters(geom: ogr.Geometry, meters: float) -> ogr.Geometry:
    """按米缓冲，并回到 CGCS2000 经纬度。"""
    if meters <= 0:
        return geom.Clone()
    center = geometry_center(geom)
    projected = to_albers(geom, center)
    buffered = projected.Buffer(meters, 8)
    return from_albers(buffered, center)


def transform_lonlat_to_albers(points: list[tuple[float, float]], center: tuple[float, float]) -> list[tuple[float, float]]:
    proj = Transformer.from_crs(
        CRS.from_epsg(SRID_CGCS2000_GEO), CRS.from_wkt(albers_srs(*center).ExportToWkt()), always_xy=True
    )
    xs, ys = proj.transform([p[0] for p in points], [p[1] for p in points])
    return list(zip(xs, ys))


def haversine_km(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    r = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dl = math.radians(lon2 - lon1)
    a = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))
