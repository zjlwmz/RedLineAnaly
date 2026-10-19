"""演示数据构建脚本。

产出：
  data/province/provinces.gpkg   省级行政区（用于动态省份识别）
  data/province/china_outline.gpkg 国界（用于判断输入面是否出界）
  data/redline/{省}.gpkg         演示红线图斑（京津冀鲁，含省界压盖场景）

说明：本脚本生成的红线是**演示用的合成数据**，形状按真实地理语义摆放
（太行山生态带、华北平原耕地、环京津城镇圈），但坐标不是业务数据，
只用于验证「识别→拆分→相交→汇总」链路正确性。
"""
from __future__ import annotations

import json
import math
import random
import sys
from pathlib import Path

from osgeo import ogr, osr

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from app import geo  # noqa: E402

DATA = BASE_DIR / "data"
PROV_DIR = DATA / "province"
REDLINE_DIR = DATA / "redline"

PROVINCE_SRC = PROV_DIR / "100000_full.json"
OUTLINE_SRC = PROV_DIR / "china_outline.json"
TARGET_PROVINCES = {"北京市", "天津市", "河北省", "山东省", "山西省", "河南省"}

#: 行政区划代码（adcode 前两位用于省份编码）
PROVINCE_CODE = {
    "北京市": "110000", "天津市": "120000", "河北省": "130000",
    "山西省": "140000", "山东省": "370000", "河南省": "410000",
}


def _ogr_driver(name: str):
    d = ogr.GetDriverByName(name)
    if d is None:
        raise RuntimeError(f"驱动不可用: {name}")
    return d


def _srs() -> osr.SpatialReference:
    return geo.srs_from_epsg(geo.SRID_CGCS2000_GEO)


def write_polygon_layer(path: Path, layer_name: str, features, fields: dict[str, ogr.OFTType],
                        geom_type=ogr.wkbMultiPolygon) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()
    ds = _ogr_driver("GPKG").CreateDataSource(str(path))
    lyr = ds.CreateLayer(layer_name, srs=_srs(), geom_type=geom_type)
    for name, ftype in fields.items():
        lyr.CreateField(ogr.FieldDefn(name, ftype))
    defn = lyr.GetLayerDefn()
    for props, geom in features:
        feat = ogr.Feature(defn)
        for k, v in props.items():
            feat.SetField(k, v)
        feat.SetGeometry(geom)
        lyr.CreateFeature(feat)
    ds = None


# --------------------------------------------------------------------------- #
# 1. 省级边界
# --------------------------------------------------------------------------- #
def build_provinces() -> dict[str, ogr.Geometry]:
    raw = json.loads(PROVINCE_SRC.read_text(encoding="utf-8"))
    feats = []
    geoms: dict[str, ogr.Geometry] = {}
    for f in raw["features"]:
        props = f["properties"]
        name = props.get("name") or ""
        if not name or props.get("adcode") == 100000:
            continue
        geom = ogr.CreateGeometryFromJson(json.dumps(f["geometry"]))
        if geom is None or geom.IsEmpty():
            continue
        if ogr.GT_Flatten(geom.GetGeometryType()) != ogr.wkbMultiPolygon:
            geom = ogr.ForceToMultiPolygon(geom)
        geoms[name] = geom
        feats.append(({
            "code": str(props.get("adcode", "")),
            "name": name,
            "center_lon": float(props.get("center", [0, 0])[0]),
            "center_lat": float(props.get("center", [0, 0])[1]),
        }, geom))

    write_polygon_layer(
        PROV_DIR / "provinces.gpkg", "provinces", feats,
        {"code": ogr.OFTString, "name": ogr.OFTString,
         "center_lon": ogr.OFTReal, "center_lat": ogr.OFTReal},
    )
    print(f"[ok] provinces.gpkg  {len(feats)} 个省")

    # 国界
    outline = json.loads(OUTLINE_SRC.read_text(encoding="utf-8"))
    og = ogr.CreateGeometryFromJson(json.dumps(outline["features"][0]["geometry"]))
    write_polygon_layer(
        PROV_DIR / "china_outline.gpkg", "outline", [({"name": "中华人民共和国"}, og)],
        {"name": ogr.OFTString},
    )
    print("[ok] china_outline.gpkg")
    return geoms


# --------------------------------------------------------------------------- #
# 2. 演示红线
# --------------------------------------------------------------------------- #
def _wkt_poly(coords: list[tuple[float, float]]) -> str:
    ring = ",".join(f"{x:.6f} {y:.6f}" for x, y in coords)
    return f"POLYGON(({ring}))"


def _rect(lon0, lat0, lon1, lat1) -> list[tuple[float, float]]:
    return [(lon0, lat0), (lon1, lat0), (lon1, lat1), (lon0, lat1), (lon0, lat0)]


def _strip(lon, lat0, lat1, width_deg, jitter=0.0, seed=0) -> list[tuple[float, float]]:
    """沿经线方向的一条带（模拟沿山脉/河流分布的线状红线），左右边界带抖动。"""
    rnd = random.Random(seed)
    n = 24
    left, right = [], []
    for i in range(n + 1):
        t = i / n
        y = lat0 + (lat1 - lat0) * t
        jx = (rnd.random() - 0.5) * jitter
        w = width_deg * (1 + 0.18 * math.sin(t * 7 + seed))
        left.append((lon - w / 2 + jx, y))
        right.append((lon + w / 2 + jx, y))
    return left + right[::-1] + [left[0]]


def redline_specs() -> list[dict]:
    """按省组织演示红线。坐标按真实地理语义摆放。"""
    specs: list[dict] = []

    def add(fid, ptype, province, coords):
        specs.append({"fid": fid, "type": ptype, "province": province, "coords": coords})

    # ---- 河北省：太行山生态带（骑在晋冀省界上，故意跨省）----
    add("HB-ECO-0001", "生态保护红线", "河北省", _strip(114.05, 36.3, 40.0, 0.55, 0.08, seed=1))
    # ---- 山西省：太行山西侧，与上面那条在省界附近互相咬合 ----
    add("SX-ECO-0001", "生态保护红线", "山西省", _strip(113.70, 36.3, 40.0, 0.55, 0.08, seed=2))
    # ---- 河北：燕山生态带（承德-唐山北）----
    add("HB-ECO-0002", "生态保护红线", "河北省", _strip(117.6, 40.2, 41.0, 1.2, 0.12, seed=3))
    # ---- 北京：北部山区生态红线 ----
    add("BJ-ECO-0001", "生态保护红线", "北京市", _rect(115.9, 40.35, 116.75, 40.75))
    add("BJ-ECO-0002", "生态保护红线", "北京市", _rect(116.78, 40.45, 117.35, 40.85))
    # ---- 北京：永久基本农田（东南平原）----
    add("BJ-FAR-0001", "永久基本农田", "北京市", _rect(116.45, 39.62, 117.05, 40.05))
    # ---- 廊坊北三县一带的耕地，故意压在北京-河北交界 ----
    add("HB-FAR-0003", "永久基本农田", "河北省", _rect(116.95, 39.98, 117.45, 40.30))
    # ---- 天津：东部湿地生态红线 ----
    add("TJ-ECO-0001", "生态保护红线", "天津市", _rect(117.45, 38.85, 118.05, 39.55))
    # ---- 天津：城镇开发边界 ----
    add("TJ-URB-0001", "城镇开发边界", "天津市", _rect(117.05, 38.95, 117.75, 39.35))
    # ---- 河北：保定-石家庄耕地带 ----
    add("HB-FAR-0001", "永久基本农田", "河北省", _rect(114.85, 38.05, 115.85, 38.95))
    # ---- 河北：环京津城镇开发边界（含固安、涿州）----
    add("HB-URB-0001", "城镇开发边界", "河北省", _rect(116.05, 39.20, 116.75, 39.75))
    # ---- 河北：雄安城镇开发边界 ----
    add("HB-URB-0002", "城镇开发边界", "河北省", _rect(115.75, 38.85, 116.20, 39.15))
    # ---- 河北：坝上生态（张家口北）----
    add("HB-ECO-0003", "生态保护红线", "河北省", _rect(114.20, 41.05, 115.60, 41.80))
    # ---- 山东：黄河三角洲生态 + 耕地 ----
    add("SD-ECO-0001", "生态保护红线", "山东省", _rect(118.40, 37.55, 119.30, 38.15))
    add("SD-FAR-0001", "永久基本农田", "山东省", _rect(115.90, 35.60, 117.10, 36.60))
    # ---- 山东：与河北交界的卫运河一带耕地（跨省压盖场景）----
    add("SD-FAR-0002", "永久基本农田", "山东省", _rect(115.55, 36.55, 116.30, 37.45))
    # ---- 河南：豫北耕地 ----
    add("HN-FAR-0001", "永久基本农田", "河南省", _rect(113.90, 35.20, 114.80, 36.05))
    # ---- 山西：汾河谷地耕地 ----
    add("SX-FAR-0001", "永久基本农田", "山西省", _rect(112.35, 37.05, 113.10, 37.95))
    # ---- 北京：门头沟/房山西部深山区（大块，用于多省拆分的演示）----
    add("BJ-ECO-0003", "生态保护红线", "北京市", _rect(115.42, 39.65, 115.95, 40.15))
    # ---- 城镇开发边界：石家庄 ----
    add("HB-URB-0003", "城镇开发边界", "河北省", _rect(114.35, 37.95, 114.72, 38.18))

    for s in specs:
        s["wkt"] = _wkt_poly(s["coords"])
    return specs


def build_redlines() -> None:
    REDLINE_DIR.mkdir(parents=True, exist_ok=True)
    by_prov: dict[str, list] = {}
    for s in redline_specs():
        by_prov.setdefault(s["province"], []).append(s)

    for prov, items in by_prov.items():
        feats = []
        for s in items:
            geom = ogr.CreateGeometryFromWkt(s["wkt"])
            if not geom.IsValid():
                geom = geom.MakeValid()
            geom = ogr.ForceToMultiPolygon(geom)
            feats.append(({"patch_id": s["fid"], "type": s["type"], "province": prov}, geom))
        name = f"{PROVINCE_CODE.get(prov, prov)}.gpkg"
        write_polygon_layer(
            REDLINE_DIR / name, "redline", feats,
            {"patch_id": ogr.OFTString, "type": ogr.OFTString, "province": ogr.OFTString},
        )
        print(f"[ok] redline/{name}  {len(feats)} 个图斑")


def main() -> None:
    build_provinces()
    build_redlines()


if __name__ == "__main__":
    main()
