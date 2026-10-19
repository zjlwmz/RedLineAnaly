"""红线图斑数据访问层。

设计要点：
1. 用 GeoPackage（自带 R 树索引）而不是 Shapefile —— .shp 有 2GB 上限、字段名 10 字符限制。
2. `SetSpatialFilter(bbox)` 先让索引做粗筛，只把候选要素读进内存做精算；
   全国尺度的省数据动辄几十万图斑，全量遍历不可接受。
3. 按省惰性打开数据源，进程内缓存句柄（只读，多线程安全由 OGR 保证？并不——
   因此 worker 里加锁，或每个线程独立打开。这里用线程本地存储）。
"""
from __future__ import annotations

import threading
from pathlib import Path
from typing import Iterable, Iterator, Optional

from osgeo import ogr

from _config import settings

ogr.UseExceptions()

_PATCH_ID_FIELD_CANDIDATES = ("patch_id", "fid", "ID", "id", "OBJECTID")
_TYPE_FIELD_CANDIDATES = ("type", "TYPE", "rl_type", "redline_type", "红线类型")


class RedlineStore:
    """按省组织的数据集，一个省一个 GPKG。"""

    def __init__(self, root: Optional[Path] = None) -> None:
        self.root = Path(root or settings.redline_dir)
        self._lock = threading.Lock()
        self._datasources: dict[Path, ogr.DataSource] = {}
        self._meta: dict[str, dict] = {}

    # ------------------------------------------------------------------ #
    def _province_files(self) -> dict[str, Path]:
        """省份名 → 文件路径。文件名用行政区划代码，映射来自 provinces.gpkg 的 code 字段；
        这里额外支持「{省名}.gpkg」的直连方式，便于对接真实数据。"""
        if not self.root.exists():
            return {}
        return {p.stem: p for p in sorted(self.root.glob("*.gpkg"))}

    def _resolve(self, province_code: str) -> Optional[Path]:
        files = self._province_files()
        if province_code in files:
            return files[province_code]
        # 也允许以省名命名
        for stem, p in files.items():
            if stem == province_code:
                return p
        return None

    def _dataset(self, path: Path) -> ogr.DataSource:
        ds = self._datasources.get(path)
        if ds is None:
            ds = ogr.Open(str(path), 0)
            if ds is None:
                raise RuntimeError(f"无法打开红线数据: {path}")
            self._datasources[path] = ds
        return ds

    # ------------------------------------------------------------------ #
    def layer_meta(self, province_code: str) -> dict:
        """返回图层字段名、图斑数、数据版本（文件 mtime）。"""
        if province_code in self._meta:
            return self._meta[province_code]
        path = self._resolve(province_code)
        if path is None:
            meta = {"available": False, "count": 0, "patch_field": "patch_id",
                    "type_field": "type", "version": None}
            self._meta[province_code] = meta
            return meta
        ds = self._dataset(path)
        lyr = ds.GetLayer(0)
        names = [lyr.GetLayerDefn().GetFieldDefn(i).GetName() for i in range(lyr.GetLayerDefn().GetFieldCount())]
        patch_field = next((c for c in _PATCH_ID_FIELD_CANDIDATES if c in names), names[0] if names else "patch_id")
        type_field = next((c for c in _TYPE_FIELD_CANDIDATES if c in names), names[1] if len(names) > 1 else "type")
        meta = {
            "available": True,
            "count": lyr.GetFeatureCount(),
            "patch_field": patch_field,
            "type_field": type_field,
            "version": f"{path.name}@{int(path.stat().st_mtime)}",
            "crs": lyr.GetSpatialRef().GetAuthorityCode(None) if lyr.GetSpatialRef() else None,
        }
        self._meta[province_code] = meta
        return meta

    def iter_patches(
        self,
        province_code: str,
        bbox: Optional[tuple[float, float, float, float]] = None,
        buffer_deg: float = 0.0,
    ) -> Iterator[dict]:
        """遍历候选图斑。bbox 交给空间索引做粗筛，buffer_deg 用于边界容差。"""
        path = self._resolve(province_code)
        if path is None:
            return
        ds = self._dataset(path)
        lyr = ds.GetLayer(0)
        meta = self.layer_meta(province_code)

        with self._lock:
            lyr.SetSpatialFilter(None)
            if bbox is not None:
                minx, maxx, miny, maxy = bbox
                lyr.SetSpatialFilterRect(minx - buffer_deg, miny - buffer_deg, maxx + buffer_deg, maxy + buffer_deg)
            for feat in lyr:
                geom = feat.GetGeometryRef()
                if geom is None or geom.IsEmpty():
                    continue
                yield {
                    "patch_id": str(feat.GetField(meta["patch_field"]) or ""),
                    "redline_type": str(feat.GetField(meta["type_field"]) or "未分类"),
                    "geometry": geom.Clone(),
                }
            lyr.SetSpatialFilter(None)

    def close(self) -> None:
        self._datasources.clear()
        self._meta.clear()


_store: Optional[RedlineStore] = None
_store_lock = threading.Lock()


def get_store() -> RedlineStore:
    global _store
    with _store_lock:
        if _store is None:
            _store = RedlineStore()
        return _store
