"""CLCD 分块：主节点分配像元，从节点裁剪并矢量化，主节点合并成果。"""
from __future__ import annotations

import json
import logging
import math
import os
import re
import uuid
from pathlib import Path

import numpy as np
from osgeo import gdal, ogr, osr

gdal.UseExceptions()
ogr.UseExceptions()
osr.UseExceptions()

ADMIN_FILES = {"province": "中国_省2.shp", "city": "中国_市2.shp", "county": "中国_县2.shp"}
TASK_ID_RE = re.compile(r"^[0-9a-f]{32}$")
TILE_SIZE = 512
logger = logging.getLogger(__name__)


def _mask(geom, width, height, gt, projection):
    ds = gdal.GetDriverByName("MEM").Create("", width, height, 1, gdal.GDT_Byte)
    ds.SetGeoTransform(gt)
    ds.SetProjection(projection)
    source = (ogr.GetDriverByName("MEM") or ogr.GetDriverByName("Memory")).CreateDataSource("")
    layer = source.CreateLayer("mask", srs=osr.SpatialReference(wkt=projection), geom_type=ogr.wkbUnknown)
    feature = ogr.Feature(layer.GetLayerDefn())
    feature.SetGeometry(geom)
    layer.CreateFeature(feature)
    gdal.RasterizeLayer(ds, [1], layer, burn_values=[1])
    return ds.GetRasterBand(1).ReadAsArray() != 0


def create_plan(geometry: dict, srid: int, raster_path: Path, admin_dir: Path,
                split_level: str, output_root: Path, max_pixels: int,
                classes: dict[int, str], nodata: int, year: int, task_id: str | None = None) -> dict:
    """只把输入面内的像元分配给一个子任务；行政区重叠取首个，缝隙归入补充任务。"""
    if split_level not in ADMIN_FILES:
        raise ValueError("split_level 只支持 province、city、county")
    raster_path = raster_path.resolve()
    if not raster_path.is_file():
        raise ValueError(f"CLCD 栅格不存在：{raster_path}")
    admin_path = admin_dir / ADMIN_FILES[split_level]
    if not admin_path.is_file():
        raise ValueError(f"缺少行政区划文件：{admin_path}")
    raster = gdal.Open(str(raster_path), gdal.GA_ReadOnly)
    if raster is None or raster.RasterCount != 1 or raster.GetRasterBand(1).GetNoDataValue() != nodata:
        raise ValueError("CLCD 栅格或 NoData 配置无效")
    gt = raster.GetGeoTransform()
    if gt[2] != 0 or gt[4] != 0 or gt[1] <= 0 or gt[5] >= 0:
        raise ValueError("当前仅支持北向、无旋转的分类栅格")
    geom = ogr.CreateGeometryFromJson(json.dumps(geometry, ensure_ascii=False))
    if geom is None or geom.IsEmpty() or ogr.GT_Flatten(geom.GetGeometryType()) not in (ogr.wkbPolygon, ogr.wkbMultiPolygon):
        raise ValueError("输入必须是有效的 Polygon 或 MultiPolygon")
    if not geom.IsValid():
        raise ValueError("输入面存在自相交等拓扑错误，请先修复")
    source_srs = osr.SpatialReference()
    if srid <= 0 or source_srs.ImportFromEPSG(srid) != 0:
        raise ValueError("geometry_srid 必须是有效的 EPSG 编码")
    source_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    raster_srs = osr.SpatialReference(wkt=raster.GetProjectionRef())
    raster_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    if not raster_srs.IsProjected():
        raise ValueError("CLCD 栅格缺少有效的投影坐标系")
    geom.Transform(osr.CoordinateTransformation(source_srs, raster_srs))
    area = geom.GetArea()
    if not math.isfinite(area) or area <= 0:
        raise ValueError("输入面投影后面积无效")
    minx, maxx, miny, maxy = geom.GetEnvelope()
    x0 = max(0, math.floor((minx - gt[0]) / gt[1]))
    x1 = min(raster.RasterXSize, math.ceil((maxx - gt[0]) / gt[1]))
    y0 = max(0, math.floor((maxy - gt[3]) / gt[5]))
    y1 = min(raster.RasterYSize, math.ceil((miny - gt[3]) / gt[5]))
    width, height = x1 - x0, y1 - y0
    if width <= 0 or height <= 0:
        raise ValueError("输入面与 1993 年 CLCD 栅格范围不相交")
    if max_pixels > 0 and width * height > max_pixels:
        raise ValueError(f"范围过大：需处理 {width * height:,} 个像元，当前上限 {max_pixels:,}；请缩小输入面")
    clip_gt = (gt[0] + x0 * gt[1], gt[1], 0., gt[3] + y0 * gt[5], 0., gt[5])

    task_id = task_id or uuid.uuid4().hex
    if not TASK_ID_RE.fullmatch(task_id):
        raise ValueError("任务 ID 格式无效")
    task_dir = output_root / task_id
    task_dir.mkdir(parents=True, exist_ok=False)
    assignment_path = task_dir / "assignment.tif"
    assignment_ds = gdal.GetDriverByName("GTiff").Create(str(assignment_path), width, height, 1, gdal.GDT_UInt16,
                                                          options=["TILED=YES", "COMPRESS=LZW"])
    assignment_ds.SetGeoTransform(clip_gt)
    assignment_ds.SetProjection(raster.GetProjectionRef())
    assignment_band = assignment_ds.GetRasterBand(1)
    assignment_band.Fill(0)
    vector_ds = (ogr.GetDriverByName("MEM") or ogr.GetDriverByName("Memory")).CreateDataSource("")
    input_layer = vector_ds.CreateLayer("input", srs=raster_srs, geom_type=ogr.wkbUnknown)
    feature = ogr.Feature(input_layer.GetLayerDefn())
    feature.SetGeometry(geom)
    input_layer.CreateFeature(feature)
    # 65535 是尚未分配的面内像元；直接栅格化到磁盘，避免读取整个省级掩膜。
    gdal.RasterizeLayer(assignment_ds, [1], input_layer, burn_values=[65535])
    feature = None
    vector_ds = None

    gdal.SetConfigOption("SHAPE_ENCODING", "UTF-8")
    admin = ogr.Open(str(admin_path), 0)
    if admin is None:
        raise ValueError(f"无法读取行政区划文件：{admin_path}")
    layer = admin.GetLayer(0)
    admin_srs = layer.GetSpatialRef()
    if admin_srs is None:
        raise ValueError("行政区划缺少坐标系")
    admin_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    search_geom = geom.Clone()
    search_geom.Transform(osr.CoordinateTransformation(raster_srs, admin_srs))
    layer.SetSpatialFilter(search_geom)
    regions = []
    # 固定按行政代码排序，边界重叠时归属稳定；每次只处理行政区包围框内的小块。
    features = sorted(((str(f.GetField("gb")), f.GetField("name"), f.GetGeometryRef().Clone())
                       for f in layer if f.GetGeometryRef() is not None), key=lambda item: item[0])
    if len(features) >= 65535:
        raise ValueError("行政区划要素数量超过 UInt16 像元归属容量")
    for label, (code, name, boundary) in enumerate(features, start=1):
        if not boundary.IsValid():
            boundary = boundary.MakeValid()
        if boundary is None or boundary.IsEmpty():
            continue
        boundary.Transform(osr.CoordinateTransformation(admin_srs, raster_srs))
        minbx, maxbx, minby, maxby = boundary.GetEnvelope()
        bx0 = max(0, math.floor((minbx - clip_gt[0]) / gt[1]))
        bx1 = min(width, math.ceil((maxbx - clip_gt[0]) / gt[1]))
        by0 = max(0, math.floor((maxby - clip_gt[3]) / gt[5]))
        by1 = min(height, math.ceil((minby - clip_gt[3]) / gt[5]))
        bounds = [width, height, 0, 0]
        for ty in range(by0, by1, TILE_SIZE):
            for tx in range(bx0, bx1, TILE_SIZE):
                tw, th = min(TILE_SIZE, bx1 - tx), min(TILE_SIZE, by1 - ty)
                tile = assignment_band.ReadAsArray(tx, ty, tw, th)
                if not np.any(tile == 65535):
                    continue
                tile_gt = (clip_gt[0] + tx * gt[1], gt[1], 0., clip_gt[3] + ty * gt[5], 0., gt[5])
                hits = _mask(boundary, tw, th, tile_gt, raster.GetProjectionRef()) & (tile == 65535)
                if not np.any(hits):
                    continue
                tile[hits] = label
                assignment_band.WriteArray(tile, tx, ty)
                ys, xs = np.where(hits)
                bounds = [min(bounds[0], tx + int(xs.min())), min(bounds[1], ty + int(ys.min())),
                          max(bounds[2], tx + int(xs.max()) + 1), max(bounds[3], ty + int(ys.max()) + 1)]
        if bounds[0] < bounds[2]:
            regions.append({"label": label, "code": code, "name": name,
                            "window": [bounds[0], bounds[1], bounds[2] - bounds[0], bounds[3] - bounds[1]]})
    # 行政边界之间的空隙仍属于输入面，单独交给补充子任务。
    bounds = [width, height, 0, 0]
    for ty in range(0, height, TILE_SIZE):
        for tx in range(0, width, TILE_SIZE):
            tw, th = min(TILE_SIZE, width - tx), min(TILE_SIZE, height - ty)
            tile = assignment_band.ReadAsArray(tx, ty, tw, th)
            ys, xs = np.where(tile == 65535)
            if xs.size:
                bounds = [min(bounds[0], tx + int(xs.min())), min(bounds[1], ty + int(ys.min())),
                          max(bounds[2], tx + int(xs.max()) + 1), max(bounds[3], ty + int(ys.max()) + 1)]
    if bounds[0] < bounds[2]:
        regions.append({"label": 65535, "code": "unassigned", "name": "行政区划空隙",
                        "window": [bounds[0], bounds[1], bounds[2] - bounds[0], bounds[3] - bounds[1]]})
    if not regions:
        raise ValueError("输入面小于像元或未覆盖任何像元中心")
    assignment_ds = None
    plan = {"task_id": task_id, "split_level": split_level, "regions": regions,
            "chunk_vectors": True,
            "source_raster": str(raster_path), "source_window": [x0, y0, width, height],
            "geotransform": clip_gt, "projection": raster.GetProjectionRef(),
            "input_area_m2": round(area, 2), "year": year, "nodata": nodata,
            "classes": {str(k): v for k, v in classes.items()}}
    (task_dir / "plan.json").write_text(json.dumps(plan, ensure_ascii=False), encoding="utf-8")
    return plan


def run_chunk(output_root: Path, task_id: str, label: int) -> dict:
    """从节点处理自己的像元窗口和分类面，成果通过共享目录交给主节点。"""
    if not TASK_ID_RE.fullmatch(task_id) or label <= 0:
        raise ValueError("子任务标识无效")
    task_dir = output_root / task_id
    plan_path = task_dir / "plan.json"
    if not plan_path.is_file():
        raise FileNotFoundError(f"找不到任务计划：{plan_path}；请检查 Master/Slave 的共享成果目录并重启使用旧配置的节点")
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    region = next(r for r in plan["regions"] if r["label"] == label)
    x, y, width, height = region["window"]
    logger.info("CLCD subtask %s:%s started, region=%s, window=%sx%s", task_id, label,
                region["name"], width, height)
    assignment = gdal.Open(str(task_dir / "assignment.tif"), gdal.GA_ReadOnly)
    source = gdal.Open(plan["source_raster"], gdal.GA_ReadOnly)
    if source is None or assignment is None:
        raise ValueError("共享分配文件或 CLCD 栅格不可读取")
    sx, sy, _, _ = plan["source_window"]
    src_gt = source.GetGeoTransform()
    expected = plan["geotransform"]
    if source.RasterCount != 1 or source.GetRasterBand(1).GetNoDataValue() != plan["nodata"]:
        raise ValueError("从节点 CLCD 波段或 NoData 与主节点不一致")
    if (assignment.RasterXSize, assignment.RasterYSize) != tuple(plan["source_window"][2:]):
        raise ValueError("共享像元分配栅格尺寸与任务计划不一致")
    if not np.allclose([src_gt[0] + sx * src_gt[1], src_gt[1], src_gt[3] + sy * src_gt[5], src_gt[5]],
                       [expected[0], expected[1], expected[3], expected[5]], rtol=0, atol=1e-7) or source.GetProjectionRef() != plan["projection"]:
        raise ValueError("从节点 CLCD 栅格与主节点任务网格不一致")
    nodata = plan["nodata"]
    target = task_dir / f"chunk_{label}.tif"
    gt = plan["geotransform"]
    chunk_gt = (gt[0] + x * gt[1], gt[1], 0., gt[3] + y * gt[5], 0., gt[5])
    reusable = False
    if target.is_file():
        try:
            existing = gdal.Open(str(target), gdal.GA_ReadOnly)
            reusable = (existing is not None and (existing.RasterXSize, existing.RasterYSize) == (width, height)
                        and existing.GetGeoTransform() == chunk_gt and existing.GetProjectionRef() == plan["projection"]
                        and existing.GetRasterBand(1).GetNoDataValue() == nodata)
            existing = None
        except RuntimeError:
            reusable = False
    pixel_count = 0
    if reusable:
        # 已完整写出的 TIFF 可以复用；矢量文件缺失时仍需补做矢量化。
        logger.info("CLCD subtask %s:%s reusing completed raster chunk", task_id, label)
        for ty in range(y, y + height, TILE_SIZE):
            for tx in range(x, x + width, TILE_SIZE):
                tw, th = min(TILE_SIZE, x + width - tx), min(TILE_SIZE, y + height - ty)
                pixel_count += int(np.count_nonzero(
                    assignment.GetRasterBand(1).ReadAsArray(tx, ty, tw, th) == label))
    else:
        temp = task_dir / f"chunk_{label}_{uuid.uuid4().hex}.tmp.tif"
        ds = gdal.GetDriverByName("GTiff").Create(str(temp), width, height, 1, gdal.GDT_Byte,
                                                  options=["TILED=YES", "COMPRESS=LZW"])
        ds.SetGeoTransform(chunk_gt)
        ds.SetProjection(plan["projection"])
        output_band = ds.GetRasterBand(1)
        output_band.SetNoDataValue(nodata)
        output_band.Fill(nodata)
        row_count = math.ceil(height / TILE_SIZE)
        next_report = 10
        for row_index, ty in enumerate(range(y, y + height, TILE_SIZE), start=1):
            for tx in range(x, x + width, TILE_SIZE):
                tw, th = min(TILE_SIZE, x + width - tx), min(TILE_SIZE, y + height - ty)
                selected = assignment.GetRasterBand(1).ReadAsArray(tx, ty, tw, th) == label
                if not np.any(selected):
                    continue
                values = source.GetRasterBand(1).ReadAsArray(sx + tx, sy + ty, tw, th)
                unknown = selected & (values != nodata) & ~np.isin(values, [int(k) for k in plan["classes"]])
                if np.any(unknown):
                    raise ValueError(f"CLCD 栅格出现未配置的分类码：{np.unique(values[unknown]).tolist()}")
                output_band.WriteArray(np.where(selected, values, nodata).astype(np.uint8), tx - x, ty - y)
                pixel_count += int(np.count_nonzero(selected))
            # 按扫描行报告约 10% 的进度，长时间裁剪时能确认节点仍在执行。
            progress = row_index * 100 // row_count
            if progress >= next_report and progress < 100:
                logger.info("CLCD subtask %s:%s raster clipping %s%%", task_id, label, progress)
                next_report = (progress // 10 + 1) * 10
        ds = None
        os.replace(temp, target)
        logger.info("CLCD subtask %s:%s raster clipping completed, pixels=%s", task_id, label, pixel_count)
    result = {"task_id": task_id, "label": label, "status": "success", "pixel_count": pixel_count}
    if plan.get("chunk_vectors"):
        vector_path = task_dir / f"chunk_{label}.gpkg"
        if not reusable or not vector_path.is_file():
            logger.info("CLCD subtask %s:%s vectorizing", task_id, label)
            _vectorize_chunk(target, vector_path, plan, label)
        else:
            logger.info("CLCD subtask %s:%s reusing completed vector chunk", task_id, label)
        vector = ogr.Open(str(vector_path), 0)
        if vector is None or vector.GetLayerByName("landcover") is None:
            raise ValueError(f"子任务 {label} 矢量成果不可读取")
        result["vector_feature_count"] = vector.GetLayerByName("landcover").GetFeatureCount()
        vector = None
    return result


def _vectorize_chunk(raster_path: Path, vector_path: Path, plan: dict, label: int) -> None:
    """在 Slave 上生成带分类和面积属性的分块 GPKG，完成后原子替换。"""
    temp = vector_path.with_name(f"{vector_path.stem}_{uuid.uuid4().hex}.tmp.gpkg")
    raster = gdal.Open(str(raster_path), gdal.GA_ReadOnly)
    gpkg = ogr.GetDriverByName("GPKG").CreateDataSource(str(temp))
    layer = gpkg.CreateLayer("landcover", srs=osr.SpatialReference(wkt=plan["projection"]),
                             geom_type=ogr.wkbPolygon, options=["SPATIAL_INDEX=NO"])
    layer.CreateField(ogr.FieldDefn("class_code", ogr.OFTInteger))
    layer.CreateField(ogr.FieldDefn("class_name", ogr.OFTString))
    layer.CreateField(ogr.FieldDefn("area_m2", ogr.OFTReal))
    band = raster.GetRasterBand(1)
    # 每个分块只矢量化本节点的有效像元；跨分块的同类斑块允许保留边界。
    progress_step = 0

    def polygonize_progress(fraction, _message, _data):
        nonlocal progress_step
        step = int(fraction * 10)
        if step > progress_step and step < 10:
            progress_step = step
            logger.info("CLCD subtask %s:%s polygonize %s%%", plan["task_id"], label, step * 10)
        return 1

    gdal.Polygonize(band, band.GetMaskBand(), layer, 0, [], callback=polygonize_progress)
    logger.info("CLCD subtask %s:%s polygonize completed, writing attributes", plan["task_id"], label)
    classes = {int(code): name for code, name in plan["classes"].items()}
    layer.StartTransaction()
    feature_count = 0
    for feature_count, feature in enumerate(layer, start=1):
        feature.SetField("class_name", classes[feature.GetField("class_code")])
        feature.SetField("area_m2", round(feature.GetGeometryRef().GetArea(), 2))
        layer.SetFeature(feature)
        if feature_count % 10000 == 0:
            layer.CommitTransaction()
            layer.StartTransaction()
        if feature_count % 100000 == 0:
            logger.info("CLCD subtask %s:%s attributes written: %s features", plan["task_id"],
                        label, feature_count)
    layer.CommitTransaction()
    gpkg = None
    raster = None
    os.replace(temp, vector_path)
    logger.info("CLCD subtask %s:%s vector completed, features=%s", plan["task_id"], label, feature_count)


def _merge_chunk_vectors(task_dir: Path, plan: dict, task_id: str, vector_path: Path, srs) -> int:
    """在 Master 上批量追加分块矢量，并只为最终成果建立一次空间索引。"""
    temp_vector = task_dir / f"CLCD_{plan['year']}_{uuid.uuid4().hex}.tmp.gpkg"
    vector_count = 0
    try:
        for index, region in enumerate(plan["regions"], start=1):
            chunk_vector = task_dir / f"chunk_{region['label']}.gpkg"
            source_vector = ogr.Open(str(chunk_vector), 0)
            source_layer = source_vector.GetLayerByName("landcover") if source_vector else None
            if source_layer is None:
                raise ValueError(f"子任务 {region['code']} 矢量成果缺失或不可读取")
            if not source_layer.GetSpatialRef().IsSame(srs):
                raise ValueError(f"子任务 {region['code']} 矢量坐标系不一致")
            vector_count += source_layer.GetFeatureCount()
            source_vector = None
            # GDAL 在 C++ 中批量追加要素，避免 Master 用 Python 遍历千万级斑块。
            options = gdal.VectorTranslateOptions(format="GPKG", accessMode=None if index == 1 else "append",
                                                   layers=["landcover"], layerName="landcover",
                                                   layerCreationOptions=["SPATIAL_INDEX=NO"] if index == 1 else None,
                                                   transactionSize=10000)
            merged = gdal.VectorTranslate(str(temp_vector), str(chunk_vector), options=options)
            if merged is None:
                raise RuntimeError(f"无法合并子任务 {region['code']} 的矢量成果")
            merged = None
            if index % 10 == 0 or index == len(plan["regions"]):
                logger.info("CLCD task %s vector merge %s/%s", task_id, index, len(plan["regions"]))
        logger.info("CLCD task %s building final vector spatial index", task_id)
        gpkg = ogr.Open(str(temp_vector), 1)
        gpkg.ExecuteSQL("SELECT CreateSpatialIndex('landcover', 'geom')")
        gpkg = None
        os.replace(temp_vector, vector_path)
        return vector_count
    finally:
        if temp_vector.exists():
            try:
                temp_vector.unlink()
            except OSError:
                logger.warning("CLCD task %s could not remove incomplete vector %s", task_id, temp_vector)


def finish_task(output_root: Path, task_id: str) -> dict:
    """按唯一像元分配表合并栅格并统计；新版任务合并 Slave 矢量成果。"""
    if not TASK_ID_RE.fullmatch(task_id):
        raise ValueError("任务标识无效")
    task_dir = output_root / task_id
    plan = json.loads((task_dir / "plan.json").read_text(encoding="utf-8"))
    sx, sy, width, height = plan["source_window"]
    nodata, year = plan["nodata"], plan["year"]
    classes = {int(k): v for k, v in plan["classes"].items()}
    assignment = gdal.Open(str(task_dir / "assignment.tif"), gdal.GA_ReadOnly)
    if assignment is None or (assignment.RasterXSize, assignment.RasterYSize) != (width, height):
        raise ValueError("像元分配表缺失或尺寸不符")
    raster_path = task_dir / f"CLCD_{year}_clip.tif"
    raster = gdal.GetDriverByName("GTiff").Create(str(raster_path), width, height, 1, gdal.GDT_Byte,
                                                 options=["TILED=YES", "COMPRESS=LZW"])
    raster.SetGeoTransform(plan["geotransform"])
    raster.SetProjection(plan["projection"])
    band = raster.GetRasterBand(1)
    band.SetNoDataValue(nodata)
    source = gdal.Open(plan["source_raster"], gdal.GA_ReadOnly)
    if source and source.GetRasterBand(1).GetColorTable():
        band.SetColorTable(source.GetRasterBand(1).GetColorTable())
    band.Fill(nodata)
    inside_count = 0
    valid_count = 0
    class_counts = {code: 0 for code in classes}
    for region_index, region in enumerate(plan["regions"], start=1):
        x, y, w, h = region["window"]
        chunk = gdal.Open(str(task_dir / f"chunk_{region['label']}.tif"), gdal.GA_ReadOnly)
        if chunk is None or (chunk.RasterXSize, chunk.RasterYSize) != (w, h):
            raise ValueError(f"子任务 {region['code']} 成果缺失或尺寸不符")
        for ty in range(y, y + h, TILE_SIZE):
            for tx in range(x, x + w, TILE_SIZE):
                tw, th = min(TILE_SIZE, x + w - tx), min(TILE_SIZE, y + h - ty)
                selected = assignment.GetRasterBand(1).ReadAsArray(tx, ty, tw, th) == region["label"]
                values = chunk.GetRasterBand(1).ReadAsArray(tx - x, ty - y, tw, th)
                if np.any(values[~selected] != nodata):
                    raise ValueError(f"子任务 {region['code']} 成果越界")
                if not np.any(selected):
                    continue
                # 包围框可能交叠，只覆盖当前标签像元，防止擦掉已合并的其他分块。
                current = band.ReadAsArray(tx, ty, tw, th)
                current[selected] = values[selected]
                band.WriteArray(current, tx, ty)
                inside_count += int(np.count_nonzero(selected))
                valid_values = values[selected & (values != nodata)]
                valid_count += int(valid_values.size)
                codes, counts = np.unique(valid_values, return_counts=True)
                for code, count in zip(codes.tolist(), counts.tolist()):
                    if code not in class_counts:
                        raise ValueError(f"合并成果存在未配置的分类码：{code}")
                    class_counts[code] += count
        chunk = None
        if region_index % 10 == 0 or region_index == len(plan["regions"]):
            logger.info("CLCD task %s raster merge %s/%s", task_id, region_index, len(plan["regions"]))
    band.FlushCache()
    assigned_count = 0
    region_labels = {r["label"] for r in plan["regions"]}
    logger.info("CLCD task %s verifying pixel assignment", task_id)
    row_count = math.ceil(height / TILE_SIZE)
    next_report = 10
    for row_index, ty in enumerate(range(0, height, TILE_SIZE), start=1):
        for tx in range(0, width, TILE_SIZE):
            labels = assignment.GetRasterBand(1).ReadAsArray(tx, ty, min(TILE_SIZE, width - tx), min(TILE_SIZE, height - ty))
            if set(np.unique(labels).tolist()) - {0} - region_labels:
                raise ValueError("像元分配表与子任务清单不一致")
            assigned_count += int(np.count_nonzero(labels))
        progress = row_index * 100 // row_count
        if progress >= next_report and progress < 100:
            logger.info("CLCD task %s pixel assignment verification %s%%", task_id, progress)
            next_report = (progress // 10 + 1) * 10
    if assigned_count != inside_count:
        raise ValueError("分块成果没有覆盖全部面内像元")
    pixel_area = abs(plan["geotransform"][1] * plan["geotransform"][5])
    rows = []
    for code, name in sorted(classes.items()):
        count = class_counts[code]
        rows.append({"code": code, "name": name, "pixel_count": count,
                     "area_m2": round(count * pixel_area, 2),
                     "percent": round(count / valid_count * 100, 4) if valid_count else 0.})
    srs = osr.SpatialReference(wkt=plan["projection"])
    vector_path = task_dir / f"CLCD_{year}_classes.gpkg"
    if plan.get("chunk_vectors"):
        logger.info("CLCD task %s raster merge completed, merging vector chunks", task_id)
        vector_count = _merge_chunk_vectors(task_dir, plan, task_id, vector_path, srs)
    else:
        # 旧计划没有分块矢量，新版 Master 仍可恢复其原有合并流程。
        if vector_path.exists():
            vector_path.unlink()
        gpkg = ogr.GetDriverByName("GPKG").CreateDataSource(str(vector_path))
        layer = gpkg.CreateLayer("landcover", srs=srs, geom_type=ogr.wkbPolygon)
        layer.CreateField(ogr.FieldDefn("class_code", ogr.OFTInteger))
        layer.CreateField(ogr.FieldDefn("class_name", ogr.OFTString))
        layer.CreateField(ogr.FieldDefn("area_m2", ogr.OFTReal))
        gdal.Polygonize(band, band.GetMaskBand(), layer, 0, [])
        vector_count = 0
        for feature in layer:
            feature.SetField("class_name", classes[feature.GetField("class_code")])
            feature.SetField("area_m2", round(feature.GetGeometryRef().GetArea(), 2))
            layer.SetFeature(feature)
            vector_count += 1
        gpkg = None
    raster = None
    result = {"task_id": task_id, "year": year, "split_level": plan["split_level"],
              "subtask_count": len(plan["regions"]), "input_area_m2": plan["input_area_m2"],
              "pixel_mask_area_m2": round(inside_count * pixel_area, 2),
              "valid_area_m2": round(valid_count * pixel_area, 2),
              "nodata_area_m2": round((inside_count - valid_count) * pixel_area, 2),
              "coverage_percent": round(valid_count / inside_count * 100, 4),
              "pixel_size_m": abs(plan["geotransform"][1]), "area_method": "pixel_center",
              "classes": rows, "vector_feature_count": vector_count,
              "raster_url": f"/landcover/download/{task_id}/raster",
              "vector_url": f"/landcover/download/{task_id}/vector"}
    (task_dir / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result
