"""按输入面统计 CLCD 分类，并导出同一像元口径的栅格和矢量成果。"""
from __future__ import annotations

import json
import math
import uuid
from pathlib import Path

import numpy as np
from osgeo import gdal, ogr, osr

gdal.UseExceptions()
ogr.UseExceptions()
osr.UseExceptions()


def analyze_landcover(
    geometry: dict,
    geometry_srid: int,
    raster_path: Path,
    output_root: Path,
    classes: dict[int, str],
    nodata: int,
    year: int,
    max_pixels: int,
) -> dict:
    """统计原始 30 米像元中心落入面内的类别，并输出裁剪栅格和分类面。"""
    if not raster_path.is_file():
        raise ValueError(f"CLCD 栅格不存在：{raster_path}")
    raster = gdal.Open(str(raster_path), gdal.GA_ReadOnly)
    if raster is None or raster.RasterCount != 1:
        raise ValueError("CLCD 栅格不存在或不是单波段数据")
    band = raster.GetRasterBand(1)
    if band.GetNoDataValue() != nodata:
        raise ValueError("分类配置中的 NoData 与栅格元数据不一致")
    gt = raster.GetGeoTransform()
    if gt[2] != 0 or gt[4] != 0 or gt[1] <= 0 or gt[5] >= 0:
        raise ValueError("当前仅支持北向、无旋转的分类栅格")

    geom = ogr.CreateGeometryFromJson(json.dumps(geometry, ensure_ascii=False))
    if geom is None or geom.IsEmpty():
        raise ValueError("输入面为空或 GeoJSON 几何无效")
    if ogr.GT_Flatten(geom.GetGeometryType()) not in (ogr.wkbPolygon, ogr.wkbMultiPolygon):
        raise ValueError("输入必须是 Polygon 或 MultiPolygon")
    if not geom.IsValid():
        raise ValueError("输入面存在自相交等拓扑错误，请先修复")
    if geometry_srid <= 0:
        raise ValueError("geometry_srid 必须是有效的 EPSG 编码")

    source_srs = osr.SpatialReference()
    if source_srs.ImportFromEPSG(geometry_srid) != 0:
        raise ValueError(f"不支持输入坐标系 EPSG:{geometry_srid}")
    source_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    raster_srs = osr.SpatialReference(wkt=raster.GetProjectionRef())
    if not raster_srs.IsProjected():
        raise ValueError("CLCD 栅格缺少有效的投影坐标系")
    raster_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    geom.Transform(osr.CoordinateTransformation(source_srs, raster_srs))
    input_area_m2 = geom.GetArea()
    if not math.isfinite(input_area_m2) or input_area_m2 <= 0:
        raise ValueError("输入面投影后面积无效")

    # 用输入面的投影包围框定位源栅格窗口；绝不读取全国整幅栅格。
    minx, maxx, miny, maxy = geom.GetEnvelope()
    x0 = max(0, math.floor((minx - gt[0]) / gt[1]))
    x1 = min(raster.RasterXSize, math.ceil((maxx - gt[0]) / gt[1]))
    y0 = max(0, math.floor((maxy - gt[3]) / gt[5]))
    y1 = min(raster.RasterYSize, math.ceil((miny - gt[3]) / gt[5]))
    width, height = x1 - x0, y1 - y0
    if width <= 0 or height <= 0:
        raise ValueError("输入面与 1993 年 CLCD 栅格范围不相交")
    if width * height > max_pixels:
        raise ValueError(f"范围过大：需处理 {width * height:,} 个像元，当前上限 {max_pixels:,}；请缩小输入面")

    clip_gt = (gt[0] + x0 * gt[1], gt[1], 0.0, gt[3] + y0 * gt[5], 0.0, gt[5])
    mask_ds = gdal.GetDriverByName("MEM").Create("", width, height, 1, gdal.GDT_Byte)
    mask_ds.SetGeoTransform(clip_gt)
    mask_ds.SetProjection(raster.GetProjectionRef())
    vector_driver = ogr.GetDriverByName("MEM") or ogr.GetDriverByName("Memory")
    vector_ds = vector_driver.CreateDataSource("")
    vector_layer = vector_ds.CreateLayer("input", srs=raster_srs, geom_type=ogr.wkbUnknown)
    feature = ogr.Feature(vector_layer.GetLayerDefn())
    feature.SetGeometry(geom)
    vector_layer.CreateFeature(feature)
    feature = None
    gdal.RasterizeLayer(mask_ds, [1], vector_layer, burn_values=[1])
    inside = mask_ds.GetRasterBand(1).ReadAsArray() != 0
    values = band.ReadAsArray(x0, y0, width, height)
    if values is None:
        raise ValueError("无法读取输入面覆盖的 CLCD 栅格")
    inside_count = int(np.count_nonzero(inside))
    if inside_count == 0:
        raise ValueError("输入面小于像元或未覆盖任何像元中心")

    known = np.isin(values, list(classes))
    unknown = inside & (values != nodata) & ~known
    if np.any(unknown):
        codes = np.unique(values[unknown]).tolist()
        raise ValueError(f"CLCD 栅格出现未配置的分类码：{codes}")

    valid = inside & (values != nodata)
    valid_count = int(np.count_nonzero(valid))
    pixel_area_m2 = abs(gt[1] * gt[5])
    rows = []
    for code, name in sorted(classes.items()):
        count = int(np.count_nonzero(valid & (values == code)))
        rows.append({
            "code": code,
            "name": name,
            "pixel_count": count,
            "area_m2": round(count * pixel_area_m2, 2),
            "percent": round(count / valid_count * 100, 4) if valid_count else 0.0,
        })

    task_id = uuid.uuid4().hex
    task_dir = output_root / task_id
    task_dir.mkdir(parents=True, exist_ok=False)
    raster_file = task_dir / f"CLCD_{year}_clip.tif"
    vector_file = task_dir / f"CLCD_{year}_classes.gpkg"

    # 面外及源栅格 NoData 都写 0；统计、GeoTIFF 和矢量化共用同一掩膜口径。
    clipped = np.where(valid, values, nodata).astype(np.uint8)
    out_ds = gdal.GetDriverByName("GTiff").Create(
        str(raster_file), width, height, 1, gdal.GDT_Byte,
        options=["TILED=YES", "COMPRESS=LZW"],
    )
    out_ds.SetGeoTransform(clip_gt)
    out_ds.SetProjection(raster.GetProjectionRef())
    out_band = out_ds.GetRasterBand(1)
    # TIFF 写入像元后不能再修改调色板相关标签，必须先设置颜色表。
    out_band.SetNoDataValue(nodata)
    if band.GetColorTable() is not None:
        out_band.SetColorTable(band.GetColorTable())
    out_band.WriteArray(clipped)
    out_band.FlushCache()

    gpkg_ds = ogr.GetDriverByName("GPKG").CreateDataSource(str(vector_file))
    gpkg_layer = gpkg_ds.CreateLayer("landcover", srs=raster_srs, geom_type=ogr.wkbPolygon)
    gpkg_layer.CreateField(ogr.FieldDefn("class_code", ogr.OFTInteger))
    gpkg_layer.CreateField(ogr.FieldDefn("class_name", ogr.OFTString))
    gpkg_layer.CreateField(ogr.FieldDefn("area_m2", ogr.OFTReal))
    # Polygonize 仅处理非 NoData 像元；输出的是像元边界形成的分类斑块。
    gdal.Polygonize(out_band, out_band.GetMaskBand(), gpkg_layer, 0, [])
    vector_count = 0
    for polygon in gpkg_layer:
        polygon.SetField("class_name", classes[polygon.GetField("class_code")])
        polygon.SetField("area_m2", round(polygon.GetGeometryRef().GetArea(), 2))
        gpkg_layer.SetFeature(polygon)
        vector_count += 1
    gpkg_ds = None
    out_ds = None

    result = {
        "task_id": task_id,
        "year": year,
        "input_area_m2": round(input_area_m2, 2),
        "pixel_mask_area_m2": round(inside_count * pixel_area_m2, 2),
        "valid_area_m2": round(valid_count * pixel_area_m2, 2),
        "nodata_area_m2": round((inside_count - valid_count) * pixel_area_m2, 2),
        "coverage_percent": round(valid_count / inside_count * 100, 4),
        "pixel_size_m": abs(gt[1]),
        "area_method": "pixel_center",
        "classes": rows,
        "vector_feature_count": vector_count,
        "raster_url": f"/landcover/download/{task_id}/raster",
        "vector_url": f"/landcover/download/{task_id}/vector",
    }
    (task_dir / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result
