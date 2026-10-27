"""小型合成栅格验证面积统计与两种导出成果的一致性。"""
import shutil
import unittest
import uuid
from pathlib import Path

import numpy as np
from osgeo import gdal, ogr, osr

from core.landcover import analyze_landcover


class LandcoverTest(unittest.TestCase):
    def setUp(self):
        self.root = Path(__file__).resolve().parent / f"_landcover_test_{uuid.uuid4().hex}"
        self.root.mkdir()

    def tearDown(self):
        # 仅清理本用例在 tests 下创建的目录，避免碰到其他测试数据。
        tests_dir = Path(__file__).resolve().parent
        if self.root.resolve().parent == tests_dir:
            shutil.rmtree(self.root)

    def _source(self, values):
        source = self.root / "source.tif"
        ds = gdal.GetDriverByName("GTiff").Create(str(source), 4, 4, 1, gdal.GDT_Byte)
        ds.SetGeoTransform((0, 30, 0, 120, 0, -30))
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(3857)
        ds.SetProjection(srs.ExportToWkt())
        colors = gdal.ColorTable()
        colors.SetColorEntry(0, (0, 0, 0, 0))
        colors.SetColorEntry(1, (250, 227, 156, 255))
        ds.GetRasterBand(1).SetColorTable(colors)
        ds.GetRasterBand(1).WriteArray(np.array(values, dtype=np.uint8))
        ds.GetRasterBand(1).SetNoDataValue(0)
        ds = None
        return source

    def test_statistics_and_exports_use_same_mask(self):
        source = self._source([
            [1, 1, 2, 0],
            [1, 2, 2, 9],
            [0, 2, 9, 9],
            [4, 4, 4, 4],
        ])
        geometry = {"type": "Polygon", "coordinates": [[[0, 30], [90, 30], [90, 120], [0, 120], [0, 30]]]}
        result = analyze_landcover(geometry, 3857, source, self.root / "outputs", {1: "耕地", 2: "林地", 9: "湿地"}, 0, 1993, 100)
        rows = {row["code"]: row for row in result["classes"]}
        self.assertEqual((rows[1]["pixel_count"], rows[2]["pixel_count"], rows[9]["pixel_count"]), (3, 4, 1))
        self.assertEqual(result["valid_area_m2"], 7200)
        self.assertEqual(result["nodata_area_m2"], 900)
        self.assertAlmostEqual(sum(row["percent"] for row in rows.values()), 100, places=3)

        task_dir = self.root / "outputs" / result["task_id"]
        raster = gdal.Open(str(task_dir / "CLCD_1993_clip.tif"))
        self.assertEqual((raster.RasterXSize, raster.RasterYSize), (3, 3))
        self.assertEqual(raster.GetRasterBand(1).ReadAsArray().tolist(), [[1, 1, 2], [1, 2, 2], [0, 2, 9]])
        raster = None
        vector = ogr.Open(str(task_dir / "CLCD_1993_classes.gpkg"))
        layer = vector.GetLayer(0)
        self.assertEqual(sum(feature.GetField("area_m2") for feature in layer), 7200)
        vector = None
        self.assertTrue((task_dir / "result.json").is_file())

    def test_large_window_is_rejected_before_export(self):
        source = self._source([[1] * 4] * 4)
        geometry = {"type": "Polygon", "coordinates": [[[0, 0], [120, 0], [120, 120], [0, 120], [0, 0]]]}
        with self.assertRaisesRegex(ValueError, "范围过大"):
            analyze_landcover(geometry, 3857, source, self.root / "outputs", {1: "耕地"}, 0, 1993, 10)
        self.assertFalse((self.root / "outputs").exists())


if __name__ == "__main__":
    unittest.main()
