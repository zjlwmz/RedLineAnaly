"""小型合成行政区验证拆分边界、缺口和成果合并口径。"""
import json
import sqlite3
import shutil
import unittest
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import numpy as np
from osgeo import gdal, ogr, osr

from core.distributed_landcover import create_plan, finish_task, run_chunk
from ophub import landcover_jobs


class DistributedLandcoverTest(unittest.TestCase):
    def setUp(self):
        # 用 2×2 小块覆盖分配、Worker 裁剪和 Master 合并的跨块路径。
        tile_patch = patch("core.distributed_landcover.TILE_SIZE", 2)
        tile_patch.start()
        self.addCleanup(tile_patch.stop)
        self.root = Path(__file__).resolve().parent / f"_distributed_test_{uuid.uuid4().hex}"
        self.root.mkdir()
        self.admin = self.root / "admin"
        self.admin.mkdir()
        self.source = self.root / "source.tif"
        self.srs = osr.SpatialReference()
        self.srs.ImportFromEPSG(3857)
        self.srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        admin_srs = osr.SpatialReference()
        admin_srs.ImportFromEPSG(4326)
        admin_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        raster = gdal.GetDriverByName("GTiff").Create(str(self.source), 4, 4, 1, gdal.GDT_Byte)
        raster.SetGeoTransform((0, 30, 0, 120, 0, -30))
        raster.SetProjection(self.srs.ExportToWkt())
        band = raster.GetRasterBand(1)
        band.SetNoDataValue(0)
        band.WriteArray(np.array([[1, 1, 2, 9], [1, 2, 2, 9], [0, 2, 9, 9], [4, 4, 4, 4]], dtype=np.uint8))
        raster = None
        # 两个行政面在中间有重叠，最右一列不属于任一行政区。
        for level, name in (("province", "中国_省2.shp"), ("city", "中国_市2.shp"), ("county", "中国_县2.shp")):
            ds = ogr.GetDriverByName("ESRI Shapefile").CreateDataSource(str(self.admin / name))
            layer = ds.CreateLayer(level, srs=admin_srs, geom_type=ogr.wkbPolygon)
            layer.CreateField(ogr.FieldDefn("gb", ogr.OFTString))
            layer.CreateField(ogr.FieldDefn("name", ogr.OFTString))
            for code, x0, x1 in (("000000001", 0, 70), ("000000002", 60, 90)):
                geom = ogr.CreateGeometryFromWkt(f"POLYGON (({x0} 0,{x1} 0,{x1} 120,{x0} 120,{x0} 0))")
                geom.Transform(osr.CoordinateTransformation(self.srs, admin_srs))
                feature = ogr.Feature(layer.GetLayerDefn())
                feature.SetField("gb", code)
                feature.SetField("name", code)
                feature.SetGeometry(geom)
                layer.CreateFeature(feature)
            ds = None

    def tearDown(self):
        if self.root.resolve().parent == Path(__file__).resolve().parent:
            shutil.rmtree(self.root)

    def test_split_and_merge_preserve_each_pixel_once(self):
        geometry = {"type": "Polygon", "coordinates": [[[0, 0], [120, 0], [120, 120], [0, 120], [0, 0]]]}
        for level in ("province", "city", "county"):
            output = self.root / level
            task_id = uuid.uuid4().hex
            plan = create_plan(geometry, 3857, self.source, self.admin, level, output, 0,
                               {1: "耕地", 2: "林地", 4: "草地", 9: "湿地"}, 0, 1993, task_id=task_id)
            self.assertEqual(plan["task_id"], task_id)
            self.assertEqual(len(plan["regions"]), 3)
            self.assertEqual(plan["regions"][-1]["code"], "unassigned")
            # 逆序合并会覆盖彼此的包围框，能检出误写 NoData 的问题。
            chunk_vector_count = 0
            for region in reversed(plan["regions"]):
                first_chunk = run_chunk(output, plan["task_id"], region["label"])
                chunk_vector_count += first_chunk["vector_feature_count"]
                self.assertTrue((output / plan["task_id"] / f"chunk_{region['label']}.gpkg").is_file())
                self.assertEqual(run_chunk(output, plan["task_id"], region["label"])["pixel_count"],
                                 first_chunk["pixel_count"])
            result = finish_task(output, plan["task_id"])
            self.assertEqual(result["subtask_count"], 3)
            self.assertEqual(result["valid_area_m2"], 15 * 900)
            self.assertEqual(result["nodata_area_m2"], 900)
            raster = gdal.Open(str(output / plan["task_id"] / "CLCD_1993_clip.tif"))
            self.assertEqual(raster.GetRasterBand(1).ReadAsArray().tolist(),
                             [[1, 1, 2, 9], [1, 2, 2, 9], [0, 2, 9, 9], [4, 4, 4, 4]])
            raster = None
            vector = ogr.Open(str(output / plan["task_id"] / "CLCD_1993_classes.gpkg"))
            features = list(vector.GetLayer(0))
            self.assertEqual(len(features), chunk_vector_count)
            self.assertEqual(sum(f.GetField("area_m2") for f in features), 15 * 900)
            self.assertTrue(all(f.GetField("class_name") in {"耕地", "林地", "草地", "湿地"} for f in features))
            vector = None
            saved = json.loads((output / plan["task_id"] / "result.json").read_text(encoding="utf-8"))
            self.assertEqual(saved["valid_area_m2"], result["valid_area_m2"])

    def test_duplicate_result_does_not_repeat_aggregation(self):
        geometry = {"type": "Polygon", "coordinates": [[[0, 0], [120, 0], [120, 120], [0, 120], [0, 0]]]}
        output = self.root / "jobs"
        plan = create_plan(geometry, 3857, self.source, self.admin, "county", output, 100,
                           {1: "耕地", 2: "林地", 4: "草地", 9: "湿地"}, 0, 1993)
        fake_settings = SimpleNamespace(db_path=str(self.root / "tasks.db"), output_dir=str(output),
                                        landcover_task_queue="landcover_task_queue", role="master")
        with patch.object(landcover_jobs, "settings", fake_settings):
            landcover_jobs.init_db()
            landcover_jobs.save_plan(plan)
            published = []
            with patch.object(landcover_jobs, "connect", return_value=MagicMock()), \
                 patch.object(landcover_jobs, "_topology"), \
                 patch.object(landcover_jobs, "_publish", side_effect=lambda _channel, _queue, body: published.append(body)):
                landcover_jobs.dispatch_task(plan["task_id"])
            self.assertEqual(len(published), 3)
            self.assertTrue(all(body["output_root"] == str(output) for body in published))
            first = None
            for region in plan["regions"]:
                message = run_chunk(output, plan["task_id"], region["label"])
                if first is None:
                    first = message
                landcover_jobs.accept_result(message)
            self.assertEqual(landcover_jobs.task_state(plan["task_id"])["status"], "done")
            landcover_jobs.accept_result(first)
            state = landcover_jobs.task_state(plan["task_id"])
            self.assertEqual((state["status"], state["finished"]), ("done", 3))
            self.assertIsNotNone(state["finished_at"])
            self.assertFalse(state["finished_at_estimated"])
            self.assertGreaterEqual(state["duration_seconds"], 0)

    def test_existing_database_uses_result_time_for_historical_task(self):
        task_id = uuid.uuid4().hex
        db_path = self.root / "old_schema.db"
        output = self.root / "historical_output"
        task_dir = output / task_id
        task_dir.mkdir(parents=True)
        result_path = task_dir / "result.json"
        result_path.write_text("{}", encoding="utf-8")
        created_at = (datetime.now() - timedelta(seconds=20)).isoformat(timespec="seconds")
        with sqlite3.connect(db_path) as db:
            db.execute("CREATE TABLE landcover_tasks (task_id TEXT PRIMARY KEY, status TEXT NOT NULL, "
                       "split_level TEXT NOT NULL, created_at TEXT NOT NULL, error TEXT)")
            db.execute("INSERT INTO landcover_tasks VALUES (?, 'done', 'county', ?, NULL)", (task_id, created_at))
        db.close()
        fake_settings = SimpleNamespace(db_path=str(db_path), output_dir=str(output))
        with patch.object(landcover_jobs, "settings", fake_settings):
            landcover_jobs.init_db()
            state = landcover_jobs.task_state(task_id)
        self.assertTrue(state["finished_at_estimated"])
        self.assertEqual(state["finished_at"], datetime.fromtimestamp(result_path.stat().st_mtime).isoformat(timespec="seconds"))
        self.assertGreaterEqual(state["duration_seconds"], 19)
        with sqlite3.connect(db_path) as db:
            self.assertIn("finished_at", {row[1] for row in db.execute("PRAGMA table_info(landcover_tasks)")})
        db.close()

    def test_old_slave_result_without_vector_is_rejected(self):
        geometry = {"type": "Polygon", "coordinates": [[[0, 0], [120, 0], [120, 120], [0, 120], [0, 0]]]}
        output = self.root / "old_slave"
        plan = create_plan(geometry, 3857, self.source, self.admin, "county", output, 0,
                           {1: "耕地", 2: "林地", 4: "草地", 9: "湿地"}, 0, 1993)
        fake_settings = SimpleNamespace(db_path=str(self.root / "old_slave.db"), output_dir=str(output))
        with patch.object(landcover_jobs, "settings", fake_settings):
            landcover_jobs.init_db()
            landcover_jobs.save_plan(plan)
            landcover_jobs.accept_result({"task_id": plan["task_id"], "label": plan["regions"][0]["label"],
                                          "status": "success", "pixel_count": 1})
            state = landcover_jobs.task_state(plan["task_id"])
            self.assertEqual(state["status"], "failed")
            self.assertIn("重启 Slave", state["error"])

    def test_existing_plan_without_chunk_vectors_still_finishes(self):
        geometry = {"type": "Polygon", "coordinates": [[[0, 0], [120, 0], [120, 120], [0, 120], [0, 0]]]}
        output = self.root / "legacy"
        plan = create_plan(geometry, 3857, self.source, self.admin, "county", output, 0,
                           {1: "耕地", 2: "林地", 4: "草地", 9: "湿地"}, 0, 1993)
        plan.pop("chunk_vectors")
        (output / plan["task_id"] / "plan.json").write_text(json.dumps(plan, ensure_ascii=False), encoding="utf-8")
        for region in plan["regions"]:
            result = run_chunk(output, plan["task_id"], region["label"])
            self.assertNotIn("vector_feature_count", result)
        self.assertEqual(finish_task(output, plan["task_id"])["valid_area_m2"], 15 * 900)

    def test_failed_task_can_reuse_plan_and_retry(self):
        geometry = {"type": "Polygon", "coordinates": [[[0, 0], [120, 0], [120, 120], [0, 120], [0, 0]]]}
        output = self.root / "retry"
        plan = create_plan(geometry, 3857, self.source, self.admin, "county", output, 0,
                           {1: "耕地", 2: "林地", 4: "草地", 9: "湿地"}, 0, 1993)
        fake_settings = SimpleNamespace(db_path=str(self.root / "retry.db"), output_dir=str(output))
        with patch.object(landcover_jobs, "settings", fake_settings):
            landcover_jobs.init_db()
            landcover_jobs.save_plan(plan)
            landcover_jobs.accept_result({"task_id": plan["task_id"], "label": plan["regions"][0]["label"],
                                          "status": "failed", "error": "旧目录"})
            failed_state = landcover_jobs.task_state(plan["task_id"])
            self.assertEqual(failed_state["status"], "failed")
            self.assertIsNotNone(failed_state["finished_at"])
            state = landcover_jobs.retry_failed_task(plan["task_id"])
            self.assertEqual((state["status"], state["finished"]), ("pending", 0))
            self.assertIsNone(state["finished_at"])
            for region in plan["regions"]:
                landcover_jobs.accept_result(run_chunk(output, plan["task_id"], region["label"]))
            self.assertEqual(landcover_jobs.task_state(plan["task_id"])["status"], "done")


if __name__ == "__main__":
    unittest.main()
