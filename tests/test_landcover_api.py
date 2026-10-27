"""验证分析接口先返回任务 ID，后台规划结果可通过状态接口查询。"""
import json
import shutil
import threading
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from ophub import landcover_api, landcover_jobs


class AnalyzeAsyncTest(unittest.TestCase):
    def setUp(self):
        root = Path(__file__).resolve().parent / f"_landcover_api_test_{uuid.uuid4().hex}"
        root.mkdir()
        self.addCleanup(shutil.rmtree, root)
        fake_settings = SimpleNamespace(db_path=str(root / "tasks.db"), output_dir=str(root / "out"),
                                        split_level="county", clcd_raster=str(root / "source.tif"),
                                        admin_data_dir=str(root / "admin"), max_export_pixels=0)
        for module in (landcover_api, landcover_jobs):
            setting_patch = patch.object(module, "settings", fake_settings)
            setting_patch.start()
            self.addCleanup(setting_patch.stop)
        landcover_jobs.init_db()
        self.request = landcover_api.AnalyzeRequest(geometry={"type": "Polygon", "coordinates": []})

    def test_response_precedes_planning_and_dispatch(self):
        planning_started = threading.Event()
        continue_planning = threading.Event()
        dispatched = threading.Event()

        def create_plan(**kwargs):
            planning_started.set()
            if not continue_planning.wait(5):
                raise TimeoutError("测试等待规划超时")
            return {"task_id": kwargs["task_id"], "split_level": kwargs["split_level"],
                    "regions": [{"label": 1, "code": "001", "name": "测试区"}]}

        with patch.object(landcover_api, "_classes", return_value={"classes": [{"code": 1, "name": "耕地"}],
                                                                  "nodata": 0, "year": 1993}), \
             patch.object(landcover_api, "create_plan", side_effect=create_plan), \
             patch.object(landcover_jobs, "dispatch_task", side_effect=lambda _id: dispatched.set()):
            try:
                response = landcover_api.analyze(self.request)
                body = json.loads(response.body)
                task_id = body["data"]["task_id"]
                self.assertEqual(response.status_code, 202)
                self.assertEqual(set(body["data"]), {"task_id"})
                self.assertTrue(planning_started.wait(2))
                state = landcover_api.result(task_id)["data"]
                self.assertEqual((state["status"], state["total"]), ("planning", 0))
            finally:
                continue_planning.set()
            self.assertTrue(dispatched.wait(2), landcover_jobs.task_state(task_id))
            state = landcover_jobs.task_state(task_id)
            self.assertEqual((state["status"], state["total"]), ("pending", 1))

    def test_planning_failure_is_queryable(self):
        failed = threading.Event()
        original_fail = landcover_jobs._fail_task

        def record_failure(task_id, error):
            original_fail(task_id, error)
            failed.set()

        with patch.object(landcover_api, "_classes", side_effect=ValueError("分类配置错误")), \
             patch.object(landcover_jobs, "_fail_task", side_effect=record_failure):
            response = landcover_api.analyze(self.request)
            task_id = json.loads(response.body)["data"]["task_id"]
            self.assertTrue(failed.wait(2))
        state = landcover_api.result(task_id)["data"]
        self.assertEqual(state["status"], "failed")
        self.assertEqual(state["error"], "分类配置错误")
        self.assertIsNotNone(state["finished_at"])

    def test_interrupted_planning_is_failed_before_recovery(self):
        task_id = "a" * 32
        landcover_jobs.create_task(task_id, "county")
        landcover_jobs.fail_interrupted_planning()
        state = landcover_jobs.task_state(task_id)
        self.assertEqual(state["status"], "failed")
        self.assertIn("重新提交", state["error"])


if __name__ == "__main__":
    unittest.main()
