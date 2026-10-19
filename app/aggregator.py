"""结果聚合与合规判定。

原方案的 `is_compliant = overlap_ratio < 1` 太粗，这里给出可解释的四级结论：

  超界（OUTSIDE）  输入面有显著面积落在国界外 —— 结果不可信，拒绝给结论
  压盖（OCCUPIED） 实际压盖面积 > 0，占比 < 严重阈值
  严重压盖（SEVERE）压盖占比 ≥ 严重阈值（默认 50%，需按客户标准确认）
  未压盖（CLEAR）   实际压盖面积 ≈ 0

“压盖占比”用**并集面积**算，不是各省面积相加 —— 跨省时同一块重叠会被两侧各算一次，
相加会 >100%，这是个很容易踩的坑。
"""
from __future__ import annotations

import json
import threading
import time
from typing import Optional

from osgeo import ogr

from . import geo
from .config import settings

ogr.UseExceptions()

SEVERITIES = ("CLEAR", "OCCUPIED", "SEVERE", "OUTSIDE")


class JobStore:
    """批次状态。演示用内存 + TTL；生产换成 PG，batch_id/task_id 建唯一索引即可幂等。"""

    def __init__(self) -> None:
        self._jobs: dict[str, dict] = {}
        self._lock = threading.Lock()

    def create(self, batch_id: str, payload: dict) -> dict:
        with self._lock:
            job = {
                "batch_id": batch_id,
                "feature_id": payload.get("feature_id"),
                "created_at": time.time(),
                "input": payload,
                "results": {},       # task_id -> result（幂等去重）
                "expected": 0,
                "finished": False,
                "notified": threading.Event(),
            }
            self._jobs[batch_id] = job
            self._evict()
            return job

    def _evict(self) -> None:
        if len(self._jobs) <= settings.max_jobs_in_memory:
            return
        for bid, _ in sorted(self._jobs.items(), key=lambda kv: kv[1]["created_at"])[
            : len(self._jobs) - settings.max_jobs_in_memory
        ]:
            if self._jobs[bid]["finished"]:
                self._jobs.pop(bid, None)

    def get(self, batch_id: str) -> Optional[dict]:
        with self._lock:
            return self._jobs.get(batch_id)

    def add_result(self, result: dict) -> None:
        batch_id = result.get("batch_id")
        task_id = result.get("task_id")
        job = self.get(batch_id)
        if job is None or not task_id:
            return
        with self._lock:
            # 幂等：同一 task_id 重复投递只保留第一次成功的结果
            prev = job["results"].get(task_id)
            if prev is not None and prev.get("status") == "success" and result.get("status") != "success":
                return
            job["results"][task_id] = result
            if job["expected"] and len(job["results"]) >= job["expected"]:
                job["finished"] = True
                job["notified"].set()

    def wait(self, batch_id: str, timeout: float) -> bool:
        job = self.get(batch_id)
        if job is None:
            return False
        return job["notified"].wait(timeout)


store = JobStore()


# --------------------------------------------------------------------------- #
def aggregate(
    job: dict,
    province_hits: list[dict],
    input_area_m2: float,
    input_area_ellipsoid: float,
    severe_ratio: float,
    buffer_m: float,
    extra_warnings: list[Optional[str]],
) -> dict:
    results = list(job["results"].values())
    ok = [r for r in results if r.get("status") in ("success", "no_data")]
    failed = [r for r in results if r.get("status") not in ("success", "no_data")]
    pending = [p for p in province_hits if p["code"] not in {r["province_code"] for r in ok + failed}]

    # 省级明细
    provinces_out = []
    for r in sorted(ok, key=lambda x: x.get("province_name", "")):
        provinces_out.append({
            "province_code": r.get("province_code"),
            "province_name": r.get("province_name"),
            "status": r.get("status"),
            "patch_count": r.get("patch_count", 0),
            "patches_scanned": r.get("patches_scanned", 0),
            "overlap_area": r.get("province_overlap_area", 0.0),
            "union_overlap_area": r.get("province_union_area", 0.0),
            "by_type": r.get("by_type", []),
            "elapsed_ms": r.get("total_elapsed_ms", r.get("elapsed_ms")),
            "worker_id": r.get("worker_id"),
            "data_version": r.get("data_version"),
            "warnings": r.get("warnings", []),
        })

    # 图斑明细：跨省同一图斑合并
    patches: dict[str, dict] = {}
    for r in ok:
        for m in r.get("matches", []):
            key = m["patch_id"]
            p = patches.get(key)
            if p is None:
                p = patches[key] = {
                    "patch_id": key,
                    "redline_type": m["redline_type"],
                    "overlap_area": 0.0,
                    "provinces": [],
                    "geometry": m.get("geometry"),
                }
            p["overlap_area"] = round(p["overlap_area"] + m["overlap_area"], 2)
            p["provinces"].append(r.get("province_name"))

    # 按类型汇总（跨省相加：不同省的红线是不同地块，可以直接加）
    by_type: dict[str, dict] = {}
    for p in patches.values():
        b = by_type.setdefault(p["redline_type"], {"redline_type": p["redline_type"], "overlap_area": 0.0, "patch_count": 0})
        b["overlap_area"] = round(b["overlap_area"] + p["overlap_area"], 2)
        b["patch_count"] += 1

    sum_overlap = round(sum(p["overlap_area"] for p in patches.values()), 2)

    # 并集面积：把各省重叠几何拼起来 union，得到“真正被压盖的面积”
    union_area, union_geom = _union_matches([m for r in ok for m in r.get("matches", [])])

    ratio_of_original = round(sum_overlap / input_area_m2 * 100, 4) if input_area_m2 else 0.0
    ratio_union = round(union_area / input_area_m2 * 100, 4) if input_area_m2 else 0.0
    # 相加口径可能因为重叠而略超 100%，封顶展示
    ratio_sum_capped = min(ratio_of_original, 100.0)

    warnings = [w for w in extra_warnings if w]
    for p in provinces_out:
        warnings.extend(p["warnings"])
    if failed:
        warnings.append("以下省份计算失败：" + "、".join(f"{r['province_name']}({r.get('error', '未知')})" for r in failed))
    if pending:
        warnings.append("以下省份未在超时时间内返回：" + "、".join(p["name"] for p in pending))
    if input_area_m2 and union_area > input_area_m2 * 1.002:
        warnings.append("重叠并集面积大于输入面面积，请检查红线数据是否越界或跨省重复存储")

    outside = any("国界" in w for w in warnings)
    if outside:
        severity, compliant = "OUTSIDE", None
    elif union_area <= 1e-6:
        severity, compliant = "CLEAR", True
    elif ratio_union >= severe_ratio * 100:
        severity, compliant = "SEVERE", False
    else:
        severity, compliant = "OCCUPIED", False

    return {
        "batch_id": job["batch_id"],
        "feature_id": job.get("feature_id"),
        "input_area_m2": round(input_area_m2, 2),
        "input_area_ellipsoid_m2": round(input_area_ellipsoid, 2),
        "buffer_m": buffer_m,
        "severe_ratio": severe_ratio,
        # --- 结论 ---
        "is_compliant": compliant,
        "severity": severity,
        "severity_text": {
            "CLEAR": "未压盖红线",
            "OCCUPIED": "压盖红线",
            "SEVERE": "严重压盖（已达预警阈值）",
            "OUTSIDE": "输入面超出国土范围，结论无效",
        }[severity],
        # --- 指标（两个口径都给，避免客户拿错数字）---
        "sum_overlap_area_m2": sum_overlap,
        "union_overlap_area_m2": round(union_area, 2),
        "overlap_ratio": ratio_sum_capped,       # 兼容原方案字段名，口径 = 各图斑相加 / 输入面
        "overlap_ratio_union": ratio_union,      # 推荐口径 = 并集 / 输入面
        # --- 明细 ---
        "by_type": sorted(by_type.values(), key=lambda x: -x["overlap_area"]),
        "provinces": provinces_out,
        "patches": sorted(patches.values(), key=lambda x: -x["overlap_area"]),
        "overlap_geometry": union_geom,
        "task_summary": {
            "expected": len(province_hits),
            "succeeded": len([r for r in ok if r.get("status") == "success"]),
            "failed": len(failed),
            "pending": len(pending),
        },
        "warnings": warnings,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    }


def _union_matches(matches: list[dict]) -> tuple[float, Optional[dict]]:
    geoms = []
    for m in matches:
        gj = m.get("geometry")
        if not gj:
            continue
        g = ogr.CreateGeometryFromJson(json.dumps(gj))
        if g is not None and not g.IsEmpty():
            geoms.append(g)
    if not geoms:
        return 0.0, None
    union = geoms[0]
    for g in geoms[1:]:
        union = union.Union(g)
    return geo.area_m2(union), json.loads(union.ExportToJson())
