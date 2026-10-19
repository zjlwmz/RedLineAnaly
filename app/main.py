"""主节点：API + 结果收集 + 静态页面。"""
from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Optional

import pika
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from osgeo import ogr
from pydantic import BaseModel, Field

from . import aggregator, geo, province, topology
from .config import settings
from .overlay import overlay_province
from .publisher import get_publisher, new_batch_id
from .redline_index import RedlineStore

ogr.UseExceptions()

WEB_DIR = Path(__file__).resolve().parent.parent / "web"

app = FastAPI(title="红线叠加分析", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# --------------------------------------------------------------------------- #
# 请求/响应模型
# --------------------------------------------------------------------------- #
class AnalyzeRequest(BaseModel):
    feature_id: str = Field(..., description="输入面唯一标识")
    geometry: dict = Field(..., description="GeoJSON geometry")
    geometry_srid: int = Field(4326, description="输入坐标系 EPSG")
    severe_ratio: Optional[float] = Field(None, ge=0.0, le=1.0, description="严重压盖阈值，0~1")
    buffer_m: Optional[float] = Field(None, ge=0.0, le=5000.0, description="红线缓冲距离（米）")
    wait_timeout: Optional[float] = Field(None, ge=0.0, le=300.0)
    prefer_local: bool = Field(False, description="强制本地计算（不走 MQ），用于调试")


# --------------------------------------------------------------------------- #
# 结果收集线程
# --------------------------------------------------------------------------- #
def _result_consumer() -> None:
    while True:
        try:
            conn = topology.connect()
            ch = conn.channel()
            topology.declare_topology(ch)
            ch.basic_qos(prefetch_count=16)

            def on_result(chan, method, properties, body):  # noqa: ARG001
                try:
                    aggregator.store.add_result(json.loads(body))
                except Exception as exc:  # pragma: no cover
                    print(f"[main] 结果解析失败: {exc}", flush=True)
                chan.basic_ack(method.delivery_tag)

            ch.basic_consume(queue=settings.task_result_queue, on_message_callback=on_result)
            print("[main] 结果收集线程已启动", flush=True)
            ch.start_consuming()
        except Exception as exc:  # pragma: no cover
            print(f"[main] 结果收集线程异常，5s 后重连: {exc}", flush=True)
            time.sleep(5)


@app.on_event("startup")
def _startup() -> None:
    threading.Thread(target=_result_consumer, name="result-consumer", daemon=True).start()


# --------------------------------------------------------------------------- #
# 核心接口
# --------------------------------------------------------------------------- #
@app.post("/api/v1/overlay/analyze")
def analyze(req: AnalyzeRequest):
    severe_ratio = settings.severe_ratio if req.severe_ratio is None else req.severe_ratio
    buffer_m = settings.buffer_m if req.buffer_m is None else req.buffer_m
    wait_timeout = settings.wait_timeout if req.wait_timeout is None else req.wait_timeout

    # 1) 坐标统一 + 几何校验
    try:
        geom = geo.parse_geojson_geometry(req.geometry, req.geometry_srid)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"几何无效：{exc}")

    area = geo.area_m2(geom)
    if area <= 0:
        raise HTTPException(status_code=400, detail="输入面面积为 0")
    if area > settings.max_input_area_m2:
        raise HTTPException(
            status_code=400,
            detail=f"输入面过大（{area / 1e6:.1f} km²），上限 {settings.max_input_area_m2 / 1e6:.0f} km²",
        )
    ellipsoid_area = geo.ellipsoid_area_m2(geom)

    # 2) 动态省份识别
    detect = province.detect_provinces(geom)
    hits = [p for p in detect.provinces if p.match == "geom"]
    extra_warnings: list[Optional[str]] = list(detect.warnings)

    if not hits:
        return aggregator.aggregate(
            job=aggregator.JobStore().create(new_batch_id(), {"feature_id": req.feature_id}),
            province_hits=[],
            input_area_m2=area,
            input_area_ellipsoid=ellipsoid_area,
            severe_ratio=severe_ratio,
            buffer_m=buffer_m,
            extra_warnings=extra_warnings + ["输入面未落在任何省级行政区范围内"],
        )

    # 3) 建批次
    batch_id = new_batch_id()
    job = aggregator.store.create(batch_id, {"feature_id": req.feature_id})
    job["expected"] = len(hits)

    base_payload = {
        "batch_id": batch_id,
        "feature_id": req.feature_id,
        "input_wkt": geom.ExportToWkt(),
        "input_srid": geo.SRID_CGCS2000_GEO,
        "input_area_m2": area,
        "buffer_m": buffer_m,
    }

    hits_obj = [{"code": h.code, "name": h.name} for h in hits]

    # 4) 分发；MQ 不可用时降级本地串行计算（保证 demo 可跑，生产应直接失败告警）
    tasks, workers = [], []
    if not req.prefer_local:
        try:
            pub = get_publisher()
            if pub.wait_ready(3.0):
                tasks, workers = pub.fanout(base_payload, hits_obj)
        except Exception as exc:
            extra_warnings.append(f"消息队列不可用，已降级本地计算：{exc}")

    dispatch_mode = "mq" if tasks else "local"
    if dispatch_mode == "local":
        store_ = RedlineStore()
        for h in hits_obj:
            result = overlay_province(
                input_geom=geom,
                province_code=h["code"],
                province_name=h["name"],
                buffer_m=buffer_m,
                input_area_m2=area,
                store=store_,
            )
            result.update({
                "batch_id": batch_id,
                "task_id": f"{batch_id}:{h['code']}",
                "worker_id": "local",
                "status": result.get("status", "success"),
                "total_elapsed_ms": result.get("elapsed_ms"),
            })
            aggregator.store.add_result(result)
        job["finished"] = True
    else:
        # 5) 等结果（可能部分超时）
        aggregator.store.wait(batch_id, wait_timeout)

    # 6) 汇总
    out = aggregator.aggregate(
        job=job,
        province_hits=hits_obj,
        input_area_m2=area,
        input_area_ellipsoid=ellipsoid_area,
        severe_ratio=severe_ratio,
        buffer_m=buffer_m,
        extra_warnings=extra_warnings,
    )
    out["dispatch"] = {
        "mode": dispatch_mode,
        "workers": workers,
        "provinces_detected": [
            {"code": h.code, "name": h.name, "area_ratio": h.area_ratio} for h in hits
        ],
        "detect_ms": detect.elapsed_ms,
    }
    return out


@app.get("/api/v1/overlay/jobs/{batch_id}")
def get_job(batch_id: str):
    job = aggregator.store.get(batch_id)
    if job is None:
        raise HTTPException(status_code=404, detail="批次不存在或已过期")
    return {
        "batch_id": batch_id,
        "expected": job["expected"],
        "received": len(job["results"]),
        "finished": job["finished"],
        "results": list(job["results"].values()),
    }


@app.get("/api/v1/provinces")
def provinces():
    return [{"code": p["code"], "name": p["name"]} for p in province.load_provinces()]


@app.get("/api/v1/redline/{province_code}")
def redline_preview(province_code: str, simplify: float = 0.002):
    """导出一个省的红线图斑（简化后的 GeoJSON），仅供前端预览核对。
    真正的计算永远是服务端拿原始几何做的，前端这份只是显示。"""
    store = RedlineStore()
    features = []
    for patch in store.iter_patches(province_code):
        g = patch["geometry"]
        s = g.SimplifyPreserveTopology(simplify) if simplify > 0 else g
        features.append({
            "type": "Feature",
            "properties": {"patch_id": patch["patch_id"], "redline_type": patch["redline_type"]},
            "geometry": json.loads((s or g).ExportToJson()),
        })
    return {"type": "FeatureCollection", "features": features}


@app.get("/api/v1/meta")
def meta():
    try:
        pub = get_publisher()
        workers = pub.workers()
        mq_ok = True
    except Exception:
        workers, mq_ok = [], False
    from .registry import discover_workers

    _, source = discover_workers()
    return {
        "mq_available": mq_ok,
        "workers": workers,
        "worker_source": source,
        "severe_ratio": settings.severe_ratio,
        "buffer_m": settings.buffer_m,
        "redline_stats": _redline_stats(),
    }


def _redline_stats() -> dict:
    stats = {}
    store = RedlineStore()
    for p in province.load_provinces():
        m = store.layer_meta(p["code"])
        if m["available"]:
            stats[p["name"]] = m["count"]
    return stats


@app.get("/healthz")
def healthz():
    return {"ok": True, "ts": int(time.time())}


# --------------------------------------------------------------------------- #
# 静态页
# --------------------------------------------------------------------------- #
@app.get("/")
def index():
    return FileResponse(WEB_DIR / "index.html")


if (WEB_DIR / "assets").exists():
    app.mount("/assets", StaticFiles(directory=str(WEB_DIR / "assets")), name="assets")
app.mount("/data", StaticFiles(directory=str(settings.data_dir)), name="data")
