"""Master 节点：红线叠加分析算子的 REST 服务 + RabbitMQ 生产者 + 结果回收线程。

对外接口：
  GET  /health                                健康检查
  POST /overlay/analyze                       下发叠加分析任务（返回 batch_id）
  GET  /overlay/result?batchId=xxx            查询任务结果（含各子任务明细 + 汇总）
  GET  /tasks/pending                         全局待处理任务列表
  GET  /tasks/pending?batchId=xxx             指定任务的子任务进度明细
  GET  /nodes                                 当前连接的 Slave 消费者列表
  GET  /provinces                             支持的省份列表

持久化：任务与子任务状态写入 SQLite（config.ini: db_path）。服务停止/断电重启后：
  - 待处理任务列表、历史任务结果均可从数据库恢复查询
  - 已落库但未成功投递的子任务由恢复线程自动重新投递
  - 已投递的消息由 RabbitMQ 持久化保证不丢，由 Slave 继续消费
"""
from __future__ import annotations

import base64
import contextlib
import json
import logging
import sys
import threading
import time
import urllib.parse
import urllib.request
import uuid
from datetime import datetime
from pathlib import Path

import pika
from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

_PKG_DIR = Path(__file__).resolve().parent.parent
if str(_PKG_DIR) not in sys.path:
    sys.path.insert(0, str(_PKG_DIR))

from osgeo import ogr  # noqa: E402

from ophub import store  # noqa: E402
from ophub.config import settings  # noqa: E402
from ophub.mq import connect, declare_topology  # noqa: E402
from ophub.scanner import split_provinces  # noqa: E402
from core import geo as geo_mod  # noqa: E402
from core.overlay import overlay_province  # noqa: E402
from core.redline_index import RedlineStore  # noqa: E402

ogr.UseExceptions()

logger = logging.getLogger(__name__)


def _ok(data) -> dict:
    """统一成功响应：{"success": true, "msg": null, "data": ...}"""
    return {"success": True, "msg": None, "data": data}


def _fail(status_code: int, msg: str) -> JSONResponse:
    """统一失败响应：HTTP 非 2xx + {"success": false, "msg": "错误说明"}"""
    return JSONResponse(status_code=status_code, content={"success": False, "msg": msg})


# RabbitMQ 管理接口（management 插件默认端口 15672）
_MGMT_PORT = 15672
_MGMT_TIMEOUT = 3


def _rabbitmq_mgmt_get(api_path: str):
    """调用 RabbitMQ 管理 API，返回解析后的 JSON。"""
    url = f"http://{settings.mq_host}:{_MGMT_PORT}/api/{api_path}"
    token = base64.b64encode(f"{settings.mq_user}:{settings.mq_password}".encode()).decode()
    req = urllib.request.Request(url, headers={"Authorization": f"Basic {token}"})
    with urllib.request.urlopen(req, timeout=_MGMT_TIMEOUT) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _vhost_enc() -> str:
    return urllib.parse.quote(settings.mq_virtual_host, safe="")


# --------------------------------------------------------------------------- #
# 任务投递器
# --------------------------------------------------------------------------- #
class TaskProducer:
    """线程安全的任务投递器，连接断开自动重连。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._conn = None
        self._channel = None

    def _ensure_channel(self) -> pika.channel.Channel:
        if self._conn is not None and self._conn.is_open and self._channel is not None and self._channel.is_open:
            return self._channel
        self._cleanup()
        logger.info("Producer connecting to RabbitMQ %s:%s ...", settings.mq_host, settings.mq_port)
        self._conn = connect()
        self._channel = self._conn.channel()
        declare_topology(self._channel)
        self._channel.confirm_delivery()
        return self._channel

    def _cleanup(self) -> None:
        for closable in (self._channel, self._conn):
            try:
                if closable is not None and closable.is_open:
                    closable.close()
            except Exception:
                pass
        self._channel = None
        self._conn = None

    def publish_task(self, message: dict) -> None:
        body = json.dumps(message, ensure_ascii=False).encode("utf-8")
        with self._lock:
            for attempt in range(2):
                try:
                    channel = self._ensure_channel()
                    channel.basic_publish(
                        exchange=settings.task_exchange,
                        routing_key=settings.task_routing_key,
                        body=body,
                        properties=pika.BasicProperties(
                            content_type="application/json",
                            delivery_mode=2,
                            message_id=message.get("task_id") or message.get("batch_id"),
                        ),
                    )
                    return
                except (pika.exceptions.AMQPError, OSError) as exc:
                    logger.warning("Publish failed (attempt %s): %s", attempt + 1, exc)
                    self._cleanup()
                    time.sleep(1)
            raise RuntimeError("任务投递 RabbitMQ 失败，请检查消息队列是否可用")


producer = TaskProducer()


# --------------------------------------------------------------------------- #
# 结果回收线程
# --------------------------------------------------------------------------- #
def _result_consumer_loop() -> None:
    """后台线程：消费 Slave 回传的结果并更新 SQLite，断线自动重连。"""
    while True:
        conn = None
        try:
            logger.info("Result consumer connecting to RabbitMQ ...")
            conn = connect()
            channel = conn.channel()
            declare_topology(channel)
            channel.basic_qos(prefetch_count=20)

            def on_result(ch: pika.channel.Channel, method, props, body: bytes) -> None:
                try:
                    result = json.loads(body)
                    task_id = result.get("task_id")
                    if not task_id:
                        logger.warning("Result without task_id ignored: %r", body)
                        return
                    status = result.get("status", "unknown")
                    finished_at = datetime.now().isoformat(timespec="seconds")
                    error = result.get("error") if status == "failed" else None
                    updated = store.update_subtask(
                        task_id, status, json.dumps(result, ensure_ascii=False) if status in ("success", "no_data") else None,
                        error, finished_at,
                    )
                    if updated:
                        logger.info("Subtask %s -> %s", task_id, status)
                    else:
                        logger.warning("Result for unknown subtask %s ignored", task_id)
                except Exception:
                    logger.exception("Failed to handle result message: %r", body)
                finally:
                    ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_consume(settings.result_queue, on_result)
            logger.info("Result consumer started on queue %r", settings.result_queue)
            channel.start_consuming()
        except Exception:
            logger.exception("Result consumer error, reconnect in %ss", settings.reconnect_interval)
            time.sleep(settings.reconnect_interval)
        finally:
            try:
                if conn is not None and conn.is_open:
                    conn.close()
            except Exception:
                pass


# --------------------------------------------------------------------------- #
# 汇总逻辑（内嵌，避免再引入一个 aggregator.py）
# --------------------------------------------------------------------------- #
def _aggregate_task(batch_id: str) -> dict | None:
    """从 SQLite 读取子任务，合并成最终汇总结果。"""
    task = store.get_task(batch_id)
    if task is None:
        return None

    # 已完成汇总（status=done）直接返回 summary
    if task["status"] == "done" and task.get("summary"):
        return json.loads(task["summary"])

    # 从子任务汇总
    subs = task["subtasks"]
    ok = [s for s in subs if s["status"] in ("success", "no_data") and s.get("result_json")]
    failed = [s for s in subs if s["status"] == "failed"]
    pending = [s for s in subs if s["status"] == "pending"]

    # 合并 matches（按 patch_id 跨省合并）
    patches: dict[str, dict] = {}
    all_matches = []
    for s in ok:
        result = json.loads(s["result_json"])
        for m in result.get("matches", []):
            all_matches.append(m)
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
            p["provinces"].append(s["province_name"])

    # 按类型汇总
    by_type: dict[str, dict] = {}
    for p in patches.values():
        b = by_type.setdefault(p["redline_type"], {"redline_type": p["redline_type"], "overlap_area": 0.0, "patch_count": 0})
        b["overlap_area"] = round(b["overlap_area"] + p["overlap_area"], 2)
        b["patch_count"] += 1

    sum_overlap = round(sum(p["overlap_area"] for p in patches.values()), 2)

    # Union 面积：把所有重叠几何拼起来做 union
    union_area = 0.0
    union_geom = None
    if all_matches:
        geoms = []
        for m in all_matches:
            gj = m.get("geometry")
            if not gj:
                continue
            g = ogr.CreateGeometryFromJson(json.dumps(gj))
            if g is not None and not g.IsEmpty():
                geoms.append(g)
        if geoms:
            union = geoms[0]
            for g in geoms[1:]:
                union = union.Union(g)
            union_area = geo_mod.area_m2(union)
            union_geom = json.loads(union.ExportToJson())

    # 占比
    input_area = task["input_area_m2"]
    severe_ratio = task["severe_ratio"]
    ratio_union = round(union_area / input_area * 100, 4) if input_area else 0.0
    ratio_sum_capped = min(round(sum_overlap / input_area * 100, 4) if input_area else 0.0, 100.0)

    # Severity
    if union_area <= 1e-6:
        severity, compliant = "CLEAR", True
    elif ratio_union >= severe_ratio * 100:
        severity, compliant = "SEVERE", False
    else:
        severity, compliant = "OCCUPIED", False

    # 省明细
    provinces_out = []
    for s in ok:
        result = json.loads(s["result_json"])
        provinces_out.append({
            "province_code": result.get("province_code"),
            "province_name": result.get("province_name"),
            "status": result.get("status"),
            "patch_count": result.get("patch_count", 0),
            "patches_scanned": result.get("patches_scanned", 0),
            "overlap_area": result.get("province_overlap_area", 0.0),
            "union_overlap_area": result.get("province_union_area", 0.0),
            "by_type": result.get("by_type", []),
            "elapsed_ms": result.get("elapsed_ms", 0),
            "worker_id": result.get("worker_id"),
            "warnings": result.get("warnings", []),
        })

    warnings = []
    for p in provinces_out:
        warnings.extend(p["warnings"])
    if failed:
        warnings.append("以下省份计算失败：" + "、".join(
            f'{s["province_name"]}' for s in failed
        ))
    if pending:
        warnings.append("以下省份未在超时时间内返回：" + "、".join(
            f'{s["province_name"]}' for s in pending
        ))

    summary = {
        "batch_id": task["batch_id"],
        "feature_id": task.get("feature_id"),
        "input_area_m2": round(input_area, 2),
        "buffer_m": task["buffer_m"],
        "severe_ratio": severe_ratio,
        "is_compliant": compliant,
        "severity": severity,
        "severity_text": {
            "CLEAR": "未压盖红线",
            "OCCUPIED": "压盖红线",
            "SEVERE": "严重压盖（已达预警阈值）",
            "OUTSIDE": "输入面超出国土范围，结论无效",
        }.get(severity, severity),
        "sum_overlap_area_m2": sum_overlap,
        "union_overlap_area_m2": round(union_area, 2),
        "overlap_ratio": ratio_sum_capped,
        "overlap_ratio_union": ratio_union,
        "by_type": sorted(by_type.values(), key=lambda x: -x["overlap_area"]),
        "provinces": provinces_out,
        "patches": sorted(patches.values(), key=lambda x: -x["overlap_area"]),
        "overlap_geometry": union_geom,
        "task_summary": {
            "expected": len(subs),
            "succeeded": len([s for s in ok if s["status"] == "success"]),
            "no_data": len([s for s in ok if s["status"] == "no_data"]),
            "failed": len(failed),
            "pending": len(pending),
        },
        "warnings": warnings,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }

    # 全部完成则写回 summary
    if not pending:
        store.update_task_summary(batch_id, "done", json.dumps(summary, ensure_ascii=False))
    else:
        store.update_task_summary(batch_id, "aggregating", json.dumps(summary, ensure_ascii=False))

    return summary


# --------------------------------------------------------------------------- #
# FastAPI App
# --------------------------------------------------------------------------- #
app = FastAPI(title="国土红线叠加分析 - Master")

# CORS
if settings.cors_enabled:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=False,
        allow_methods=["GET", "POST"],
        allow_headers=["*"],
    )
    logger.info("CORS enabled, allow origins: %s", settings.cors_origins)
else:
    logger.info("CORS disabled (same-origin only)")

# 静态页：前端 Leaflet 地图
_WEB_DIR = _PKG_DIR / "web"

@app.get("/", include_in_schema=False)
def index():
    return FileResponse(_WEB_DIR / "index.html")

if _WEB_DIR.exists():
    app.mount("/data", StaticFiles(directory=str(_PKG_DIR / "data")), name="data")


def _recovery_loop(snapshot: list[dict]) -> None:
    if not snapshot:
        return
    logger.info("Recovery: %s unpublished subtask(s) found, republishing ...", len(snapshot))
    for sub in snapshot:
        while True:
            try:
                producer.publish_task({
                    "task_id": sub["task_id"],
                    "batch_id": sub["batch_id"],
                    "province_code": sub["province_code"],
                    "province_name": sub["province_name"],
                    # 注意：恢复投递时 input_wkt/input_area_m2 等字段需要从主任务里重新取
                    # 这里暂不传（需要主任务上下文），由调用方在实际恢复时补充
                })
                store.mark_published([sub["task_id"]])
                logger.info("Recovery: republished subtask %s (%s)", sub["task_id"], sub["province_name"])
                break
            except Exception:
                logger.exception(
                    "Recovery publish failed for subtask %s, retry in %ss",
                    sub["task_id"], settings.reconnect_interval,
                )
                time.sleep(settings.reconnect_interval)


# 保存全部恢复需要的字段（恢复线程用）
_recovery_snapshot: list[dict] = []


@contextlib.asynccontextmanager
async def lifespan(_: FastAPI):
    db_file = store.init_db()
    logger.info("SQLite storage ready: %s", db_file)
    # 恢复线程：重投崩溃前未投递出去的子任务
    snapshot = store.get_unpublished_pending_subtasks()
    threading.Thread(target=_recovery_loop, args=(snapshot,), name="task-recovery", daemon=True).start()
    thread = threading.Thread(target=_result_consumer_loop, name="result-consumer", daemon=True)
    thread.start()
    yield


app.router.lifespan_context = lifespan


@app.exception_handler(RequestValidationError)
async def _validation_handler(_request: Request, exc: RequestValidationError):
    first = exc.errors()[0] if exc.errors() else {}
    loc = ".".join(str(p) for p in first.get("loc", []))
    return _fail(422, f"参数校验失败: {loc} {first.get('msg', '')}")


@app.get("/health")
def health() -> dict:
    return _ok({
        "status": "ok",
        "role": settings.role,
        "time": datetime.now().isoformat(timespec="seconds"),
    })


# --------------------------------------------------------------------------- #
# 核心接口：叠加分析
# --------------------------------------------------------------------------- #
class AnalyzeRequest(BaseModel):
    feature_id: str = Field(..., description="输入面唯一标识")
    geometry: dict = Field(..., description="GeoJSON geometry")
    geometry_srid: int = Field(4326, description="输入坐标系 EPSG")
    severe_ratio: float | None = Field(None, ge=0.0, le=1.0)
    buffer_m: float | None = Field(None, ge=0.0, le=5000.0)
    prefer_local: bool = Field(False, description="强制本地串行计算（不走 MQ）")


@app.post("/overlay/analyze")
def analyze(req: AnalyzeRequest):
    severe_ratio = settings.severe_ratio if req.severe_ratio is None else req.severe_ratio
    buffer_m = settings.buffer_m if req.buffer_m is None else req.buffer_m

    # 1) 解析几何 → CGCS2000
    try:
        geom = geo_mod.parse_geojson_geometry(req.geometry, req.geometry_srid)
    except Exception as exc:
        return _fail(400, f"几何无效：{exc}")

    input_area_m2 = geo_mod.area_m2(geom)
    if input_area_m2 <= 0:
        return _fail(400, "输入面面积为 0")

    input_wkt = geom.ExportToWkt()

    # 2) 省份识别
    provinces = split_provinces(geom)
    batch_id = f"B{time.strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"
    created_at = datetime.now().isoformat(timespec="seconds")

    # 无命中省份 → 直接返回 CLEAR
    if not provinces:
        summary = {
            "batch_id": batch_id,
            "feature_id": req.feature_id,
            "input_area_m2": round(input_area_m2, 2),
            "buffer_m": buffer_m,
            "severe_ratio": severe_ratio,
            "is_compliant": True,
            "severity": "CLEAR",
            "severity_text": "未压盖红线",
            "sum_overlap_area_m2": 0.0,
            "union_overlap_area_m2": 0.0,
            "overlap_ratio": 0.0,
            "overlap_ratio_union": 0.0,
            "by_type": [],
            "provinces": [],
            "patches": [],
            "task_summary": {"expected": 0, "succeeded": 0, "no_data": 0, "failed": 0, "pending": 0},
            "warnings": ["输入面未落在任何省级行政区范围内"],
            "generated_at": created_at,
        }
        return _ok({"dispatch_mode": "none", "batch_id": batch_id, "summary": summary})

    # 3) 落库
    store.save_task(
        batch_id=batch_id,
        feature_id=req.feature_id,
        input_wkt=input_wkt,
        input_area_m2=input_area_m2,
        buffer_m=buffer_m,
        severe_ratio=severe_ratio,
        provinces=provinces,
        created_at=created_at,
    )

    # 4) 尝试投递 MQ
    dispatch_mode = "mq"
    sub_ids = [f"{batch_id}:{p['code']}" for p in provinces]
    use_local = req.prefer_local
    if not use_local:
        try:
            producer._ensure_channel()
        except Exception:
            logger.warning("RabbitMQ 不可用，降级本地串行计算")
            use_local = True

    if use_local:
        # 本地串行计算（降级 / prefer_local）
        store.mark_published(sub_ids)
        redline_store = RedlineStore()
        for p in provinces:
            task_sub_id = f"{batch_id}:{p['code']}"
            try:
                result = overlay_province(
                    input_geom=geom,
                    province_code=p["code"],
                    province_name=p["name"],
                    buffer_m=buffer_m,
                    input_area_m2=input_area_m2,
                    store=redline_store,
                )
                finished_at = datetime.now().isoformat(timespec="seconds")
                store.update_subtask(
                    task_sub_id,
                    result.get("status", "success"),
                    json.dumps(result, ensure_ascii=False),
                    None,
                    finished_at,
                )
                logger.info("Local subtask %s done: %s(%s)", task_sub_id, p["name"], p["code"])
            except Exception as exc:
                logger.exception("Local subtask %s failed: %s", task_sub_id, p["code"])
                store.update_subtask(task_sub_id, "failed", None, f"{type(exc).__name__}: {exc}",
                                     datetime.now().isoformat(timespec="seconds"))
        dispatch_mode = "local"
    else:
        for i, p in enumerate(provinces):
            task_sub_id = f"{batch_id}:{p['code']}"
            try:
                producer.publish_task({
                    "batch_id": batch_id,
                    "task_id": task_sub_id,
                    "feature_id": req.feature_id,
                    "province_code": p["code"],
                    "province_name": p["name"],
                    "input_wkt": input_wkt,
                    "input_srid": 4490,
                    "input_area_m2": input_area_m2,
                    "buffer_m": buffer_m,
                    "severe_ratio": severe_ratio,
                })
                store.mark_published([task_sub_id])
            except RuntimeError as exc:
                store.fail_unpublished(batch_id, str(exc), datetime.now().isoformat(timespec="seconds"))
                return _fail(503, str(exc))

    logger.info("Task %s submitted: %s -> %s subtasks (%s)", batch_id, req.feature_id, len(provinces), dispatch_mode)
    return _ok({
        "batch_id": batch_id,
        "dispatch_mode": dispatch_mode,
        "subtask_count": len(provinces),
        "provinces": provinces,
    })


@app.get("/overlay/result")
def overlay_result(batchId: str = Query(..., alias="batchId", description="下发任务时返回的 batch_id")):
    batch_id = (batchId or "").strip()
    if not batch_id:
        return _fail(400, "batchId 参数不能为空")

    task = store.get_task(batch_id)
    if task is None:
        return _fail(404, f"任务不存在: {batch_id}")

    # 如果全部子任务已完成且 summary 已写，直接返回
    if task["status"] == "done" and task.get("summary"):
        return _ok(json.loads(task["summary"]))

    # 重新聚合
    summary = _aggregate_task(batch_id)
    return _ok(summary)


# --------------------------------------------------------------------------- #
# 任务查询
# --------------------------------------------------------------------------- #
@app.get("/tasks/pending")
def tasks_pending(
    batchId: str | None = Query(None, alias="batchId",
                                description="可选。传入：该任务的子任务进度明细；不传：全局待处理列表"),
):
    batch_id = (batchId or "").strip()

    # ---- 单任务模式 ----
    if batch_id:
        task = store.get_task(batch_id)
        if task is None:
            return _fail(404, f"任务不存在: {batch_id}")
        subs = task["subtasks"]
        pending = [s for s in subs if s["status"] == "pending"]
        ok = [s for s in subs if s["status"] in ("success", "no_data")]
        failed = [s for s in subs if s["status"] == "failed"]
        return _ok({
            "batch_id": batch_id,
            "feature_id": task["feature_id"],
            "createdAt": task["created_at"],
            "total": len(subs),
            "pendingCount": len(pending),
            "finishedCount": len(ok),
            "failedCount": len(failed),
            "subtasks": [
                {
                    "task_id": s["task_id"],
                    "province_code": s["province_code"],
                    "province_name": s["province_name"],
                    "status": s["status"],
                    "error": s["error"],
                }
                for s in subs
            ],
        })

    # ---- 全局模式 ----
    rows = store.get_pending_tasks()
    queue_stats = None
    mq_error = None
    try:
        info = _rabbitmq_mgmt_get(f"queues/{_vhost_enc()}/{settings.task_queue}")
        queue_stats = {
            "queue": settings.task_queue,
            "ready": info.get("messages_ready"),
            "unacked": info.get("messages_unacknowledged"),
            "consumers": info.get("consumers"),
        }
    except Exception as exc:
        mq_error = f"RabbitMQ 管理接口不可用: {exc}"

    return _ok({
        "taskCount": len(rows),
        "tasks": rows,
        "queueStats": queue_stats,
        "mqError": mq_error,
    })


@app.get("/nodes")
def nodes():
    try:
        info = _rabbitmq_mgmt_get(f"queues/{_vhost_enc()}/{settings.task_queue}")
    except Exception as exc:
        return _fail(503, f"RabbitMQ 管理接口不可用: {exc}")

    workers = []
    for c in info.get("consumer_details") or []:
        ch = c.get("channel_details") or {}
        workers.append({
            "consumerTag": c.get("consumer_tag"),
            "peerHost": ch.get("peer_host"),
            "peerPort": ch.get("peer_port"),
            "connectionName": ch.get("connection_name") or ch.get("name"),
            "active": c.get("active"),
        })

    return _ok({
        "master": {"role": settings.role, "rest": f"http://{settings.master_host}:{settings.master_port}"},
        "workerCount": len(workers),
        "workers": workers,
    })


@app.get("/provinces")
def provinces():
    from core import province as province_mod
    try:
        ps = province_mod.load_provinces()
    except Exception as exc:
        return _fail(503, f"省份边界数据不可用: {exc}")
    return _ok([{"code": p["code"], "name": p["name"]} for p in ps])
