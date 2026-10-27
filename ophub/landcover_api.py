"""1993 年 CLCD 土地覆盖统计与成果下载接口。"""
from __future__ import annotations

import json
import re
import contextlib
import logging
import threading
import uuid
from datetime import datetime
from functools import lru_cache
from pathlib import Path

from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse
from osgeo import gdal, ogr
from pydantic import BaseModel, Field

from core.distributed_landcover import create_plan
from ophub.config import BASE_DIR, settings
from ophub import landcover_jobs

gdal.UseExceptions()
ogr.UseExceptions()
logger = logging.getLogger(__name__)

@contextlib.asynccontextmanager
async def lifespan(_app: FastAPI):
    landcover_jobs.init_db()
    if settings.role in ("all", "master"):
        landcover_jobs.fail_interrupted_planning()
        threading.Thread(target=landcover_jobs.result_consumer, name="clcd-results", daemon=True).start()
        threading.Thread(target=landcover_jobs.recover_tasks, name="clcd-recovery", daemon=True).start()
    if settings.role == "all":
        threading.Thread(target=landcover_jobs.run_slave, name="clcd-worker", daemon=True).start()
    yield


app = FastAPI(title="CLCD 土地覆盖分析", lifespan=lifespan)
WEB_DIR = Path(BASE_DIR) / "web"
ADMIN_FILES = {"province": "中国_省2.shp", "city": "中国_市2.shp", "county": "中国_县2.shp"}
TASK_ID_RE = re.compile(r"^[0-9a-f]{32}$")
REGION_CODE_RE = re.compile(r"^\d{9}$")


def _ok(data):
    return {"success": True, "msg": None, "data": data}


def _fail(code: int, message: str):
    return JSONResponse(status_code=code, content={"success": False, "msg": message})


def _output_root() -> Path:
    path = Path(settings.output_dir)
    return path if path.is_absolute() else Path(BASE_DIR) / path


@lru_cache(maxsize=1)
def _classes() -> dict:
    info = json.loads((Path(BASE_DIR) / "clcd_classes.json").read_text(encoding="utf-8"))
    codes = [int(item["code"]) for item in info["classes"]]
    if len(codes) != len(set(codes)) or int(info["nodata"]) in codes:
        raise ValueError("CLCD 分类配置有重复编码或包含 NoData")
    return info


def _admin_layer(level: str):
    if level not in ADMIN_FILES:
        raise ValueError("level 只支持 province、city、county")
    path = Path(settings.admin_data_dir) / ADMIN_FILES[level]
    if not path.is_file():
        raise ValueError(f"缺少行政区划文件：{path}")
    # 原始 Shapefile 没有 .cpg，DBF 名称字段实际采用 UTF-8。
    gdal.SetConfigOption("SHAPE_ENCODING", "UTF-8")
    ds = ogr.Open(str(path), 0)
    if ds is None:
        raise ValueError(f"无法读取行政区划文件：{path}")
    return ds, ds.GetLayer(0)


def _region_geometry(level: str, code: str) -> dict | None:
    if not REGION_CODE_RE.fullmatch(code):
        raise ValueError("行政区代码必须是 9 位数字")
    ds, layer = _admin_layer(level)
    layer.SetAttributeFilter(f"gb = '{code}'")
    feature = layer.GetNextFeature()
    if feature is None or feature.GetGeometryRef() is None:
        return None
    geom = feature.GetGeometryRef().Clone()
    if not geom.IsValid():
        # 基础省界中有一处拓扑无效，修复副本后再用于预览与统计，不改动源数据。
        geom = geom.MakeValid()
        if geom is None or geom.IsEmpty():
            raise ValueError(f"行政区 {code} 边界无法修复")
    result = {
        "code": code,
        "name": feature.GetField("name"),
        "geometry": json.loads(geom.ExportToJson()),
    }
    feature = None
    ds = None
    return result


class AnalyzeRequest(BaseModel):
    geometry: dict | None = None
    geometry_srid: int = Field(4326, gt=0)
    region_level: str | None = None
    region_code: str | None = None


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(WEB_DIR / "index.html")


@app.get("/health")
def health():
    raster = Path(settings.clcd_raster)
    admin = Path(settings.admin_data_dir)
    return _ok({
        "raster_ready": raster.is_file(),
        "admin_ready": all((admin / name).is_file() for name in ADMIN_FILES.values()),
        "year": _classes()["year"],
        "max_export_pixels": settings.max_export_pixels,
        "split_level": settings.split_level,
        "role": settings.role,
        "output_dir": str(_output_root()),
    })


@app.get("/landcover/regions")
def regions(level: str = Query(...), parent_code: str | None = Query(None)):
    try:
        ds, layer = _admin_layer(level)
        if parent_code and not REGION_CODE_RE.fullmatch(parent_code):
            return _fail(400, "上级行政区代码必须是 9 位数字")
        # 行政代码前 5 位表示省、前 7 位表示市；仅用于列表筛选，计算仍使用原始边界几何。
        prefix = parent_code[:5] if level == "city" and parent_code else parent_code[:7] if level == "county" and parent_code else ""
        data = []
        for feature in layer:
            code = str(feature.GetField("gb") or "")
            if prefix and not code.startswith(prefix):
                continue
            data.append({"code": code, "name": feature.GetField("name")})
        ds = None
        return _ok(sorted(data, key=lambda item: item["name"]))
    except ValueError as exc:
        return _fail(400, str(exc))


@app.get("/landcover/region-geometry")
def region_geometry(level: str = Query(...), code: str = Query(...)):
    try:
        result = _region_geometry(level, code)
    except ValueError as exc:
        return _fail(400, str(exc))
    return _ok(result) if result else _fail(404, "行政区不存在")


@app.post("/landcover/analyze")
def analyze(request: AnalyzeRequest):
    if request.geometry is not None and (request.region_level or request.region_code):
        return _fail(400, "手绘面与行政区选择只能传一种")
    if request.geometry is None:
        if not request.region_level or not request.region_code:
            return _fail(400, "请传入 geometry 或行政区级别与代码")
        if request.region_level not in ADMIN_FILES or not REGION_CODE_RE.fullmatch(request.region_code):
            return _fail(400, "行政区级别或代码无效")

    task_id = uuid.uuid4().hex
    split_level = settings.split_level
    landcover_jobs.create_task(task_id, split_level)
    # 先持久化任务 ID，再把行政区读取和栅格分块放入后台，避免大范围请求阻塞 HTTP 响应。
    try:
        threading.Thread(target=_plan_and_dispatch, args=(task_id, split_level, request),
                         name=f"clcd-plan-{task_id[:8]}", daemon=True).start()
    except Exception as exc:
        logger.exception("CLCD task %s planning thread failed to start", task_id)
        landcover_jobs._fail_task(task_id, f"任务规划线程启动失败：{exc}")
        return _fail(500, "任务规划线程启动失败")
    return JSONResponse(status_code=202, content=_ok({"task_id": task_id}))


def _plan_and_dispatch(task_id: str, split_level: str, request: AnalyzeRequest) -> None:
    try:
        logger.info("CLCD task %s planning started, split=%s", task_id, split_level)
        geometry = request.geometry
        srid = request.geometry_srid
        if geometry is None:
            region = _region_geometry(request.region_level, request.region_code)
            if region is None:
                raise ValueError("行政区不存在")
            geometry = region["geometry"]
            srid = 4326
        info = _classes()
        plan = create_plan(
            geometry=geometry,
            srid=srid,
            raster_path=Path(settings.clcd_raster),
            admin_dir=Path(settings.admin_data_dir),
            split_level=split_level,
            output_root=_output_root(),
            classes={int(item["code"]): item["name"] for item in info["classes"]},
            nodata=int(info["nodata"]),
            year=int(info["year"]),
            max_pixels=settings.max_export_pixels,
            task_id=task_id,
        )
        landcover_jobs.save_plan(plan)
        logger.info("CLCD task %s planning completed, subtasks=%s", task_id, len(plan["regions"]))
        landcover_jobs.dispatch_task(task_id)
    except Exception as exc:
        logger.exception("CLCD task %s planning or dispatch failed", task_id)
        landcover_jobs._fail_task(task_id, str(exc))


@app.get("/landcover/result/{task_id}")
def result(task_id: str):
    if not TASK_ID_RE.fullmatch(task_id):
        return _fail(400, "任务 ID 格式无效")
    state = landcover_jobs.task_state(task_id)
    if state is None:
        # 兼容改造前同步计算生成、没有 SQLite 记录的历史成果。
        old_result = _output_root() / task_id / "result.json"
        if old_result.is_file():
            return _ok({"task_id": task_id, "status": "done", "total": 0, "finished": 0,
                        "created_at": None, "finished_at": datetime.fromtimestamp(old_result.stat().st_mtime).isoformat(timespec="seconds"),
                        "finished_at_estimated": True, "duration_seconds": None,
                        "subtasks": [], "result": json.loads(old_result.read_text(encoding="utf-8"))})
        return _fail(404, "结果不存在")
    if state["status"] == "done":
        path = _output_root() / task_id / "result.json"
        if not path.is_file():
            return _fail(500, "任务已完成但成果记录缺失")
        state["result"] = json.loads(path.read_text(encoding="utf-8"))
    return _ok(state)


@app.post("/landcover/retry/{task_id}")
def retry(task_id: str):
    if not TASK_ID_RE.fullmatch(task_id):
        return _fail(400, "任务 ID 格式无效")
    try:
        state = landcover_jobs.retry_failed_task(task_id)
    except FileNotFoundError as exc:
        return _fail(404, str(exc))
    except ValueError as exc:
        return _fail(400, str(exc))
    if state["status"] == "pending":
        threading.Thread(target=landcover_jobs.dispatch_task, args=(task_id,), daemon=True).start()
    return _ok(state)


@app.get("/landcover/download/{task_id}/{kind}")
def download(task_id: str, kind: str):
    if not TASK_ID_RE.fullmatch(task_id):
        return _fail(400, "任务 ID 格式无效")
    if kind not in ("raster", "vector"):
        return _fail(400, "下载类型只支持 raster 或 vector")
    state = landcover_jobs.task_state(task_id)
    if state is not None and state["status"] != "done":
        return _fail(409, "任务尚未完成，暂不能下载成果")
    year = int(_classes()["year"])
    filename = f"CLCD_{year}_clip.tif" if kind == "raster" else f"CLCD_{year}_classes.gpkg"
    path = _output_root() / task_id / filename
    return FileResponse(path, filename=filename) if path.is_file() else _fail(404, "成果文件不存在")
