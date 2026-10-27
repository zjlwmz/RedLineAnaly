"""CLCD 专用 RabbitMQ 任务流与 SQLite 状态；与旧红线队列隔离。"""
from __future__ import annotations

import json
import logging
import os
import socket
import sqlite3
import threading
import time
import contextlib
from datetime import datetime
from pathlib import Path

import pika

from core.distributed_landcover import finish_task, run_chunk
from ophub.config import BASE_DIR, settings
from ophub.mq import connect

logger = logging.getLogger(__name__)
_lock = threading.RLock()
_worker_id = f"landcover-{socket.gethostname()}-{os.getpid()}"


def output_root() -> Path:
    path = Path(settings.output_dir)
    return path if path.is_absolute() else Path(BASE_DIR) / path


@contextlib.contextmanager
def _db():
    path = Path(settings.db_path)
    if not path.is_absolute():
        path = Path(BASE_DIR) / path
    db = sqlite3.connect(path, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA foreign_keys=ON")
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def init_db() -> None:
    with _lock, _db() as db:
        db.executescript("""
        CREATE TABLE IF NOT EXISTS landcover_tasks (
            task_id TEXT PRIMARY KEY, status TEXT NOT NULL, split_level TEXT NOT NULL,
            created_at TEXT NOT NULL, error TEXT, finished_at TEXT
        );
        CREATE TABLE IF NOT EXISTS landcover_subtasks (
            task_id TEXT NOT NULL REFERENCES landcover_tasks(task_id),
            label INTEGER NOT NULL, code TEXT NOT NULL, name TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending', published INTEGER NOT NULL DEFAULT 0,
            error TEXT, worker_id TEXT, PRIMARY KEY(task_id, label)
        );
        """)
        if "finished_at" not in {row["name"] for row in db.execute("PRAGMA table_info(landcover_tasks)")}:
            # 旧数据库保留已有任务记录，启动时只补充完成时间字段。
            db.execute("ALTER TABLE landcover_tasks ADD COLUMN finished_at TEXT")


def fail_interrupted_planning() -> None:
    """Master 启动前遗留的规划线程已消失，标记失败供用户重新提交。"""
    with _lock, _db() as db:
        db.execute("UPDATE landcover_tasks SET status='failed',error=?,finished_at=? WHERE status='planning'",
                   ("Master 在任务规划期间重启，请重新提交分析", datetime.now().isoformat(timespec="seconds")))


def create_task(task_id: str, split_level: str) -> None:
    with _lock, _db() as db:
        db.execute("INSERT INTO landcover_tasks(task_id,status,split_level,created_at,error,finished_at) "
                   "VALUES (?, 'planning', ?, ?, NULL, NULL)",
                   (task_id, split_level, datetime.now().isoformat(timespec="seconds")))


def save_plan(plan: dict) -> None:
    with _lock, _db() as db:
        task = db.execute("SELECT status,split_level FROM landcover_tasks WHERE task_id=?",
                          (plan["task_id"],)).fetchone()
        if task is None:
            # 兼容直接生成计划并保存的调用方。
            db.execute("INSERT INTO landcover_tasks(task_id,status,split_level,created_at,error,finished_at) "
                       "VALUES (?, 'pending', ?, ?, NULL, NULL)",
                       (plan["task_id"], plan["split_level"], datetime.now().isoformat(timespec="seconds")))
        elif task["status"] == "planning" and task["split_level"] == plan["split_level"]:
            # 计划和子任务在同一事务写入，轮询不会看到 pending 却没有子任务。
            db.execute("UPDATE landcover_tasks SET status='pending' WHERE task_id=?", (plan["task_id"],))
        else:
            raise ValueError("任务状态或拆分级别与计划不一致")
        db.executemany("INSERT INTO landcover_subtasks(task_id,label,code,name) VALUES (?,?,?,?)",
                       [(plan["task_id"], r["label"], r["code"], r["name"]) for r in plan["regions"]])


def task_state(task_id: str) -> dict | None:
    with _lock, _db() as db:
        task = db.execute("SELECT * FROM landcover_tasks WHERE task_id=?", (task_id,)).fetchone()
        if task is None:
            return None
        subtasks = db.execute("SELECT label,code,name,status,error,worker_id FROM landcover_subtasks WHERE task_id=? ORDER BY label",
                              (task_id,)).fetchall()
    rows = [dict(row) for row in subtasks]
    task_data = dict(task)
    task_data["finished_at_estimated"] = False
    if task_data["status"] == "done" and not task_data.get("finished_at"):
        result_path = output_root() / task_id / "result.json"
        if result_path.is_file():
            # 历史任务没有数据库完成时间；成果 JSON 在合并结束时写出，只能作为近似时间。
            task_data["finished_at"] = datetime.fromtimestamp(result_path.stat().st_mtime).isoformat(timespec="seconds")
            task_data["finished_at_estimated"] = True
    task_data["duration_seconds"] = None
    if task_data.get("finished_at"):
        elapsed = datetime.fromisoformat(task_data["finished_at"]) - datetime.fromisoformat(task_data["created_at"])
        task_data["duration_seconds"] = max(0, int(elapsed.total_seconds()))
    return {**task_data, "total": len(rows), "finished": sum(r["status"] == "success" for r in rows),
            "subtasks": rows}


def retry_failed_task(task_id: str) -> dict:
    """复用已有分块计划，只重投未成功子任务；消息可能已被旧节点确认，故一律重新投递。"""
    if not (output_root() / task_id / "plan.json").is_file():
        raise FileNotFoundError("原任务计划文件不存在，无法复用任务")
    with _lock, _db() as db:
        task = db.execute("SELECT status FROM landcover_tasks WHERE task_id=?", (task_id,)).fetchone()
        if task is None:
            raise ValueError("任务不存在")
        if task["status"] != "failed":
            raise ValueError("只有失败任务可以重试")
        total = db.execute("SELECT COUNT(*) FROM landcover_subtasks WHERE task_id=?", (task_id,)).fetchone()[0]
        if not total:
            # 规划失败时即使残留 plan.json，也没有可重投的子任务，必须重新提交。
            raise ValueError("任务尚未完成拆分，请重新提交分析")
        remaining = db.execute("SELECT COUNT(*) FROM landcover_subtasks WHERE task_id=? AND status!='success'",
                               (task_id,)).fetchone()[0]
        db.execute("UPDATE landcover_subtasks SET status='pending',published=0,error=NULL,worker_id=NULL "
                   "WHERE task_id=? AND status!='success'", (task_id,))
        db.execute("UPDATE landcover_tasks SET status=?,error=NULL,finished_at=NULL WHERE task_id=?",
                   ("pending" if remaining else "aggregating", task_id))
    if not remaining:
        _finalize(task_id)
    return task_state(task_id)


def _topology(channel) -> None:
    channel.exchange_declare(settings.landcover_dlx, exchange_type="fanout", durable=True)
    channel.queue_declare(settings.landcover_dlq, durable=True)
    channel.queue_bind(settings.landcover_dlq, settings.landcover_dlx)
    channel.queue_declare(settings.landcover_task_queue, durable=True,
                          arguments={"x-dead-letter-exchange": settings.landcover_dlx})
    channel.queue_declare(settings.landcover_result_queue, durable=True)


def _publish(channel, queue: str, message: dict, headers: dict | None = None) -> None:
    # 使用 RabbitMQ 默认交换机的独立持久化队列，旧红线 Slave 无法消费 CLCD 消息。
    ok = channel.basic_publish("", queue, json.dumps(message, ensure_ascii=False).encode("utf-8"),
                               pika.BasicProperties(delivery_mode=2, content_type="application/json",
                                                    message_id=f"{message['task_id']}:{message['label']}", headers=headers),
                               mandatory=True)
    if ok is False:
        raise RuntimeError("RabbitMQ 未确认消息投递")


def _mark_published(task_id: str, label: int) -> None:
    with _lock, _db() as db:
        db.execute("UPDATE landcover_subtasks SET published=1 WHERE task_id=? AND label=?", (task_id, label))


def _fail_task(task_id: str, error: str) -> None:
    with _lock, _db() as db:
        db.execute("UPDATE landcover_tasks SET status='failed',error=?,finished_at=? "
                   "WHERE task_id=? AND status NOT IN ('done','failed')",
                   (error, datetime.now().isoformat(timespec="seconds"), task_id))


def accept_result(result: dict) -> None:
    task_id, label = result["task_id"], int(result["label"])
    if result.get("status") == "success":
        plan_path = output_root() / task_id / "plan.json"
        if plan_path.is_file() and json.loads(plan_path.read_text(encoding="utf-8")).get("chunk_vectors"):
            # 旧 Slave 只会上报 TIFF；及时失败并提示重启，避免显示全部完成后才发现缺少矢量分块。
            if not (output_root() / task_id / f"chunk_{label}.gpkg").is_file():
                result = {**result, "status": "failed",
                          "error": "子节点未生成矢量分块，请更新并重启 Slave 后重试任务"}
    with _lock, _db() as db:
        task = db.execute("SELECT status FROM landcover_tasks WHERE task_id=?", (task_id,)).fetchone()
        sub = db.execute("SELECT status FROM landcover_subtasks WHERE task_id=? AND label=?", (task_id, label)).fetchone()
        if task is None or sub is None or task["status"] != "pending" or sub["status"] != "pending":
            return  # RabbitMQ 至少投递一次；重复结果不得覆盖已完成或失败状态。
        status = "success" if result.get("status") == "success" else "failed"
        db.execute("UPDATE landcover_subtasks SET status=?,error=?,worker_id=? WHERE task_id=? AND label=?",
                   (status, result.get("error"), result.get("worker_id"), task_id, label))
        logger.info("CLCD task %s subtask %s result=%s worker=%s", task_id, label, status,
                    result.get("worker_id", "unknown"))
        if status == "failed":
            db.execute("UPDATE landcover_tasks SET status='failed',error=?,finished_at=? WHERE task_id=?",
                       (result.get("error") or "子任务失败", datetime.now().isoformat(timespec="seconds"), task_id))
            return
        pending = db.execute("SELECT COUNT(*) FROM landcover_subtasks WHERE task_id=? AND status!='success'",
                             (task_id,)).fetchone()[0]
        if pending:
            return
        db.execute("UPDATE landcover_tasks SET status='aggregating' WHERE task_id=?", (task_id,))
    _finalize(task_id)


def _finalize(task_id: str) -> None:
    try:
        logger.info("CLCD task %s aggregation started", task_id)
        finish_task(output_root(), task_id)
    except Exception as exc:
        logger.exception("CLCD task %s aggregation failed", task_id)
        _fail_task(task_id, f"成果合并失败：{exc}")
    else:
        logger.info("CLCD task %s merge completed", task_id)
        with _lock, _db() as db:
            db.execute("UPDATE landcover_tasks SET status='done',error=NULL,finished_at=? "
                       "WHERE task_id=? AND status='aggregating'",
                       (datetime.now().isoformat(timespec="seconds"), task_id))


def _run_local(task_id: str, label: int) -> None:
    try:
        logger.info("Local CLCD Slave %s accepted task %s:%s", _worker_id, task_id, label)
        started_at = time.monotonic()
        result = run_chunk(output_root(), task_id, label)
        result["worker_id"] = _worker_id
        logger.info("Local CLCD Slave %s completed task %s:%s in %.1fs, pixels=%s, vectors=%s", _worker_id,
                    task_id, label, time.monotonic() - started_at, result["pixel_count"],
                    result.get("vector_feature_count", "legacy"))
    except Exception as exc:
        logger.exception("Local CLCD subtask %s:%s failed", task_id, label)
        result = {"task_id": task_id, "label": label, "status": "failed", "error": str(exc), "worker_id": _worker_id}
    accept_result(result)


def dispatch_task(task_id: str) -> None:
    """投递未投递子任务；all 角色在 MQ 不可用时改为后台本机计算。"""
    with _lock, _db() as db:
        rows = db.execute("SELECT label FROM landcover_subtasks WHERE task_id=? AND status='pending' AND published=0 ORDER BY label",
                          (task_id,)).fetchall()
    if not rows:
        return
    logger.info("CLCD task %s dispatching %s subtasks to %s", task_id, len(rows), settings.landcover_task_queue)
    try:
        conn = connect()
        try:
            channel = conn.channel()
            _topology(channel)
            channel.confirm_delivery()
            for row in rows:
                label = row["label"]
                # 任务携带 Master 实际使用的共享目录，避免节点启动时读取了旧的 output_dir。
                _publish(channel, settings.landcover_task_queue,
                         {"task_id": task_id, "label": label, "output_root": str(output_root())})
                _mark_published(task_id, label)
                logger.info("CLCD task %s subtask %s queued", task_id, label)
        finally:
            conn.close()
    except Exception as exc:
        logger.warning("CLCD task dispatch failed: %s", exc)
        if settings.role == "all":
            # 开发机没有 RabbitMQ 时仍可通过 Web 测试；只执行尚未确认成功的子任务。
            with _lock, _db() as db:
                pending = db.execute("SELECT label FROM landcover_subtasks WHERE task_id=? AND status='pending' AND published=0",
                                     (task_id,)).fetchall()
            for row in pending:
                _run_local(task_id, row["label"])
        else:
            _fail_task(task_id, f"RabbitMQ 投递失败：{exc}")


def recover_tasks() -> None:
    with _lock, _db() as db:
        aggregating = db.execute("SELECT task_id FROM landcover_tasks WHERE status='aggregating'").fetchall()
        pending = db.execute("SELECT task_id FROM landcover_tasks WHERE status='pending' AND EXISTS "
                             "(SELECT 1 FROM landcover_subtasks s WHERE s.task_id=landcover_tasks.task_id AND s.published=0)").fetchall()
    for row in aggregating:
        _finalize(row["task_id"])
    for row in pending:
        dispatch_task(row["task_id"])


def result_consumer() -> None:
    while True:
        conn = None
        try:
            conn = connect()
            channel = conn.channel()
            _topology(channel)
            channel.basic_qos(prefetch_count=1)

            def callback(ch, method, _properties, body):
                try:
                    result = json.loads(body)
                    if not isinstance(result, dict) or not isinstance(result.get("task_id"), str) or not isinstance(result.get("label"), int):
                        raise ValueError("结果消息缺少任务标识")
                except (ValueError, TypeError):
                    logger.error("Invalid CLCD result message, sent to reject: %r", body[:200])
                    ch.basic_reject(method.delivery_tag, requeue=False)
                    return
                try:
                    accept_result(result)
                except Exception:
                    logger.exception("CLCD result persistence failed")
                    ch.basic_nack(method.delivery_tag, requeue=True)
                    return
                ch.basic_ack(method.delivery_tag)

            channel.basic_consume(settings.landcover_result_queue, callback)
            channel.start_consuming()
        except Exception:
            logger.exception("CLCD result consumer disconnected")
            time.sleep(settings.reconnect_interval)
        finally:
            if conn and conn.is_open:
                conn.close()


def run_slave() -> None:
    logger.info("CLCD Slave %s configured output directory: %s", _worker_id, output_root())
    while True:
        conn = None
        try:
            conn = connect()
            channel = conn.channel()
            _topology(channel)
            channel.basic_qos(prefetch_count=settings.prefetch_count)
            channel.confirm_delivery()

            def callback(ch, method, properties, body):
                try:
                    task = json.loads(body)
                    task_id, label = task["task_id"], int(task["label"])
                except (ValueError, KeyError, TypeError):
                    ch.basic_reject(method.delivery_tag, requeue=False)
                    return
                retries = int((properties.headers or {}).get("x-retries", 0))
                logger.info("CLCD Slave %s accepted task %s:%s, retry=%s", _worker_id, task_id, label, retries)
                started_at = time.monotonic()
                try:
                    task_output_root = Path(task.get("output_root") or output_root())
                    if not task_output_root.is_absolute():
                        raise ValueError("任务成果目录必须是绝对路径")
                    result = run_chunk(task_output_root, task_id, label)
                    result["worker_id"] = _worker_id
                    _publish(ch, settings.landcover_result_queue, result)
                    ch.basic_ack(method.delivery_tag)
                    logger.info("CLCD Slave %s completed task %s:%s in %.1fs and reported result, pixels=%s, vectors=%s",
                                _worker_id, task_id, label, time.monotonic() - started_at, result["pixel_count"],
                                result.get("vector_feature_count", "legacy"))
                    return
                except Exception as exc:
                    logger.exception("CLCD subtask %s:%s failed, retry=%s", task_id, label, retries)
                    error = f"{type(exc).__name__}: {exc}"
                try:
                    if retries < settings.task_max_retries:
                        _publish(ch, settings.landcover_task_queue, task, {"x-retries": retries + 1})
                        ch.basic_ack(method.delivery_tag)
                    else:
                        _publish(ch, settings.landcover_result_queue,
                                 {"task_id": task_id, "label": label, "status": "failed", "error": error,
                                  "worker_id": _worker_id})
                        ch.basic_reject(method.delivery_tag, requeue=False)
                except Exception:
                    logger.exception("CLCD retry/result publish failed")
                    ch.basic_nack(method.delivery_tag, requeue=True)

            channel.basic_consume(settings.landcover_task_queue, callback)
            logger.info("CLCD Slave %s consuming %s", _worker_id, settings.landcover_task_queue)
            channel.start_consuming()
        except KeyboardInterrupt:
            break
        except Exception:
            logger.exception("CLCD Slave disconnected")
            time.sleep(settings.reconnect_interval)
        finally:
            if conn and conn.is_open:
                conn.close()
