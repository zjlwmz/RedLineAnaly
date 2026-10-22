"""SQLite 持久化存储层：主任务（task）与子任务（sub，按省）状态落库。

命名对齐 op-hub 模板：
- 主任务：tasks.task_id（对外 taskId）
- 子任务：subtasks.sub_id（对外 subId），值形如 "<task_id>:<province_code>"

设计要点：
- 单连接 + 互斥锁：结果回收线程写、REST 接口读，量级很小，串行化足够
- WAL 模式：读写不互相阻塞，进程被杀/断电后不丢已提交事务
- published 标记：子任务消息是否已成功投递到 RabbitMQ。
  服务停止/断电后重启时，把 status=pending 且 published=0 的子任务重新投递；
  已投递（published=1）的消息由 RabbitMQ 持久化保证不丢，无需重投。
"""
import json
import logging
import os
import sqlite3
import threading
from typing import Optional

from ophub.config import BASE_DIR, settings

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_conn: sqlite3.Connection | None = None


def _db_file() -> str:
    """解析数据库文件绝对路径（相对路径锚定工程根目录）。"""
    path = settings.db_path
    if not os.path.isabs(path):
        path = os.path.join(BASE_DIR, path)
    return path


def init_db() -> str:
    """打开连接并建表，返回实际数据库文件绝对路径。"""
    global _conn
    with _lock:
        if _conn is None:
            _conn = sqlite3.connect(_db_file(), check_same_thread=False)
            _conn.row_factory = sqlite3.Row
            _conn.execute("PRAGMA journal_mode=WAL")
            _conn.execute("PRAGMA synchronous=NORMAL")
            _conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id       TEXT PRIMARY KEY,
                    feature_id    TEXT NOT NULL,
                    input_wkt     TEXT NOT NULL,
                    input_area_m2 REAL NOT NULL,
                    buffer_m      REAL NOT NULL,
                    severe_ratio  REAL NOT NULL,
                    provinces     TEXT NOT NULL,    -- JSON: [{"code","name"}]
                    created_at    TEXT NOT NULL,
                    status        TEXT NOT NULL DEFAULT 'pending',  -- pending / aggregating / done / failed
                    summary       TEXT,             -- JSON: 最终汇总结果（完成后写入）
                    UNIQUE(feature_id, input_wkt)
                );
                CREATE TABLE IF NOT EXISTS subtasks (
                    sub_id           TEXT PRIMARY KEY,
                    task_id          TEXT NOT NULL REFERENCES tasks(task_id),
                    province_code    TEXT NOT NULL,
                    province_name    TEXT NOT NULL,
                    status           TEXT NOT NULL DEFAULT 'pending',  -- pending / success / no_data / failed
                    result_json      TEXT,             -- Slave 回传的完整结果 JSON
                    error            TEXT,
                    finished_at      TEXT,
                    published        INTEGER NOT NULL DEFAULT 0,
                    UNIQUE(task_id, province_code)
                );
                CREATE INDEX IF NOT EXISTS idx_subtasks_task ON subtasks(task_id);
                CREATE INDEX IF NOT EXISTS idx_subtasks_status ON subtasks(status);
                """
            )
            _conn.commit()
    return _db_file()


def save_task(
    task_id: str,
    feature_id: str,
    input_wkt: str,
    input_area_m2: float,
    buffer_m: float,
    severe_ratio: float,
    provinces: list[dict],
    created_at: str,
) -> tuple[list[str], Optional[str]]:
    """保存主任务 + 全部子任务，返回 (sub_ids, None)；
    若 UNIQUE(feature_id, input_wkt) 冲突，返回 ([], existing_task_id) 表示幂等命中。"""
    sub_ids = [f"{task_id}:{p['code']}" for p in provinces]
    with _lock:
        try:
            _conn.execute(
                """
                INSERT INTO tasks (task_id, feature_id, input_wkt, input_area_m2, buffer_m, severe_ratio, provinces, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (task_id, feature_id, input_wkt, input_area_m2, buffer_m, severe_ratio,
                 json.dumps(provinces, ensure_ascii=False), created_at),
            )
            _conn.executemany(
                """
                INSERT INTO subtasks (sub_id, task_id, province_code, province_name)
                VALUES (?, ?, ?, ?)
                """,
                [
                    (sub_ids[i], task_id, provinces[i]["code"], provinces[i]["name"])
                    for i in range(len(provinces))
                ],
            )
            _conn.commit()
            return sub_ids, None
        except sqlite3.IntegrityError:
            # 同 feature_id + 同几何已存在 → 幂等命中，返回旧 task_id
            row = _conn.execute(
                "SELECT task_id FROM tasks WHERE feature_id = ? AND input_wkt = ?",
                (feature_id, input_wkt),
            ).fetchone()
            existing = row["task_id"] if row else None
            logger.info("Duplicate submission (feature_id=%s) → existing task %s", feature_id, existing)
            return [], existing


def mark_published(sub_ids: list[str]) -> None:
    """子任务消息成功投递到 RabbitMQ 后打标记。"""
    if not sub_ids:
        return
    with _lock:
        _conn.executemany(
            "UPDATE subtasks SET published = 1 WHERE sub_id = ?",
            [(s,) for s in sub_ids],
        )
        _conn.commit()


def fail_unpublished(task_id: str, error: str, finished_at: str) -> None:
    """投递失败：该任务中尚未投递的 pending 子任务全部置为 failed。"""
    with _lock:
        _conn.execute(
            "UPDATE subtasks SET status = 'failed', error = ?, finished_at = ? "
            "WHERE task_id = ? AND status = 'pending' AND published = 0",
            (error, finished_at, task_id),
        )
        _conn.execute(
            "UPDATE tasks SET status = 'failed' WHERE task_id = ?", (task_id,),
        )
        _conn.commit()


def update_subtask(sub_id: str, status: str, result_json: str | None, error: str | None, finished_at: str) -> bool:
    """结果回传：更新子任务状态。返回是否命中（False=库中无此子任务）。"""
    with _lock:
        cur = _conn.execute(
            "UPDATE subtasks SET status = ?, result_json = ?, error = ?, finished_at = ? WHERE sub_id = ?",
            (status, result_json, error, finished_at, sub_id),
        )
        _conn.commit()
    return cur.rowcount > 0


def update_task_summary(task_id: str, status: str, summary_json: str) -> None:
    """全部子任务完成或超时后，写入最终汇总结果。"""
    with _lock:
        _conn.execute(
            "UPDATE tasks SET status = ?, summary = ? WHERE task_id = ?",
            (status, summary_json, task_id),
        )
        _conn.commit()


def get_task(task_id: str) -> dict | None:
    """取主任务 + 全部子任务。"""
    with _lock:
        trow = _conn.execute(
            "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
        ).fetchone()
        if trow is None:
            return None
        srows = _conn.execute(
            "SELECT * FROM subtasks WHERE task_id = ? ORDER BY province_code",
            (task_id,),
        ).fetchall()
    task = dict(trow)
    task["provinces"] = json.loads(task["provinces"]) if task.get("provinces") else []
    task["subtasks"] = [dict(r) for r in srows]
    return task


def get_pending_tasks() -> list[dict]:
    """全局待处理任务列表：仍有 pending 子任务的任务及其进度计数（按下发时间排序）。"""
    with _lock:
        rows = _conn.execute(
            """
            SELECT t.task_id, t.feature_id, t.created_at,
                   COUNT(*)                           AS total,
                   SUM(s.status = 'pending')          AS pending_count,
                   SUM(s.status IN ('success','no_data')) AS finished_count,
                   SUM(s.status = 'failed')           AS failed_count
            FROM tasks t JOIN subtasks s ON s.task_id = t.task_id
            GROUP BY t.task_id
            HAVING pending_count > 0
            ORDER BY t.created_at, t.task_id
            """
        ).fetchall()
    return [dict(r) for r in rows]


def get_unpublished_pending_subtasks() -> list[dict]:
    """重启恢复用：已落库但未成功投递的 pending 子任务。"""
    with _lock:
        rows = _conn.execute(
            "SELECT sub_id, task_id, province_code, province_name FROM subtasks "
            "WHERE status = 'pending' AND published = 0 ORDER BY rowid"
        ).fetchall()
    return [dict(r) for r in rows]
