# 国土图斑 × 全国红线叠加分析

分布式红线压盖分析算子：**输入一个面 → 动态识别涉及省份 → RabbitMQ 共享队列分发 → 多节点并行 OGR 空间相交 → SQLite 幂等汇总 → 合规判定 + 地图渲染**。

架构参考自 [op-hub](https://github.com/) 分布式算子模板（config.ini 配置、统一入口 role 分发、共享队列 + SQLite 持久化）。

## 快速开始

```bash
conda activate gis          # 确保 GDAL / pyproj / pika / fastapi / pydantic 都在

# 1) 单节点跑通（推荐本地验证）
python main.py              # config.ini 默认 role=all：Master + Slave 合一
# → 打开 http://127.0.0.1:8001/ 点「载入示例」→「分析当前图斑」

# 2) 分布式跑
docker run -d --name redline-rmq -p 5672:5672 rabbitmq:3-alpine
python main.py master       # 终端 A：只起 REST 接口
python main.py slave        # 终端 B：起计算节点（多开几个自动分担）
```

> **依赖说明**：GDAL Python 绑定（`osgeo`）建议 conda 安装（`conda install -c conda-forge gdal pyproj shapely`）。MQ 不可用时自动降级本地串行计算。

## 架构

```
             ┌─────────────────────────────────────────────┐
  前端地图    │  main.py (role=master)                       │
  (Leaflet)  │  ├─ 坐标统一 → CGCS2000 / EPSG:4490          │
   ──POST──▶ │  ├─ 省份识别（bbox 预筛 + 包围盒精确相交）     │
             │  ├─ 按省拆 N 个子任务 → topic exchange 投递    │
             │  ├─ SQLite 落库 + 恢复线程                    │
             │  └─ 结果消费线程（更新 subtask.status）        │
             └───────────────┬─────────────────────────────┘
                             │ task.redline.calculate
              ┌──────────────┴──────────────┐
        ┌─────▼─────┐                 ┌─────▼─────┐
        │ slave-1   │     ...         │ slave-N   │   共享 redline_task_queue
        │ OGR 相交  │                 │ OGR 相交  │   prefetch=1 天然负载均衡
        └─────┬─────┘                 └─────┬─────┘
              │ result.redline.overlay        │
              └───────────────┬──────────────┘
                              ▼
                     主节点结果聚合（batch_id + task_id 去重）
```

共享队列 vs 原独占队列：同省任务任意 worker 都能处理，不需要「worker 发现」逻辑。MQ 不可用时 Master 自动降级本地串行。

## 为什么面积计算要这么复杂

绝大多数项目直接 `GetArea()` 得到的是**平方度**（EPSG:4490 经纬度坐标下），高纬误差可达数倍。本实现：

1. **统一坐标到 CGCS2000 地理坐标**（EPSG:4490），上游输入可能是 WGS84 / Web Mercator / 高斯投影
2. **投影到以区域中心为基准的 Albers 等积圆锥投影**（标准纬线 ±3°）算面积，误差 < 0.01%
3. **pyproj Karney 椭球面积交叉校验**，差异 > 0.5% 说明投影参数或数据基准有问题
4. **并集口径 vs 相加口径**：跨省时同一块压盖被两侧各算一次，`union` 去重后才是真实压盖面积

## 目录结构

```
main.py                    统一入口（role=all/master/slave）
config.ini                 配置文件（env > ini > 默认值）
ophub/
  config.py                configparser 加载 config.ini
  mq.py                    共享队列拓扑（topic exchange + 死信）
  store.py                 SQLite 持久化（tasks + subtasks，WAL 模式）
  scanner.py               省份识别 + 按省拆任务（调用 core.province）
  calculator.py            单省红线计算入口（调用 core.overlay）
  master.py                FastAPI + TaskProducer + 结果回收线程
  slave.py                 共享队列消费者（prefetch=1，失败重试）
core/                      计算核心（单机/分布式共用）
  geo.py                   坐标统一 / 等积面积 / 缓冲 / 椭球校验
  province.py              动态省份识别（bbox 预筛 + 几何相交）
  overlay.py               面-面叠加（逐图斑相交 → Union → 面积）
  redline_index.py         GPKG + R 树空间索引（SetSpatialFilter 粗筛）
  quickstart.py            单文件入口：串行跑完所有省，调试最快
  _config.py               core 内部数据路径配置
data/                      红线数据（按省 GPKG，文件名=行政区划代码）
scripts/                   gen_data.py 等开发工具（import 已过期，见下方）
tests/                     e2e_test.py（需要 RabbitMQ）
web/                       前端 Leaflet 地图（http://127.0.0.1:8001/）
```

## 配置

`config.ini`（所有字段都有默认值，按需改）：

| 分组 | 字段 | 说明 |
|---|---|---|
| 角色 | `role` | `all` / `master` / `slave`，也可命令行覆盖 `python main.py slave` |
| 业务 | `severe_ratio` | 严重压盖阈值（默认 0.5） |
| | `buffer_m` | 红线缓冲距离（默认 50m，吃省界裁剪缝隙） |
| MQ | `rabbitmq_host/port/user/password` | RabbitMQ 连接 |
| | `task_routing_key` / `result_routing_key` | 消息路由键（主/从必须一致） |
| | `prefetch_count` | 1 = 公平分发，长任务不并发 |
| | `task_max_retries` | 单任务失败重试次数，超上限进死信队列 |
| 持久化 | `db_path` | SQLite 文件，默认 `tasks.db`（WAL 模式） |

环境变量同名可覆盖（部署时方便）：`RABBITMQ_HOST`、`ROLE`、`SEVERE_RATIO` 等。

## 数据契约

红线按省一个 GPKG：

| 字段 | 类型 | 说明 |
|---|---|---|
| `patch_id` | string | 图斑唯一标识（字段名自动识别 patch_id / fid / ID / OBJECTID） |
| `type` | string | 红线类型：`生态保护红线` / `永久基本农田` / `城镇开发边界` |
| `geometry` | Polygon / MultiPolygon | CGCS2000 经纬度 |

演示数据位于 `data/redline/{110000,120000,130000,140000,370000,410000}.gpkg`（京津冀鲁晋豫）。

## API

### `POST /overlay/analyze`

下发叠加分析任务。MQ 模式立即返回 batch_id，前端轮询结果。

```json
{
  "feature_id": "P-2024-0001",
  "geometry": {"type": "Polygon", "coordinates": [[[116.2,39.0],[116.9,39.0],[116.9,39.6],[116.2,39.6],[116.2,39.0]]]},
  "geometry_srid": 4326,
  "severe_ratio": 0.5,
  "buffer_m": 50
}
```

返回（MQ 模式，包含 summary 需调 `/overlay/result`）：

```json
{"success": true, "msg": null, "data": {
  "batch_id": "B20261020161146-812d1347",
  "dispatch_mode": "mq",
  "subtask_count": 3,
  "provinces": [{"code":"110000","name":"北京市"}, {"code":"130000","name":"河北省"}]
}}
```

本地模式（`dispatch_mode=local`）在 POST 内直接算完，`data` 就是完整汇总结果。

### `GET /overlay/result?batchId=xxx`

聚合 SQLite 中已完成的子任务，返回完整汇总：

```json
{"success": true, "msg": null, "data": {
  "batch_id": "B...",
  "status": "done",              // done / aggregating / failed
  "severity": "SEVERE",           // CLEAR / OCCUPIED / SEVERE / OUTSIDE
  "is_compliant": false,
  "input_area_m2": 822119476.28,
  "union_overlap_area_m2": 822194460.35,
  "overlap_ratio_union": 100.0,
  "provinces": [
    {"province_code":"110000","province_name":"北京市","status":"success",
     "patch_count":2,"patches_scanned":1200,"worker_id":"slave-bogon-35755",
     "elapsed_ms":123.4}
  ],
  "patches": [...],          // 按 patch_id 跨省合并的明细
  "by_type": [...],          // 按红线类型汇总
  "overlap_geometry": {...}, // 全部压盖的并集 GeoJSON
  "task_summary": {"expected":3,"succeeded":2,"no_data":0,"failed":0,"pending":1}
}}
```

### 其他接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 健康检查 |
| GET | `/overlay/result?batchId=xxx` | 任务结果（见上） |
| GET | `/tasks/pending` | 全局待处理任务列表 |
| GET | `/tasks/pending?batchId=xxx` | 指定任务的子任务进度 |
| GET | `/nodes` | 当前连接的 Slave 消费者（调 RabbitMQ Management API） |
| GET | `/provinces` | 支持的省份列表 |

## 验证

```bash
conda run -n gis python3 core/quickstart.py    # 单机串行，秒级出结果
conda run -n gis python3 tests/e2e_test.py      # 端到端（需 RabbitMQ）
```

## 生产化注意

演示跑通 ≠ 能上生产，这几块刻意没做：

1. **持久化用 SQLite**——单文件 WAL，几百万任务后需换 PG，`batch_id/task_id` 唯一索引天然幂等
2. **RabbitMQ 单点**——镜像队列 / Quorum 队列 + 集群
3. **红线数据版本管理**——结果要记录用了哪版红线，合规结论要可追溯
4. **省份边界是简化数据**——约 1km 精度，生产替换为自然资源部标准行政区划
5. **坐标系审计**——每步坐标变换要可回溯（CGCS2000 → Albers → ...）
6. **消息体 WKT 直传**——大面几 MB，生产走 OSS/MinIO 传 key
