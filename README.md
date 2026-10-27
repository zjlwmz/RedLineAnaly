# 1993 年 CLCD 土地覆盖分析

输入手绘面或省、市、县行政区域，统计区域内九类土地覆盖面积和占比，并导出裁剪 GeoTIFF 与分类面 GeoPackage。CLCD 任务采用 Master/Slave + RabbitMQ 分布式模式。

## 数据与配置

- 栅格：`D:\data_work\suanzi\tif\CLCD_v01_1993_albert.tif`，单波段、30 米 Albers 等积投影、`0` 为 NoData。
- 行政区：`D:\data_work\suanzi\base` 下的 `中国_省2.shp`、`中国_市2.shp`、`中国_县2.shp`。`name` 为名称，`gb` 为 9 位编码；原始 DBF 没有 `.cpg`，读取时明确使用 UTF-8。
- `config.ini` 中的 `clcd_raster`、`admin_data_dir`、`output_dir`、`max_export_pixels` 可修改路径及任务规模；环境变量分别为 `CLCD_RASTER`、`ADMIN_DATA_DIR`、`LANDCOVER_OUTPUT_DIR`、`MAX_EXPORT_PIXELS`。当前 `output_dir = D:\data_work\suanzi\out`，`max_export_pixels = 0` 表示不按像元数拒绝省、市或手绘范围。
- `split_level = province | city | county` 控制按省、市、县拆分；环境变量 `LANDCOVER_SPLIT_LEVEL` 可覆盖。每个任务记录提交时的拆分级别，修改配置不会改变已有任务。
- `role = all | master | slave` 控制节点角色。`all` 运行 Web + 本机 Worker；RabbitMQ 不可用时自动在后台本机计算。`master` 只提供 Web、规划、投递、合并与成果下载，`slave` 只消费 CLCD 子任务。CLCD 使用持久化任务队列、结果队列和死信队列。
- 多节点部署时，所有节点的 `clcd_raster` 必须指向内容和网格相同的栅格，`output_dir` 必须是所有节点可读写的**同一路径共享目录**（例如统一挂载的网络盘）。`db_path` 只由 Master 使用；各 Slave 无需共享 SQLite。RabbitMQ 地址与队列名称需一致。
- `clcd_classes.json` 定义 `0` 和 `1–9` 的类别名称。分类依据应与数据发布方的 `CLCD_classificationsystem.xlsx` 保持一致。

Python 依赖见 `requirements.txt`。GDAL 建议在 Conda 环境中安装，需与 Python 版本匹配；`requirements.txt` 不安装 GDAL。项目使用 `numpy`、`fastapi`、`uvicorn`、`pydantic`、`pika`。CLCD 相关测试位于 `tests/test_landcover.py`、`tests/test_distributed_landcover.py` 和 `tests/test_landcover_api.py`。

## 启动与 Web 测试

首次在每台节点安装依赖：

```powershell
conda activate gis
python -m pip install -r requirements.txt
```

RabbitMQ 可用时，在 **Master 的 PowerShell 窗口**启动 Web 服务和任务调度：

```powershell
conda activate gis
$env:LANDCOVER_OUTPUT_DIR = 'D:\data_work\suanzi\out'
python main.py master
```

在 **每台 Slave 各自的 PowerShell 窗口**启动计算节点；同一台机器也可以另开窗口启动多个 Slave：

```powershell
conda activate gis
$env:LANDCOVER_OUTPUT_DIR = 'D:\data_work\suanzi\out'
python main.py slave
```

Master 和 Slave 必须连接同一 RabbitMQ，并能读写同一份 `D:\data_work\suanzi\out` 目录、读取相同的 CLCD 栅格。多台机器上的 `D:` 若只是各自的本地磁盘，需要改用所有节点都能访问的同一路径共享目录。启动后可访问 `http://127.0.0.1:8001/health` 查看 Master 的 `output_dir`，Slave 启动日志也会打印实际目录。修改配置或代码后应重启已运行的节点；`LANDCOVER_OUTPUT_DIR` 会覆盖 `config.ini`。

提交任务后，Master 控制台会记录子任务投递、结果接收与最终合并；每个 Slave 控制台会记录收到的任务 ID、分块编号、裁剪进度、矢量化进度及完成结果。日志中的 `pid` 和 `landcover-主机名-PID` 可用来区分节点。若 Slave 控制台只有 `consuming landcover_task_queue` 而没有 `accepted task`，说明该节点尚未取到任务。

打开 `http://127.0.0.1:8001/`，可选择省、市、县、导入 GeoJSON 文件或绘制面，点击“计算并导出”；页面轮询子任务进度，完成后下载成果。GeoJSON 支持 Polygon、MultiPolygon、Feature 及由多个面组成的 FeatureCollection，坐标须为 WGS84 经纬度；多个面会作为一个 MultiPolygon 范围计算。规划、从节点裁剪和主节点合并按固定大小的栅格块处理；Slave 还会各自矢量化分块，Master 最后合并分块 TIFF 和 GPKG。当前 `max_export_pixels = 0`，不按像元数限制范围；范围越大，计算、矢量化所需时间和磁盘空间越多。本地没有 RabbitMQ 时，也可只运行 `python main.py all`，由同一进程在后台计算。

任务失败且已有完整拆分计划时，可在 PowerShell 中发送 **POST** 请求重试。浏览器地址栏打开该 URL 会发送 GET，不能触发重试：

```powershell
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8001/landcover/retry/<task_id>'
```

重试会复用原任务的 `plan.json` 和完整分块，只重新投递未成功的子任务。

## 计算口径

1. 输入面从其 EPSG 坐标系转换到 CLCD 栅格自身的 Albers 投影，以栅格原始 30 米网格为准，不进行重采样。
2. 像元中心落入面内时计入该像元。每个有效像元面积为 `30 × 30 = 900 m²`。`0` 不参与九类统计。
3. `某类面积 = 该类有效像元数 × 900 m²`；`某类占比 = 某类有效像元数 ÷ 全部有效分类像元数 × 100%`。同时返回输入面面积、面内像元面积、NoData 面积和有效覆盖率。输入面真实面积与面内像元面积可能因边界像元选择而不同。
4. 裁剪 GeoTIFF 保留原始类别值、颜色表、投影和 30 米网格，面外写成 NoData。GeoPackage 的 `landcover` 图层按相邻同类像元矢量化，字段为 `class_code`、`class_name`、`area_m2`。矢量边界是栅格像元边界，不是地籍地块边界。
5. Master 按行政代码顺序把输入面内像元分配给子任务。行政边界重叠时像元归第一个行政区；行政区未覆盖的面内像元进入“行政区划空隙”子任务，避免重复或漏算。统计从合并后的 TIFF 计算；各 Slave 按同一像元口径生成分块 GPKG，Master 再合并为最终 GPKG。跨分块的连续同类斑块会保留分块边界。

## 接口

| 方法 | 路径 | 用途 |
|---|---|
| GET | `/health` | 检查数据是否就绪 |
| GET | `/landcover/regions?level=province\|city\|county&parent_code=...` | 获取行政区列表 |
| GET | `/landcover/region-geometry?level=county&code=...` | 地图预览行政区几何 |
| POST | `/landcover/analyze` | 持久化任务后立即返回 HTTP 202 和 `task_id`；行政区读取、拆分和投递在后台执行 |
| GET | `/landcover/result/{task_id}` | 查询 `planning/pending/aggregating/done/failed` 状态、拆分级别、子任务进度、`created_at`、`finished_at`、`duration_seconds`；`done` 时包含最终 `result` |
| POST | `/landcover/retry/{task_id}` | 使用原计划重试失败任务的未成功子任务 |
| GET | `/landcover/download/{task_id}/raster` | 下载 GeoTIFF |
| GET | `/landcover/download/{task_id}/vector` | 下载 GeoPackage |

手绘面请求示例：

```json
{"geometry":{"type":"Polygon","coordinates":[[[116.2,39.0],[116.3,39.0],[116.3,39.1],[116.2,39.1],[116.2,39.0]]]},"geometry_srid":4326}
```

行政区请求示例：

```json
{"region_level":"county","region_code":"156420704"}
```

成功响应采用 `{ "success": true, "msg": null, "data": ... }`；最终结果中包含九类 `classes`、面积、占比以及两个下载链接。成果保存在 `D:\data_work\suanzi\out\<task_id>\`：裁剪栅格为 `CLCD_1993_clip.tif`，分类矢量为 `CLCD_1993_classes.gpkg`。同目录还保存任务计划、像元分配表、分块 TIFF/GPKG 和结果 JSON。

`/landcover/analyze` 返回 `{"success":true,"msg":null,"data":{"task_id":"..."}}` 后，可轮询结果接口。规划期间 `status=planning`、`total=0`；规划失败时 `status=failed` 并在 `error` 中说明原因。Master 在规划期间重启时，原任务会标为失败，需要重新提交。

`finished_at` 和 `duration_seconds` 在任务完成或失败后返回，时间使用 Master 本地时间。历史已完成任务没有数据库完成时间时，接口按 `result.json` 的文件修改时间估算，并将 `finished_at_estimated` 设为 `true`；历史失败任务若无可靠时间则保持 `null`。
