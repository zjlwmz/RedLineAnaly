"""ophub 分布式红线叠加分析算子服务。

模块说明：
- config   全局配置（环境变量 > config.ini > 默认值）
- mq       RabbitMQ 连接与拓扑声明
- scanner  省份识别 + 按省拆分任务
- calculator 红线压盖计算（调用 core/overlay 等核心算法）
- store    SQLite 任务持久化
- master   Master 节点（REST 服务 + 任务下发 + 结果回收）
- slave    Slave 节点（任务消费与计算）
"""
