## 数据采集平台
从kafka 中消费数据写入hive表中，一般很容想到用spark，flink消费数据写入hive表中。业务需要申请kafka topic，再写spark/flink 任务，把数据写入hive 表，过程比较长。如果需要采集应用日志很多，启动的spark/flink任务相应也很多

为了平台化解决数据采集问题，用户只需要填写topic 和要写入hive表名，写入数据自动完成，dzlog不依赖spark/flink，是一个spring boot 应用，基于spring kafka 同时消费多个topic并发消费数据，补数据多个节点分布式消费数据，增加吞吐能力。

## dzlog 规则
1. 每十五分钟一个分区，按照消息接收时间作为区分。
2. 只写入原始数据，不做解析，用户单独跑ETL任务解析数据。
3. 支持parquet、orc、iceberg三种格式。
4. 自动合并碎片文件。

## 采集配置建表脚本
```sql
CREATE TABLE dc_log_collect_config (
    `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'Id',
    `code` varchar(128) DEFAULT NULL COMMENT 'code',
    `data_center` varchar(128) DEFAULT NULL COMMENT '数据中心',
    `app_name` varchar(128) DEFAULT NULL COMMENT '应用名',
    `database_name` varchar(128) NOT NULL COMMENT '数据库',
    `table_name` varchar(128) NOT NULL COMMENT '表名',
    `collect_file` varchar(256) NOT NULL COMMENT '采集文件路径',
    `kafka_cluster` varchar(45) NOT NULL COMMENT 'Kafka cluster',
    `kafka_topic` varchar(45) NOT NULL COMMENT 'Kafka topic',
    `flow_status` int(1) NOT NULL DEFAULT '0' COMMENT '1：审核通过，2：暂停、0: 待审核',
    `run_status` int(1) NOT NULL DEFAULT '0' COMMENT '1：启动，2：暂停、0：停止',
    `gmt_created` datetime DEFAULT NULL COMMENT '创建时间',
    `gmt_modified` datetime DEFAULT NULL COMMENT '更新时间',
    `creater` varchar(45) DEFAULT NULL COMMENT '创建人',
    `modifier` varchar(45) DEFAULT NULL COMMENT '更新人',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_code` (`code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='采集服务器信息';

CREATE TABLE `dc_dc_log_collect_metric` (
    `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'Id',
    `node_ip` varchar(128) DEFAULT NULL COMMENT '采集节点IP',
    `code` varchar(45) NOT NULL COMMENT '采集code',
    `collect_date` varchar(45) NOT NULL COMMENT '采集日期：yyyyMMdd',
    `hour_period` varchar(16) DEFAULT NULL COMMENT '小时',
    `minute_period` varchar(16) DEFAULT NULL COMMENT '分钟',
    `minute_count` int(11) DEFAULT NULL COMMENT '分钟消息数量',
    `minute_bytes` int(11) DEFAULT NULL COMMENT '分钟消息大小',
    `gmt_created` datetime DEFAULT NULL COMMENT '创建时间',
    `gmt_modified` datetime DEFAULT NULL COMMENT '更新时间',
    `creater` varchar(45) DEFAULT NULL COMMENT '创建人',
    `modifier` varchar(45) DEFAULT NULL COMMENT '更新人',
    PRIMARY KEY (`id`),
    KEY `idx_code` (`code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='采集服务器信息';
```