package com.github.dzlog;

import com.gitee.bee.core.conf.*;
import com.google.common.collect.Sets;

/**
 * @author melin
 */
public class DzlogConf extends BeeConf {

    //---------------------------------------------------------------------------------------------------------------
    public static final ConfigEntry<MapStringValue> DZLOG_DATA_CENTER_KAFKA_AUTO_OFFSET_RESET = buildConf("dzlog.data.center.kafka.auto.offset.reset")
            .doc("设置kafka auto.offset.reset 值，可设置值为：latest 和 earliest")
            .version("3.0.0")
            .mapStringConf()
            .createWithDefault(new MapStringValue("hangzhou", "latest"));

    public static final ConfigEntry<MapStringValue> DZLOG_DATA_CENTER_KAFKA_BROKERS = buildConf("dzlog.data.center.kafka.brokers")
            .doc("kafka 集群 broker列表，多个值逗号分隔")
            .version("3.0.0")
            .mapStringConf()
            .createWithDefault(new MapStringValue());

    public static final ConfigEntry<MapIntegerValue> DZLOG_DATA_CENTER_CONSUMER_RATE_LIMITER = buildConf("dzlog.data.center.consumer.rate.limiter")
            .doc("kafka client 每秒消费限速, 默认值 -1, 不限速")
            .version("3.0.0")
            .mapIntegerConf()
            .createWithDefault(new MapIntegerValue("hangzhou", -1));

    public static final ConfigEntry<Integer> DZLOG_KAFKA_COMMIT_MAX_NUM = buildConf("dzlog.kafka.commit.max.num")
            .doc("kafka client 消费一定数量提交一次")
            .version("3.0.0")
            .intConf()
            .createWithDefault(10000);

    public static final ConfigEntry<Integer> DZLOG_KAFKA_COMMIT_MAX_INTERVAL_SECONDS = buildConf("dzlog.kafka.commit.max.interval.seconds")
            .doc("kafka client 消费间隔多久提交一次，单位秒")
            .version("3.0.0")
            .intConf()
            .createWithDefault(30);

    public static final ConfigEntry<MapStringValue> DZLOG_DATA_CENTER_KERBEROS_PRINCIPAL = buildConf("dzlog.data.center.kerberos.principal")
            .doc("kerberos principal")
            .version("3.0.0")
            .mapStringConf()
            .createWithDefault(new MapStringValue("hangzhou", ""));

    public static final ConfigEntry<MapStringValue> DZLOG_DATA_CENTER_KERBEROS_KEYTAB_PATH = buildConf("dzlog.data.center.kerberos.keytab.path")
            .doc("kerberos keytab path")
            .version("3.0.0")
            .mapStringConf()
            .createWithDefault(new MapStringValue("hangzhou", ""));
}
