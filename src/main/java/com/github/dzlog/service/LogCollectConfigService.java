package com.github.dzlog.service;

import com.github.dzlog.entity.LogCollectConfig;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by admin
 */
@Service
public class LogCollectConfigService implements InitializingBean {

	private static final Logger LOGGER = LoggerFactory.getLogger(LogCollectConfigService.class);

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Value("${dzlog.kafka.cluster.name}")
	private String kafkaClusterCode;

	private LoadingCache<String, LogCollectConfig> codeCache;

	private LoadingCache<String, String> topicCache;

	@Value("${dzlog.datacenter}")
	private String dataCenter;

	@Override
	public void afterPropertiesSet() throws Exception {
		if (StringUtils.isBlank(dataCenter)) {
			throw new IllegalArgumentException("dataCenter can not empty");
		}
		LOGGER.info("dataCenter : {}", dataCenter);

		codeCache = CacheBuilder.newBuilder()
				.maximumSize(1000)
				.expireAfterWrite(60, TimeUnit.SECONDS)
				.build(
						new CacheLoader<String, LogCollectConfig>() {
							public LogCollectConfig load(String code) throws Exception {
								try {
									String sql = "SELECT * FROM dc_log_collect_config where code = ? and kafka_cluster = ? and data_center = ?";
									return jdbcTemplate.queryForObject(sql, new Object[]{code, kafkaClusterCode, dataCenter}, rowMapper);
								} catch (Exception e) {
									LOGGER.error("query code {} error: {}", code, e.getMessage());
									return null;
								}
							}
						});

		topicCache = CacheBuilder.newBuilder()
				.maximumSize(1000)
				.expireAfterWrite(60, TimeUnit.SECONDS)
				.build(
						new CacheLoader<String, String>() {
							public String load(String topic) throws Exception {
								try {
									String sql = "SELECT code, kafka_topic FROM dc_log_collect_config where kafka_topic = ? and kafka_cluster = ? and data_center = ?";
									// 支持一个采集配置多个topic， like 查询
									List<Map<String, Object>> list = jdbcTemplate.queryForList(sql, new Object[]{topic, kafkaClusterCode, dataCenter});
									if (list.size() == 1) {
										return (String) list.get(0).get("code");
									} else if (list.size() > 1) {
										// 防止模糊配置多个
										for (Map<String, Object> map : list) {
											String code = (String) map.get("code");
											String tpc = (String) map.get("topic");

											String[] topics = StringUtils.split(tpc, ",");
											if (ArrayUtils.contains(topics, topic)) {
												return code;
											}
										}
									}
									LOGGER.error("query topic {} is null", topic);
									return null;
								} catch (Exception e) {
									LOGGER.error("query topic {} error: {}", topic, e.getMessage());
									return null;
								}
							}
						});
	}

	private RowMapper<LogCollectConfig> rowMapper = (resultSet, i) -> {
		LogCollectConfig entity = new LogCollectConfig();
		entity.setAppName(resultSet.getString("app_name"));
		entity.setCode(resultSet.getString("code"));
		entity.setDataCenter(resultSet.getString("data_center"));
		entity.setCollectFile(resultSet.getString("collect_file"));
		entity.setRunStatus(resultSet.getInt("run_status"));
		entity.setDatabaseName(resultSet.getString("database_name"));
		entity.setTableName(resultSet.getString("table_name"));
		entity.setKafkaTopic(resultSet.getString("kafka_topic"));
		return entity;
	};

	public LogCollectConfig getDcLogByCode(String code) {
		try {
			return codeCache.getUnchecked(code);
		} catch (CacheLoader.InvalidCacheLoadException e) {
			return null;
		}
	}

	public String getCodeByTopic(String topic) {
		if (StringUtils.endsWith(topic, "-sh")) {
			topic = StringUtils.substringBeforeLast(topic, "-sh");
		}
	    try {
			return topicCache.getUnchecked(topic);
		} catch (CacheLoader.InvalidCacheLoadException e) {
			return null;
		}
	}

	public List<String> queryAllTopics() {
		String sql = "SELECT distinct kafka_topic FROM dc_log_collect_config where run_status = 1 and kafka_cluster = ? and data_center = ?";
		List<String> topics = jdbcTemplate.queryForList(sql, String.class, kafkaClusterCode, dataCenter);

		// 支持一个采集配置多个topic
		List<String> newTopics = new ArrayList<>();
		for (String topic : topics) {
			for (String name : StringUtils.split(topic, ",")) {
				newTopics.add(name);
			}
		}

		return newTopics;
	}

	public List<LogCollectConfig> queryAllCollects() {
		try {
			String sql = "SELECT * FROM dc_log_collect_config where data_center = ? and kafka_cluster = ? and run_status=1";
			return jdbcTemplate.query(sql, rowMapper, dataCenter, kafkaClusterCode);
		} catch (Exception e) {
			LOGGER.error("list topics error", e);
			return null;
		}
	}

}
