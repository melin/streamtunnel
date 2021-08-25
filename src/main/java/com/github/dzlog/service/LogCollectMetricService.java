package com.github.dzlog.service;

import com.github.dzlog.entity.LogCollectMetric;
import com.github.dzlog.util.CommonUtils;
import com.github.dzlog.util.NetUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Created by admin
 */
@Service
public class LogCollectMetricService {

	private static final Logger LOGGER = LoggerFactory.getLogger(LogCollectMetricService.class);

	@Autowired
	private JdbcTemplate jdbcTemplate;

	public static String currentIpLong;

	static {
		try {
			String currentIp = NetUtils.determineIpAddress();
			currentIpLong = String.valueOf(CommonUtils.ipToLong(currentIp));
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	private RowMapper<List<LogCollectMetric>> rowMapper = (resultSet, i) ->{
		List<LogCollectMetric> entityList = new LinkedList<>();
		do {
			LogCollectMetric entity = new LogCollectMetric();
			entity.setId(resultSet.getLong("id"));
			entity.setNodeIp(resultSet.getString("node_ip"));
			entity.setCode(resultSet.getString("code"));
			entity.setCollectDate(resultSet.getString("collect_date"));
			entity.setHourPeriod(resultSet.getString("hour_period"));
			entity.setMinutePeriod(resultSet.getString("minute_period"));
			entity.setMinuteCount(resultSet.getLong("minute_count"));
			entity.setMinuteBytes(resultSet.getLong("minute_bytes"));
			entityList.add(entity);
		} while (resultSet.next());
		return entityList;
	};

	public LogCollectMetric createEntity(String code, String partition, long msgCount, long msgBytes) {
		LogCollectMetric entity = new LogCollectMetric();
		entity.setCode(code);
		entity.setNodeIp(currentIpLong);
		entity.setMinuteCount(msgCount);
		entity.setMinuteBytes(msgBytes);

		String collectDate = StringUtils.substring(partition, 0, 8);
		String hour = StringUtils.substring(partition, 8, 10);
		String minute = StringUtils.substring(partition, 8, 12);
		entity.setCollectDate(collectDate);
		entity.setHourPeriod(hour);
		entity.setMinutePeriod(minute);

		return entity;
	}

	public void recordEntity(LogCollectMetric entity) {
		List<LogCollectMetric> entityList = this.findByNamedParam(
				new String[]{"code", "node_ip", "collect_date", "hour_period", "minute_period"},
				new Object[]{entity.getCode(), entity.getNodeIp(), entity.getCollectDate(),
						entity.getHourPeriod(), entity.getMinutePeriod()});
		if (entityList == null || entityList.size() <= 0) {
			this.insertEntity(entity);
		} else {
			LogCollectMetric oldEntity = entityList.get(0);
			oldEntity.setMinuteCount(oldEntity.getMinuteCount() + entity.getMinuteCount());
			oldEntity.setMinuteBytes(oldEntity.getMinuteBytes() + entity.getMinuteBytes());
			this.updateEntity(oldEntity);
		}
	}

	public List<LogCollectMetric> findByNamedParam(String[] propertyNames, Object[] values) {
		try {
			String sql = "SELECT * FROM dc_log_collect_metric";
			if (propertyNames != null && values != null) {
				if (propertyNames.length != values.length) {
					throw new RuntimeException("property length unequal to values length");
				}
				if (propertyNames.length > 0) {
					List<String> tempSql = new LinkedList<>();
					for (String property : propertyNames) {
						tempSql.add(property + "=?");
					}
					sql += " WHERE " + StringUtils.join(tempSql, " AND ");
				}
			}
			return jdbcTemplate.queryForObject(sql, values, rowMapper);
		} catch (Exception e) {
			LOGGER.error("query dc_log_collect_metric error: {} ", e.getMessage());
			return null;
		}
	}

	public int updateEntity(LogCollectMetric entity) {
		try {
			String sql = "UPDATE dc_log_collect_metric SET " +
					"code=?, collect_date=?, hour_period=?, " +
					"minute_period=?, minute_count=?, minute_bytes=?, gmt_modified=? WHERE id=?";
			return jdbcTemplate.update(sql, entity.getCode(), entity.getCollectDate(),
					entity.getHourPeriod(), entity.getMinutePeriod(), entity.getMinuteCount(), entity.getMinuteBytes(),
					new Date(), entity.getId());
		} catch (Exception e) {
			LOGGER.error("update dc_log_collect_metric error: {} ", e.getMessage());
			return 0;
		}
	}

	public int insertEntity(LogCollectMetric entity) {
		try {
			String sql = "insert into dc_log_collect_metric" +
					"(code, node_ip, collect_date, hour_period, " +
					"minute_period, minute_count, minute_bytes, gmt_created, gmt_modified) " +
					"values(?, ?, ?, ?, ?, ?, ?, ?, ?)";
			return jdbcTemplate.update(sql, entity.getCode(), entity.getNodeIp(), entity.getCollectDate(),
					entity.getHourPeriod(), entity.getMinutePeriod(), entity.getMinuteCount(), entity.getMinuteBytes(),
					new Date(), new Date());
		} catch (Exception e) {
			LOGGER.error("insert into dc_log_collect_metric error: {} ", e.getMessage());
			return 0;
		}
	}
}
