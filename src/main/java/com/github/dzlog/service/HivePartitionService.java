package com.github.dzlog.service;

import com.github.dzlog.entity.HivePartition;
import com.github.dzlog.entity.LogCollectConfig;
import com.github.dzlog.support.ConfigurationLoader;
import com.github.dzlog.util.HdfsUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;

/**
 * Created by taofu on 2018/5/28.
 */
@Service
public class HivePartitionService {

	private static final Logger LOGGER = LoggerFactory.getLogger(HivePartitionService.class);

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Autowired
	private ConfigurationLoader configurationLoader;

	public boolean checkPartitionExists(LogCollectConfig collectConfig, String partitionSpec) {
		try {
			String basePath = "/user/hive/warehouse/" + collectConfig.getDatabaseName() + ".db/" + collectConfig.getTableName();
			String partition = basePath + "/ds=" + partitionSpec;
			Path partitionPath = new Path(partition);
			FileSystem fileSystem = FileSystem.get(configurationLoader.getConfiguration());
			return fileSystem.exists(partitionPath);
		} catch (Exception e) {
			LOGGER.warn("检测分区路径是否存在失败: {}", e.getMessage());
			return false;
		}
	}

	public void recordInfoToPartitionTable(LogCollectConfig collectConfig, String partitionSpec) {
		try {
			String basePath = "/user/hive/warehouse/" + collectConfig.getDatabaseName() + ".db/" + collectConfig.getTableName();
			String partition = basePath + "/ds=" + partitionSpec;

			List<HivePartition> list = this.getPartitionEntity(collectConfig.getDatabaseName(),
					collectConfig.getTableName(), partitionSpec);
			if (list != null && list.size() > 0) {
				LOGGER.info("分区信息已经存在： {}", partitionSpec);
				return;
			}

			Configuration configuration = configurationLoader.getConfiguration();
			Triple<Long, Long, Long> tuple3 = HdfsUtils.getPartitionStatus(configuration, partition);
			if (tuple3 != null) {
				String defaultFS = configuration.get("fs.defaultFS", "hdfs://nameservice1");
				Date date = new Date();
				HivePartition hivePartition = new HivePartition();
				hivePartition.setDataBaseName(collectConfig.getDatabaseName());
				hivePartition.setTableName(collectConfig.getTableName());
				hivePartition.setPartitionSpec("ds=" + partitionSpec);
				hivePartition.setPartitionLocation(defaultFS + partition);
				hivePartition.setLastAccessTime(date);
				hivePartition.setLastUpdateTime(date);
				hivePartition.setTotalNumFiles(tuple3.getLeft());
				hivePartition.setTotalFileSize(tuple3.getMiddle());
				hivePartition.setNumRows(tuple3.getRight());
				hivePartition.setGmtCreated(date);
				this.insertEntity(hivePartition);
			} else {
				LOGGER.info("文件路径不存在：{}", partition);
			}
		} catch (Exception e) {
			LOGGER.error("insert into table {} partition info error: {}",
					collectConfig.getDatabaseName() + "." + collectConfig.getTableName(), e.getMessage());
		}
	}

	private RowMapper<HivePartition> rowMapper = (resultSet, i) ->{
		HivePartition entity = new HivePartition();
		entity.setId(resultSet.getLong("id"));
		entity.setDataBaseName(resultSet.getString("database_name"));
		entity.setTableName(resultSet.getString("table_name"));
		entity.setPartitionSpec(resultSet.getString("partition_spec"));
		entity.setPartitionLocation(resultSet.getString("location"));
		entity.setLastAccessTime(resultSet.getDate("last_access_time"));
		entity.setLastUpdateTime(resultSet.getDate("last_update_time"));
		entity.setTotalNumFiles(resultSet.getLong("total_number_files"));
		entity.setTotalFileSize(resultSet.getLong("total_file_size"));
		entity.setNumRows(resultSet.getLong("num_rows"));
		entity.setGmtCreated(resultSet.getDate("gmt_created"));
		return entity;
	};

	public int insertEntity(HivePartition entity) {
		try {
			String codecName = "zstd";
			String sql = "insert into dc_table_partition " +
					"(database_name, `table_name`, partition_spec, location, last_access_time, " +
					"last_update_time, total_number_files, total_file_size, num_rows, gmt_created, compression) " +
					"values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			return jdbcTemplate.update(sql, entity.getDataBaseName(), entity.getTableName(), entity.getPartitionSpec(),
					entity.getPartitionLocation(), entity.getLastAccessTime(), entity.getLastUpdateTime(),
					entity.getTotalNumFiles(), entity.getTotalFileSize(), entity.getNumRows(), new Date(), codecName);
		} catch (Exception e) {
			LOGGER.error("insert into t_table_partition error: {} ", e.getMessage());
			return 0;
		}
	}

	public int updateEntity(HivePartition entity) {
		try {
			String sql = "update dc_table_partition set total_number_files = ?, total_file_size = ?, num_rows = ? " +
					" where id = ?";
			return jdbcTemplate.update(sql, entity.getTotalNumFiles(), entity.getTotalFileSize(), entity.getNumRows(), entity.getId());
		} catch (Exception e) {
			LOGGER.error("insert into t_table_partition error: {} ", e.getMessage());
			return 0;
		}
	}

	public List<HivePartition> getPartitionEntity(String dbName, String tableName, String partition) {
		try {
			String sql = "select * from dc_table_partition where database_name=? and `table_name`=? and partition_spec=?";
			return jdbcTemplate.query(sql, new Object[]{dbName, tableName, partition}, rowMapper);
		} catch (Exception e) {
			LOGGER.error("query t_table_partition error: {}", e.getMessage());
			return null;
		}
	}
}
