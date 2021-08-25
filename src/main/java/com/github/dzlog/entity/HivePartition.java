package com.github.dzlog.entity;

import java.util.Date;

/**
 * Created by taofu on 2018/5/28.
 */
public class HivePartition {

	private Long id;

	private String dataBaseName;

	private String tableName;

	private String partitionSpec;

	private String partitionLocation;

	private Date lastAccessTime;

	private Date lastUpdateTime;

	private long totalNumFiles;

	private long totalFileSize;

	private long numRows;

	private Date gmtCreated;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getDataBaseName() {
		return dataBaseName;
	}

	public void setDataBaseName(String dataBaseName) {
		this.dataBaseName = dataBaseName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getPartitionSpec() {
		return partitionSpec;
	}

	public void setPartitionSpec(String partitionSpec) {
		this.partitionSpec = partitionSpec;
	}

	public String getPartitionLocation() {
		return partitionLocation;
	}

	public void setPartitionLocation(String partitionLocation) {
		this.partitionLocation = partitionLocation;
	}

	public Date getLastAccessTime() {
		return lastAccessTime;
	}

	public void setLastAccessTime(Date lastAccessTime) {
		this.lastAccessTime = lastAccessTime;
	}

	public Date getLastUpdateTime() {
		return lastUpdateTime;
	}

	public void setLastUpdateTime(Date lastUpdateTime) {
		this.lastUpdateTime = lastUpdateTime;
	}

	public long getTotalNumFiles() {
		return totalNumFiles;
	}

	public void setTotalNumFiles(long totalNumFiles) {
		this.totalNumFiles = totalNumFiles;
	}

	public long getTotalFileSize() {
		return totalFileSize;
	}

	public void setTotalFileSize(long totalFileSize) {
		this.totalFileSize = totalFileSize;
	}

	public long getNumRows() {
		return numRows;
	}

	public void setNumRows(long numRows) {
		this.numRows = numRows;
	}

	public Date getGmtCreated() {
		return gmtCreated;
	}

	public void setGmtCreated(Date gmtCreated) {
		this.gmtCreated = gmtCreated;
	}
}
