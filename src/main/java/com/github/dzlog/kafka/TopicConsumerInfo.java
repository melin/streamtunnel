package com.github.dzlog.kafka;

import com.github.dzlog.writer.AbstractFileWriter;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by admin
 */
public class TopicConsumerInfo {

	private String collectCode;

	private String currentHivePartition;

	private long recordCount = 0;

	private long lastFlushTime = System.currentTimeMillis();

	/**
	 * 采集code -> AbstractFileWriter
	 */
	private Map<String, AbstractFileWriter> fileWriterMap = new HashMap<>();

	public void incrementRecordCount() {
        recordCount++;
    }

	public String getCurrentHivePartition() {
		return currentHivePartition;
	}

	public void setCurrentHivePartition(String currentHivePartition) {
		this.currentHivePartition = currentHivePartition;
	}

	public long getRecordCount() {
		return recordCount;
	}

	public void setRecordCount(long recordCount) {
		this.recordCount = recordCount;
	}

	public long getLastFlushTime() {
		return lastFlushTime;
	}

	public void setLastFlushTime(long lastFlushTime) {
		this.lastFlushTime = lastFlushTime;
	}

	public Set<String> getCollectCodes() {
		return fileWriterMap.keySet();
	}

	public Collection<AbstractFileWriter> getFileWriters() {
		return fileWriterMap.values();
	}

	public AbstractFileWriter getFileWriter(String code) {
		return fileWriterMap.get(code);
	}

	public void setFileWriter(String code, AbstractFileWriter fileWriter) {
		fileWriterMap.put(code, fileWriter);
	}

	public void removeFileWriter(String code) {
		fileWriterMap.remove(code);
	}

	public String getCollectCode() {
		return collectCode;
	}

	public void setCollectCode(String collectCode) {
		this.collectCode = collectCode;
	}
}
