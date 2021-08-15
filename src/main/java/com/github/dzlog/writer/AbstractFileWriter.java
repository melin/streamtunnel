package com.github.dzlog.writer;

import com.github.dzlog.entity.LogCollectConfig;
import com.github.dzlog.kafka.LogEvent;
import com.github.dzlog.util.CommonUtils;
import com.github.dzlog.util.NetUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.util.Assert;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author melin
 */
public abstract class AbstractFileWriter implements Closeable {

    private static AtomicLong fileIndex = new AtomicLong(System.currentTimeMillis() / 1000);

    private Configuration configuration;

    private LogCollectConfig collectConfig;

    private String hivePartition;

    private String fileName;

    private java.nio.file.Path localFile;

    private Path hdfsPath;

    private long currentIp2Long;

    /**
     * 上次写入的时间
     */
    private long lastWriteTime = System.currentTimeMillis();

    /**
     * 当前writer缓存的记录数量
     */
    private long count = 0L;

    /**
     * 写入消息总大小
     */
    private long msgBytes = 0L;

    public AbstractFileWriter(Configuration configuration, LogCollectConfig collectConfig, String hivePartition) {
        this.configuration = configuration;
        String currentIp = NetUtils.determineIpAddress();
        this.currentIp2Long = CommonUtils.ipToLong(currentIp);
        this.fileName = createFileName();
        this.collectConfig = collectConfig;
        this.hivePartition = hivePartition;
        this.localFile = createLocalFile(collectConfig);
        this.hdfsPath = createHdfsFile(collectConfig);
    }

    protected Path createHdfsFile(LogCollectConfig collectConfig) {
        String defaultFS = configuration.get("fs.defaultFS");
        Assert.hasText(defaultFS, "defaultFS 不能为空");

        String hdfsDir = defaultFS + "/user/hive/warehouse/" + collectConfig.getDatabaseName() + ".db/"
                + collectConfig.getTableName() + "/ds=" + hivePartition;
        String hdfsFile = hdfsDir + "/" + fileName;

        return new Path(hdfsFile);
    }

    protected java.nio.file.Path createLocalFile(LogCollectConfig collectConfig) {
        try {
            //本地临时目录
            String currentUsersHomeDir = System.getProperty("user.home");
            StringBuilder result = new StringBuilder(currentUsersHomeDir + "/warehouse/");
            result.append(collectConfig.getDatabaseName()).append(".db/").append(collectConfig.getTableName());
            result.append("/ds=").append(hivePartition).append("/");
            File partitionDir = new File(result.toString());

            FileUtils.forceMkdir(partitionDir);
            result.append(fileName);
            return Paths.get(result.toString());
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private String createFileName() {
        StringBuilder result = new StringBuilder();
        result.append("m-dzlog-");
        result.append(currentIp2Long).append("-");
        result.append("t").append(Thread.currentThread().getId()).append("-");
        result.append(fileIndex.getAndIncrement());
        result.append(".zstd");
        result.append(".parquet");
        return result.toString();
    }

    public abstract void write(LogEvent logEvent) throws IOException;

    public void incrementCount() {
        this.count++;
    }

    public void incrementMsgBytes(int msgBytes) {
        this.msgBytes = this.msgBytes + msgBytes;
    }

    public void setLastWriteTime(long lastWriteTime) {
        this.lastWriteTime = lastWriteTime;
    }

    public long getCount() {
        return count;
    }

    public long getMsgBytes() {
        return msgBytes;
    }

    public long getLastWriteTime() {
        return lastWriteTime;
    }

    public LogCollectConfig getCollectConfig() {
        return collectConfig;
    }

    public java.nio.file.Path getLocalFile() {
        return localFile;
    }

    public Path getHdfsPath() {
        return hdfsPath;
    }

    public String getCode() {
        return collectConfig.getCode();
    }
}
