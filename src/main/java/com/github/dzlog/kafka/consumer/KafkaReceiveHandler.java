package com.github.dzlog.kafka.consumer;

import com.gitee.bee.core.conf.BeeConfigClient;
import com.github.dzlog.entity.LogCollectMetric;
import com.github.dzlog.kafka.LogEvent;
import com.github.dzlog.kafka.TopicConsumerInfo;
import com.github.dzlog.service.LogCollectMetricService;
import com.github.dzlog.support.DzLogContext;
import com.github.dzlog.util.CommonUtils;
import com.github.dzlog.util.HdfsUtils;
import com.github.dzlog.writer.AbstractFileWriter;
import com.github.dzlog.writer.FileWriterFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.listener.ConsumerSeekAware;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.github.dzlog.DzlogConf.DZLOG_KAFKA_COMMIT_MAX_INTERVAL_SECONDS;
import static com.github.dzlog.DzlogConf.DZLOG_KAFKA_COMMIT_MAX_NUM;

/**
 * @author admin
 */
public class KafkaReceiveHandler implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaReceiveHandler.class);

    private static final Logger TROUBLE_LOGGER = LoggerFactory.getLogger("troubleLogger");

    private Consumer consumer = null;

    private ConsumerSeekAware.ConsumerSeekCallback consumerSeekCallback;

    //spring bean start
    private BeeConfigClient configClient;

    private LogCollectMetricService collectMetricService;

    private FileWriterFactory fileWriterFactory;

    private DzLogContext dzLogContext;
    //spring bean end

    /**
     * 当前线程write
     */
    protected String currentPartition = null;

    // topic partition 对应的statusInfo
    protected Map<String, TopicConsumerInfo> partitionToTopicConsumerInfoMap = new ConcurrentHashMap<>();

    // topic partition 对应最新消息的 offset`
    protected Map<String, Long> topicPartitionOffsetMap = new ConcurrentHashMap<>();

    // topic partition 对应上一次commit的 offset`
    private Map<String, Long> lastCommitPartitionOffsetMap = new ConcurrentHashMap<>();

    protected static final ConcurrentHashMap<String, Object> CODE_LOCK_MAP = new ConcurrentHashMap<>();

    public KafkaReceiveHandler(ConsumerSeekAware.ConsumerSeekCallback consumerSeekCallback,
                                  ApplicationContext applicationContext) {
        this.consumerSeekCallback = consumerSeekCallback;
        this.configClient = applicationContext.getBean(BeeConfigClient.class);
        this.dzLogContext = applicationContext.getBean(DzLogContext.class);
        this.collectMetricService = applicationContext.getBean(LogCollectMetricService.class);
        this.fileWriterFactory = applicationContext.getBean(FileWriterFactory.class);
    }

    @Override
    public void close() throws IOException {
        topicPartitionOffsetMap.clear();
        lastCommitPartitionOffsetMap.clear();
    }

    /**
     * 初始化offset分配，分配thread partition 时调用
     * @param assignments
     */
    public void initAssignment(Map<TopicPartition, Long> assignments) {
        for (TopicPartition partition : assignments.keySet()) {
            long offset = assignments.get(partition);
            String topicPartition = partition.toString();

            topicPartitionOffsetMap.put(topicPartition, offset);
            lastCommitPartitionOffsetMap.put(topicPartition, offset);
        }
    }

    public void flushTopic(String mode, String partitionName) {
        long threadId = Thread.currentThread().getId();

        // partition 重新分配给其他线程处理。删除遗留未提交的文件。
        if (!topicPartitionOffsetMap.containsKey(partitionName)) {
            if (partitionToTopicConsumerInfoMap.containsKey(partitionName)) {
                TopicConsumerInfo topicConsumerInfo = partitionToTopicConsumerInfoMap.get(partitionName);

                Set<String> codes = topicConsumerInfo.getCollectCodes();
                for (String code : codes) {
                    AbstractFileWriter fileWriter = topicConsumerInfo.getFileWriter(code);
                    if (fileWriter == null) {
                        continue;
                    }

                    IOUtils.closeQuietly(fileWriter);
                    topicConsumerInfo.removeFileWriter(code);
                    LOGGER.info("partition 重新平衡，[{}] [{}] 被其他线程重新消费，delete file {}", code, partitionName, fileWriter.getLocalFile().toString());
                    FileUtils.deleteQuietly(fileWriter.getLocalFile().toFile());
                }
            }
            return;
        }

        if (!partitionToTopicConsumerInfoMap.containsKey(partitionName)) {
            return;
        }

        AbstractFileWriter currentWriter = null;
        try {
            TopicConsumerInfo topicConsumerInfo = partitionToTopicConsumerInfoMap.get(partitionName);
            Boolean isCommit = false;

            for (String code : topicConsumerInfo.getCollectCodes()) {
                currentWriter = topicConsumerInfo.getFileWriter(code);
                if (currentWriter == null) {
                    continue;
                }

                IOUtils.closeQuietly(currentWriter);
                topicConsumerInfo.removeFileWriter(code);

                long count = currentWriter.getCount();
                long msgBytes = currentWriter.getMsgBytes();
                if (count > 0) {
                    long times = updateLocalFile(currentWriter);
                    String file = currentWriter.getLocalFile().toString();

                    if (times > 500) {
                        LOGGER.error("[{}] thread {} prepare to flush topicPartition {} (code:{}), times: {}ms, total count: {}, file: {}",
                                mode, threadId, partitionName, code, times, count, file);
                    } else if (times > 100) {
                        LOGGER.warn("[{}] thread {} prepare to flush topicPartition {} (code:{}), times: {}ms, total count: {}, file: {}",
                                mode, threadId, partitionName, code, times, count, file);
                    } else {
                        LOGGER.info("[{}] thread {} prepare to flush topicPartition {} (code:{}), times: {}ms, total count: {}, file: {}",
                                mode, threadId, partitionName, code, times, count, file);
                    }

                    synchronized (CODE_LOCK_MAP.get(code)) {
                        LogCollectMetric entity = collectMetricService.createEntity(code, topicConsumerInfo.getCurrentHivePartition(), count, msgBytes);
                        collectMetricService.recordEntity(entity);
                    }
                    isCommit = true;
                }
            }

            if (isCommit) {
                commitTopic(partitionName);
            }
        } catch (Exception e) {
            TROUBLE_LOGGER.error("flush partitionName: " + partitionName, e);
            if (currentWriter != null) {
                rollBackTopicPartition(partitionName);
            }
        }
    }

    private void commitTopic(String topicPartition) {
        long threadId = Thread.currentThread().getId();

        Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
        long offset = topicPartitionOffsetMap.get(topicPartition);
        TopicPartition partition = CommonUtils.createTopicPartition(topicPartition);
        commits.put(partition, new OffsetAndMetadata(offset + 1));
        consumer.commitSync(commits);

        TopicConsumerInfo topicConsumerInfo = partitionToTopicConsumerInfoMap.get(topicPartition);
        topicConsumerInfo.setRecordCount(0L);
        topicConsumerInfo.setLastFlushTime(System.currentTimeMillis());
        lastCommitPartitionOffsetMap.put(topicPartition, offset + 1);

        try {
            long offsetIncrement = getOffsetIncrement(topicPartition);
            if (partitionToTopicConsumerInfoMap.containsKey(topicPartition)) {
                long recordCount = topicConsumerInfo.getRecordCount();
                if (offsetIncrement - recordCount != 0) {
                    TROUBLE_LOGGER.error("[{}] 线程{}消费数据与写入数据相差 {} 条，offsetIncrement:{}, recordCount: {}",
                            topicPartition, threadId, offsetIncrement - recordCount, offsetIncrement, recordCount);
                }
            }
        } catch (Exception e) {
            TROUBLE_LOGGER.warn("commitTopic failure : {}", e.getMessage());
        }
    }

    /**
     * 获取某个topic的offset增加量
     * @param topicPartition
     * @return
     */
    private long getOffsetIncrement(String topicPartition) {
        long currentOffset = topicPartitionOffsetMap.get(topicPartition);
        long lastOffset = lastCommitPartitionOffsetMap.getOrDefault(topicPartition, currentOffset);
        long offsetIncrement = currentOffset - lastOffset;
        return offsetIncrement + 1;
    }

    protected long updateLocalFile(AbstractFileWriter currentWriter) {
        long startTime = System.currentTimeMillis();
        Configuration remoteConf = dzLogContext.getConfiguration();

        checkForCreatePartition(currentWriter.getCode(), currentWriter.getHdfsPath().getParent());

        String localFile = currentWriter.getLocalFile().toString();
        try {
            java.nio.file.Path localFilePath = currentWriter.getLocalFile();
            File file = localFilePath.toFile();
            // 避免空文件.
            if (file.exists() && file.length() > 0) {
                HdfsUtils.putLocalFile(remoteConf, new Path(localFile), currentWriter.getHdfsPath());
            }
        } catch (Exception e) {
            LOGGER.error("上传失败，重试一次：" + localFile + ", 失败原因：" + e.getMessage());
            try {
                HdfsUtils.putLocalFile(remoteConf, new Path(localFile), currentWriter.getHdfsPath());
            } catch (Exception e1) {
                LOGGER.error("上传失败，重试一次又失败：" + localFile + ", 失败原因：" + e1.getMessage());
            }
        }

        return (System.currentTimeMillis() - startTime);
    }

    /**
     * 创建hive分区
     * @param code
     * @param partitionDir
     */
    public void checkForCreatePartition(String code, Path partitionDir) {
        synchronized (CODE_LOCK_MAP.get(code)) {
            if (!HdfsUtils.isPathExist(dzLogContext.getConfiguration(), partitionDir)) {
                HdfsUtils.mkdirs(dzLogContext.getConfiguration(), partitionDir);
            }
        }
    }

    /**
     * 服务器停止时，提交已经消费的数据
     */
    public void clearRemainTopic() {
        try {
            for (String topicPartition : partitionToTopicConsumerInfoMap.keySet()) {
                TopicConsumerInfo topicConsumerInfo = partitionToTopicConsumerInfoMap.get(topicPartition);
                for (AbstractFileWriter parquetFileWriter : topicConsumerInfo.getFileWriters()) {
                    updateLocalFile(parquetFileWriter);
                }

                commitTopic(topicPartition);
                long offset = this.lastCommitPartitionOffsetMap.get(topicPartition);
                TROUBLE_LOGGER.info("[{}] last commit offset {}", topicPartition, offset);
            }
        } catch (Exception e) {
            TROUBLE_LOGGER.error("flush remain topic error", e);
        }
    }

    /**
     * 更新topic的状态
     * @param logEvent
     */
    public void updateTopicPartition(LogEvent logEvent) {
        String topicPartition = logEvent.getTopicPartition();
        long offset = logEvent.getOffset();
        topicPartitionOffsetMap.put(topicPartition, offset);
    }

    /**
     * 追加记录至writer
     * @param logEvent
     * @return
     */
    public boolean appendEvent(LogEvent logEvent, String currentHivePartition) {
        String code = logEvent.getCode();
        String topicPartition = logEvent.getTopicPartition();
        try {
            if (!partitionToTopicConsumerInfoMap.containsKey(topicPartition)) {
                TopicConsumerInfo topicConsumerInfo = new TopicConsumerInfo(topicPartition);
                topicConsumerInfo.setCurrentHivePartition(currentHivePartition);
                partitionToTopicConsumerInfoMap.put(topicPartition, topicConsumerInfo);
                CODE_LOCK_MAP.putIfAbsent(code, new Object());
            }

            TopicConsumerInfo topicConsumerInfo = partitionToTopicConsumerInfoMap.get(topicPartition);
            topicConsumerInfo.setCurrentHivePartition(currentHivePartition);
            if (topicConsumerInfo.getFileWriter(code) == null) {
                CODE_LOCK_MAP.putIfAbsent(code, new Object());
            }

            AbstractFileWriter fileWriter = topicConsumerInfo.getFileWriter(code);
            if (fileWriter == null) {
                fileWriter = createNewWriter(code, topicPartition, currentHivePartition);
            } else {
                // 如果回滚了offset 清除本地已经写入数据
                long lastoffset = topicPartitionOffsetMap.get(topicPartition);
                if (lastoffset >= logEvent.getOffset()) {
                    if (fileWriter != null) {
                        IOUtils.closeQuietly(fileWriter);
                        topicConsumerInfo.removeFileWriter(code);

                        FileUtils.deleteQuietly(fileWriter.getLocalFile().toFile());
                        LOGGER.info("rollback msg, [{}], lastoffset: {}, currentOffset: {}, delete file {}",
                                topicPartition, lastoffset, logEvent.getOffset(), fileWriter.getLocalFile().toString());

                        fileWriter = createNewWriter(code, topicPartition, currentHivePartition);
                    }
                }
            }

            if (fileWriter != null) {
                fileWriter.write(logEvent);
                fileWriter.setLastWriteTime(System.currentTimeMillis());
                fileWriter.incrementCount();
                fileWriter.incrementMsgBytes(logEvent.getMsgBytes());

                topicConsumerInfo.incrementRecordCount();
                LOGGER.debug("append event: " + logEvent);
            }

            return true;
        } catch (Exception e) {
            TROUBLE_LOGGER.error("append event failure, topicPartition: " + topicPartition + ", code: " + code, e);
            return false;
        }
    }

    /**
     * 创建新的writer
     * @param code
     * @param topicPartition
     * @param partition
     * @throws IOException
     */
    protected AbstractFileWriter createNewWriter(String code, String topicPartition, String partition) throws IOException {
        AbstractFileWriter newWriter = fileWriterFactory.createFileWriter(code, topicPartition, partition);

        if (newWriter != null) {
            TopicConsumerInfo topicConsumerInfo = partitionToTopicConsumerInfoMap.get(topicPartition);
            topicConsumerInfo.setFileWriter(code, newWriter);
        }

        return newWriter;
    }

    /**
     * 检测是否到达提交该topic partition, 两个条件满足一个即可提交：
     * 1、消费一定数量，dzlog.kafka.commit.max.num，默认10000
     * 2、消费一定时间，dzlog.kafka.commit.max.interval.seconds, 默认30秒
     * @param topicPartition
     */
    public boolean checkTopicForCommit(String topicPartition) {
        boolean flush = false;
        TopicConsumerInfo topicConsumerInfo = partitionToTopicConsumerInfoMap.get(topicPartition);
        if (topicConsumerInfo != null) {
            long cachedCount = topicConsumerInfo.getRecordCount();
            long lastUpdateTime = topicConsumerInfo.getLastFlushTime();
            int commitMaxNum = configClient.getInteger(DZLOG_KAFKA_COMMIT_MAX_NUM);
            int commitMaxIntervalSeconds = configClient.getInteger(DZLOG_KAFKA_COMMIT_MAX_INTERVAL_SECONDS);

            boolean rollBatchCommit = commitMaxNum > 0 && cachedCount >= commitMaxNum;
            long times = System.currentTimeMillis() - lastUpdateTime;
            boolean rollTimeCommit = commitMaxIntervalSeconds > 0 && times >= (commitMaxIntervalSeconds * 1000L);

            flush = rollBatchCommit || rollTimeCommit;

            if (flush) {
                LOGGER.info("topicPartition: {}, commitMaxNum: {}, cachedCount: {}, lastUpdateTime: {}, commitMaxIntervalSeconds: {}, times: {}",
                        commitMaxNum, cachedCount, lastUpdateTime, commitMaxIntervalSeconds, times);
            }
        }

        return flush;
    }

    /**
     * 回滚某个 topicPartition
     *
     * @param topicPartition
     */
    public void rollBackTopicPartition(String topicPartition) {
        LOGGER.error("rollBack topicPartition {}", topicPartition);

        TopicPartition partition = CommonUtils.createTopicPartition(topicPartition);
        long offset = lastCommitPartitionOffsetMap.get(topicPartition);
        consumerSeekCallback.seek(partition.topic(), partition.partition(), offset);

        long threadId = Thread.currentThread().getId();
        LOGGER.info("thread {} rollback topicPartition {} to offset {}", threadId, topicPartition, offset);
    }

    public Consumer getConsumer() {
        return consumer;
    }

    public void setConsumer(Consumer consumer) {
        this.consumer = consumer;
    }

    public String getCurrentPartition() {
        return currentPartition;
    }

    public void setCurrentPartition(String currentPartition) {
        this.currentPartition = currentPartition;
    }

    public Map<String, TopicConsumerInfo> getPartitionToTopicConsumerInfoMap() {
        return partitionToTopicConsumerInfoMap;
    }

    public void setPartitionToTopicConsumerInfoMap(Map<String, TopicConsumerInfo> partitionToTopicConsumerInfoMap) {
        this.partitionToTopicConsumerInfoMap = partitionToTopicConsumerInfoMap;
    }
}
