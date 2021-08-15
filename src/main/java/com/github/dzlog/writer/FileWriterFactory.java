package com.github.dzlog.writer;

import com.github.dzlog.entity.LogCollectConfig;
import com.github.dzlog.service.LogCollectConfigService;
import com.github.dzlog.util.CommonUtils;
import com.github.dzlog.util.NetUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

/**
 * @author melin 2021/7/21 10:28 下午
 */
@Service
public class FileWriterFactory implements InitializingBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileWriterFactory.class);

    @Autowired
    private LogCollectConfigService collectConfigService;

    @Autowired
    private CollectSchemaCache collectSchemaCache;

    private long currentIp2Long;

    @Override
    public void afterPropertiesSet() throws Exception {
        String currentIp = NetUtils.determineIpAddress();
        currentIp2Long = CommonUtils.ipToLong(currentIp);
    }

    public AbstractFileWriter createFileWriter(String code,
                                                String topicPartition,
                                                String hivePartition) throws IOException {

        return createParquetFileWriter(code, topicPartition, hivePartition);
    }

    private AbstractFileWriter createParquetFileWriter(String code,
                                                       String topicPartition,
                                                       String hivePartition) throws IOException {
        long threadId = Thread.currentThread().getId();

        LogCollectConfig collect = collectConfigService.getDcLogPathByCode(code);
        if (collect == null) {
            throw new RuntimeException("can not get entity about topic: " + code);
        }

        try {
            SimpleGroupFactory groupFactory = collectSchemaCache.getGroupFactory(code);
            Configuration configuration = collectSchemaCache.getConfiguration(code);

            if (configuration == null || groupFactory == null) {
                LOGGER.warn(code + " 停止采集");
                return null;
            }

            ParquetFileWriter writer = new ParquetFileWriter(groupFactory, configuration, collect, hivePartition);

            LOGGER.info("thread {} create new writer for topic {} at file {}", threadId, topicPartition, writer.getLocalFile());
            return writer;
        } catch (Exception e) {
            LOGGER.error("创建parquet file writer 失败" + e.getMessage(), e);
            return null;
        }
    }
}
