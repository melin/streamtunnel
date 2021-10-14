package com.github.dzlog.writer;

import com.github.dzlog.entity.LogCollectConfig;
import com.github.dzlog.service.LogCollectConfigService;
import com.github.dzlog.util.ThreadUtils;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.*;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;

/**
 * Created by libinsong on 2019/5/16
 */
@Service
public class CollectSchemaCache implements InitializingBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(CollectSchemaCache.class);

    private static final MessageType DCLOG_SCHEMA = new MessageType("dzlog",
            new PrimitiveType(OPTIONAL, BINARY, "message"),
            new PrimitiveType(OPTIONAL, INT96, "collect_time"));

    @Autowired
    private LogCollectConfigService collectConfigService;

    private final ConcurrentMap<String, SimpleGroupFactory> groupFactoryCache = Maps.newConcurrentMap();

    private final ConcurrentMap<String, MessageType> schemaCache = Maps.newConcurrentMap();

    private final ConcurrentMap<String, Configuration> configurationCache = Maps.newConcurrentMap();

    private final SimpleGroupFactory defaultGroupFactory = new SimpleGroupFactory(DCLOG_SCHEMA);

    private final Configuration defaultConfiguration = new Configuration(false);

    private final ThreadFactory threadFactory = ThreadUtils.namedThreadFactory("update-schema");

    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(threadFactory);

    @Override
    public void afterPropertiesSet() throws Exception {
        GroupWriteSupport.setSchema(DCLOG_SCHEMA, defaultConfiguration);

        LOGGER.info("init collect schema");
        initCollectSchema(true);
        executorService.scheduleWithFixedDelay(() -> initCollectSchema(false), 60, 60, TimeUnit.SECONDS);
    }

    private void initCollectSchema(boolean first) {
        try {
            List<LogCollectConfig> collectList = collectConfigService.queryAllCollects();

            if (first) {
                LOGGER.info("init collect schema: " + collectList.size());
            }

            for (int i = 0, len = collectList.size(); i < len; i++) {
                LogCollectConfig collect = collectList.get(i);

                String code = StringUtils.trim(collect.getCode());

                if (!schemaCache.containsKey(code)) {
                    groupFactoryCache.put(code, defaultGroupFactory);
                    schemaCache.put(code, DCLOG_SCHEMA);
                    configurationCache.put(code, defaultConfiguration);

                    LOGGER.info("collect code: {} first init common schema", code);
                }
            }

            if (first) {
                LOGGER.info("groupFactoryCache: " + StringUtils.join(groupFactoryCache.keySet(), ","));
                LOGGER.info("schemaCache: " + StringUtils.join(schemaCache.keySet(), ","));
                LOGGER.info("configurationCache: " + StringUtils.join(configurationCache.keySet(), ","));
            }
        } catch (Exception e) {
            LOGGER.error("initCollectSchema error", e);
        }
    }

    public SimpleGroupFactory getGroupFactory(String collectCode) {
        return groupFactoryCache.get(collectCode);
    }

    public Configuration getConfiguration(String collectCode) {
        return configurationCache.get(collectCode);
    }

}
