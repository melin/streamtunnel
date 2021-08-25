package com.github.dzlog.support;

import com.github.dzlog.entity.LogCollectConfig;
import com.github.dzlog.service.HivePartitionService;
import com.github.dzlog.service.LogCollectConfigService;
import com.github.dzlog.util.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author melin 2021/8/25 8:09 下午
 */
@Service
public class HivePartitionMonitor {
    private static final Logger LOGGER = LoggerFactory.getLogger(HivePartitionMonitor.class);

    @Autowired
    private LogCollectConfigService dcLogCollectService;

    @Autowired
    private HivePartitionService hivePartitionService;

    @Scheduled(cron = "0 */15 * * * ?")
    public void repairHivePartition() {
        LOGGER.info("repair hive partition ...");

        //@TODO 增加leader 选举
        repairHivePartitionTask();
    }

    private void repairHivePartitionTask() {
        List<LogCollectConfig> configList = dcLogCollectService.queryAllCollects();
        String currentHivePartition = TimeUtils.getCurrentHivePartition();
        if (configList != null) {
            for (LogCollectConfig collectConfig : configList) {
                String partition = null;
                for (int i = 1; i <= 8; i++) {
                    try {
                        String timePartition = TimeUtils.addMinute(currentHivePartition, (-i - 1) * 15);
                        hivePartitionService.addPartitionInfo(collectConfig, timePartition);
                    } catch (Exception e) {
                        LOGGER.error("add partition {} error: {}", partition, e.getMessage());
                    }
                }
            }
        }
    }
}
