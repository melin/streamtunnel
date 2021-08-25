package com.github.dzlog.support.jmx;

import ch.qos.logback.classic.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by admin on 2020/5/7 6:35 下午
 */
public class StopServerTool extends AbstractShellTool {
    private static final Logger LOGGER = LoggerFactory.getLogger(StopServerTool.class);

    public static void main(String[] args) throws Exception {
        setLogLevel(Level.INFO);

        LOGGER.info("shutdown server starting...");
        try {
            new StopServerTool().doMain("shutdown");
            LOGGER.info("shutdown server finished");
        } catch (Exception e) {
            LOGGER.error("关闭服务失败: " + e.getMessage());
        }
    }

    @Override
    public String getBrokerName() {
        return "org.springframework.boot:type=Admin,name=SpringApplication";
    }
}
