package com.github.dzlog.support.jmx;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectInstance;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.regex.Pattern;

/**
 * Created by admin on 2020/5/7 6:29 下午
 */
public abstract class AbstractShellTool {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractShellTool.class);

    private static final Pattern INTEGER_PATTERN = Pattern.compile("-?[0-9]+");

    protected static void setLogLevel(Level logLevel) {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();

        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger)
                LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
        root.setLevel(logLevel);
    }

    public Object doMain(String methodName) throws Exception {
        String host = getSystemValue("host", "127.0.0.1");
        int port = getSystemInt("port", 4002);

        JMXClient jmxClient = JMXClient.getJMXClient(host, port);
        LOGGER.info("connected to " + jmxClient.getAddressAsString());
        ObjectInstance brokerInstance = jmxClient.queryMBeanForOne(this.getBrokerName());

        Object result = null;
        if (brokerInstance != null) {
            result = jmxClient.invoke(brokerInstance.getObjectName(), methodName, new Object[0], new String[0]);
            jmxClient.close();
            LOGGER.debug("invoke " + brokerInstance.getClassName() + "#" + methodName + " success");
        } else {
            LOGGER.warn("没有找到 " + this.getBrokerName());
        }
        return result;
    }

    public abstract String getBrokerName();

    private String getSystemValue(String key) {
        return getSystemValue(key, null);
    }

    private String getSystemValue(final String key, String def) {
        if (key == null) {
            throw new NullPointerException("key");
        }
        if (key.isEmpty()) {
            throw new IllegalArgumentException("key must not be empty.");
        }

        String value = null;
        try {
            if (System.getSecurityManager() == null) {
                value = System.getProperty(key);
            } else {
                value = AccessController.doPrivileged(new PrivilegedAction<String>() {
                    @Override
                    public String run() {
                        return System.getProperty(key);
                    }
                });
            }
        } catch (Exception e) {
            LOGGER.warn("Unable to retrieve a system property '" + key + "'; default values will be used.", e);
        }

        if (value == null) {
            return def;
        }

        return value;
    }

    private int getSystemInt(String key, int def) {
        String value = getSystemValue(key);
        if (value == null) {
            return def;
        }

        value = value.trim().toLowerCase();
        if (INTEGER_PATTERN.matcher(value).matches()) {
            try {
                return Integer.parseInt(value);
            } catch (Exception e) {
                // Ignore
            }
        }

        LOGGER.warn("Unable to parse the integer system property '" + key + "':" + value + " - " +
                "using the default value: " + def);

        return def;
    }
}
