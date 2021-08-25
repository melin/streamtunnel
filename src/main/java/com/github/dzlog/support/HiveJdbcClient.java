package com.github.dzlog.support;

import com.gitee.bee.core.conf.BeeConfigClient;
import com.github.dzlog.util.NetUtils;
import com.github.dzlog.util.ThreadUtils;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.github.dzlog.DzlogConf.DZLOG_DATA_CENTER_KERBEROS_PRINCIPAL;
import static com.github.dzlog.DzlogConf.DZLOG_DATA_CENTER_SPARK_JDBC_URLS;

/**
 * Created by binsong.li on 2021/08/11.
 */
@Component
public class HiveJdbcClient implements InitializingBean {

    private static final Logger LOGGER = LoggerFactory.getLogger("errorLogger");

    private ExecutorService executorService = ThreadUtils.newDaemonSingleThreadScheduledExecutor("check-thrift-server-alive");

    @Autowired
    private ConfigurationLoader configurationLoader;

    @Autowired
    private BeeConfigClient configClient;

    @Value("${dzlog.datacenter}")
    private String dataCenter;

    private List<String> availableHiveUrls = Lists.newArrayList();

    @Override
    public void afterPropertiesSet() throws Exception {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch (Exception e) {
            LOGGER.error("error load hive jdbc driver:" + e.getMessage());
        }

        executorService.execute(() -> {
            while (true) {
                try {
                    String sourceHiveUrls = configClient.getMapString(DZLOG_DATA_CENTER_SPARK_JDBC_URLS).get(dataCenter);
                    String[] newHiveUrls = StringUtils.split(sourceHiveUrls, ";");
                    for (String hiveUrl : newHiveUrls) {
                        boolean alive = NetUtils.isRunning(hiveUrl);
                        if (!alive && availableHiveUrls.contains(hiveUrl)) {
                            availableHiveUrls.remove(hiveUrl);
                            LOGGER.warn("spark thrift server {} is unavailable， deleted", hiveUrl);
                        } else if (alive && !availableHiveUrls.contains(hiveUrl)) {
                            availableHiveUrls.add(hiveUrl);
                            LOGGER.warn("spark thrift server {} is available, added", hiveUrl);
                        }
                    }

                    Iterator<String> iterator = availableHiveUrls.iterator();
                    while (iterator.hasNext()) {
                        String oldUrl = iterator.next();
                        if (!ArrayUtils.contains(newHiveUrls, oldUrl)) {
                            iterator.remove();
                            LOGGER.warn("spark thrift server {} is unavailable， deleted", oldUrl);
                        }
                    }
                } catch (Throwable e) {
                    LOGGER.error(e.getMessage());
                } finally {
                    try {
                        TimeUnit.SECONDS.sleep(3);
                    } catch (Exception e) {}
                }
            }
        });
    }

    public Boolean addPartition(String tableName, String partition) {
        Connection conn = null;
        Statement stmt = null;
        String url = getSparkJdbcUrl();
        try {
            conn = DriverManager.getConnection(url, "dclog", "dclog");
            stmt = conn.createStatement();

            StringBuilder sb = new StringBuilder("ALTER TABLE ");
            sb.append(tableName).append(" ADD PARTITION (").append(partition).append(")");
            stmt.execute(sb.toString());

            LOGGER.info("add partition {} for table {}", partition, tableName);
            return true;
        } catch (Exception e) {
            LOGGER.error("hive url: " + url + ", add partition for table" + tableName , e);
            return false;
        } finally {
            JdbcUtils.closeConnection(conn);
            JdbcUtils.closeStatement(stmt);
        }
    }

    public Boolean repairTable(String tableName) {
        Connection conn = null;
        Statement stmt = null;
        try {
            String url = getSparkJdbcUrl();
            conn = DriverManager.getConnection(url, "dclog", "dclog");
            stmt = conn.createStatement();
            String sql = "MSCK REPAIR TABLE " + tableName;
            LOGGER.info("msck repair table: {}", sql);
            stmt.execute(sql);

            return true;
        } catch (Exception e) {
            LOGGER.error("repair table {} error: {}", tableName, e.getMessage());
            return false;
        } finally {
            JdbcUtils.closeConnection(conn);
            JdbcUtils.closeStatement(stmt);
        }
    }

    private String getSparkJdbcUrl() {
        String hiveUrl = availableHiveUrls.get(new Random().nextInt(availableHiveUrls.size()));
        hiveUrl = getKerberosedUrl(hiveUrl);

        if (StringUtils.isNotBlank(hiveUrl)) {
            return "jdbc:hive2://" + hiveUrl;
        } else {
            throw new RuntimeException("no available thriftserver");
        }
    }

    public String getKerberosedUrl(String hiveUrl) {
        String authentication = configurationLoader.getConfiguration().get("hadoop.security.authentication");
        String principal = configClient.getMapString(DZLOG_DATA_CENTER_KERBEROS_PRINCIPAL).get(dataCenter);

        if ("kerberos".equalsIgnoreCase(authentication)){
            if (principal.length() > 0) {
                String[] items = StringUtils.split(hiveUrl, ":");
                if (items.length == 2) {
                    principal = StringUtils.replace(principal, "_HOST", items[0]);
                }
                hiveUrl = hiveUrl + "/;principal=" + principal;
            }
        }

        return hiveUrl;
    }
}
