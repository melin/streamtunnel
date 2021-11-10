package com.github.dzlog.support;

import com.gitee.bee.core.conf.BeeConfigClient;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.io.IOException;

import static com.github.dzlog.DzlogConf.DZLOG_DATA_CENTER_KERBEROS_PRINCIPAL;

/**
 * Created by admin
 */
@Component
public class ConfigurationLoader implements InitializingBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationLoader.class);

    @Autowired
    private Environment environment;

    @Autowired
    private BeeConfigClient configClient;

    private Configuration configuration;

    @Value("${dzlog.datacenter}")
    private String dataCenter;

    @Override
    public void afterPropertiesSet() throws Exception {
        if (StringUtils.isBlank(dataCenter)) {
            throw new IllegalArgumentException("dataCenter can not empty");
        }

        System.setProperty("HADOOP_USER_NAME", "admin");
        String configPath = "";
        try {
            String profile = "dev";
            if (ArrayUtils.contains(environment.getActiveProfiles(), "production")) {
                profile = "production";
            } else if (ArrayUtils.contains(environment.getActiveProfiles(), "test")) {
                profile = "test";
            }

            LOGGER.info("load hadoop hive hbase config, dataCenter: {}, profile: {}", dataCenter, profile);
            configPath = "datacenter/" + dataCenter + "/" + profile + "/";

            configuration = new Configuration();
            ClassPathResource hadoopResource = new ClassPathResource(configPath + "core-site.xml");
            configuration.addResource(hadoopResource.getInputStream());
            LOGGER.info("加载文件：{}", hadoopResource.getURL().getPath());

            String authentication = configuration.get("hadoop.security.authentication");
            String principal = configClient.getMapString(DZLOG_DATA_CENTER_KERBEROS_PRINCIPAL).get(dataCenter);

            if ("kerberos".equalsIgnoreCase(authentication)) {
                Assert.hasText(principal, "启动kerberos，principal 不能为空");
                LOGGER.info("kerberos 认证");
                ClassPathResource keytabFile = new ClassPathResource(configPath + "dzlog.keytab");
                if (!keytabFile.exists()) {
                    throw new IllegalArgumentException("启动kerberos，keytab文件不存在: dzlog.keytab");
                }

                ClassPathResource krb5File = new ClassPathResource(configPath + "krb5.conf");
                if (!krb5File.exists()) {
                    throw new IllegalArgumentException("启动kerberos，krb5.conf 文件不存在: krb5.conf");
                }

                try {
                    System.setProperty("java.security.krb5.conf", krb5File.getURL().getPath());
                    UserGroupInformation.setConfiguration(configuration);
                    UserGroupInformation.loginUserFromKeytab(principal, keytabFile.getURL().getPath());
                } catch (IOException e1) {
                    throw new RuntimeException("afterPropertiesSet,kerberos login error: " + e1.getMessage());
                }
            }
        } catch (Throwable e) {
            throw new RuntimeException("加载hadoop、 hive配置文件失败: " + configPath, e);
        }
    }

    public Configuration getConfiguration() {
        return configuration;
    }
}
