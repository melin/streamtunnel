package com.github.dzlog.support;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.support.http.StatViewServlet;
import com.alibaba.druid.support.http.WebStatFilter;
import com.gitee.melin.bee.core.conf.BeeConfigClient;
import com.gitee.melin.bee.core.enums.mvc.IntegerToEnumConverterFactory;
import com.gitee.melin.bee.core.enums.mvc.StringToEnumConverterFactory;
import com.github.dzlog.DzlogConf;
import org.apache.commons.lang3.ArrayUtils;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.MessageSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.support.ReloadableResourceBundleMessageSource;
import org.springframework.core.annotation.Order;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.core.env.Environment;
import org.springframework.format.FormatterRegistry;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.servlet.resource.ResourceUrlEncodingFilter;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;

import static org.springframework.core.Ordered.HIGHEST_PRECEDENCE;

/**
 * Created by admin
 */
@Configuration
public class WebConfig implements WebMvcConfigurer {

    @Autowired
    private Environment environment;

    @Value("${spring.application.name}")
    private String appName;

    @Primary //默认数据源
    @Bean(name = "dataSource", destroyMethod = "close")
    @ConfigurationProperties(prefix = "spring.datasource.primary")
    public DruidDataSource druidDataSource() {
        DruidDataSource druidDataSource = new DruidDataSource();
        return druidDataSource;
    }

    @Bean
    public ResourceUrlEncodingFilter resourceUrlEncodingFilter() {
        return new ResourceUrlEncodingFilter();
    }

    @Override
    public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
        Jackson2ObjectMapperBuilder builder = new Jackson2ObjectMapperBuilder();
        builder.dateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
        converters.add(new MappingJackson2HttpMessageConverter(builder.build()));
    }

    @Override
    public void addFormatters(FormatterRegistry registry) {
        registry.addConverterFactory(new IntegerToEnumConverterFactory());
        registry.addConverterFactory(new StringToEnumConverterFactory());
    }

    @Bean
    public DefaultConversionService conversionService() {
        DefaultConversionService conversionService = new DefaultConversionService();
        conversionService.addConverterFactory(new IntegerToEnumConverterFactory());
        conversionService.addConverterFactory(new StringToEnumConverterFactory());
        return conversionService;
    }

    @Bean
    public ServletRegistrationBean druidServlet() {
        ServletRegistrationBean reg = new ServletRegistrationBean(new StatViewServlet(), "/druid/*");
        reg.addInitParameter("resetEnable", "false");
        reg.addInitParameter("loginUsername", "dataworker");
        reg.addInitParameter("loginPassword", "dataworker2020");
        return reg;
    }

    @Bean
    public FilterRegistrationBean filterRegistrationBean() {
        FilterRegistrationBean filterRegistrationBean = new FilterRegistrationBean();
        filterRegistrationBean.setFilter(new WebStatFilter());
        filterRegistrationBean.addUrlPatterns("/*");
        filterRegistrationBean.addInitParameter("exclusions", "*.js,*.gif,*.jpg,*.png,*.css,*.ico,/druid/*");
        return filterRegistrationBean;
    }

    @Bean
    public MessageSource messageSource() {
        Locale.setDefault(Locale.SIMPLIFIED_CHINESE);
        ReloadableResourceBundleMessageSource messageSource = new ReloadableResourceBundleMessageSource();
        messageSource.addBasenames("classpath:org/springframework/security/messages");
        return messageSource;
    }

    @Bean("configClient")
    @Order(HIGHEST_PRECEDENCE)
    public BeeConfigClient buildBeeConfigClient(JdbcTemplate jdbcTemplate) {
        String profile = "";
        if (ArrayUtils.contains(environment.getActiveProfiles(), "dev")) {
            profile = "dev";
        } else if (ArrayUtils.contains(environment.getActiveProfiles(), "test")) {
            profile = "test";
        } else if (ArrayUtils.contains(environment.getActiveProfiles(), "production")) {
            profile = "production";
        } else {
            throw new IllegalArgumentException("profile 值不正确");
        }

        String sql = "SELECT config_text FROM dc_config where appname='dzlog' and profile='" + profile + "'";
        BeeConfigClient configClient = new BeeConfigClient(jdbcTemplate, sql, DzlogConf.class, "dzlog.");
        return configClient;
    }

    @Bean(destroyMethod = "shutdown")
    public LeaderElection leaderElection(RedissonClient redissonClient) {
        LeaderElection leaderElection = new LeaderElection();
        leaderElection.setRedissonClient(redissonClient);
        leaderElection.tryHold("leader-lock-" + appName);
        return leaderElection;
    }
}
