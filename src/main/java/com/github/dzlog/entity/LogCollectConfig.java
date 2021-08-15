package com.github.dzlog.entity;

import lombok.Data;

/**
 * Created by admin
 */
@Data
public class LogCollectConfig {

    private Long id;

    private String code;

    private String dataCenter;

    private String appName;

    private String databaseName;

    private String tableName;

    private String collectFile;

    private String kafkaCluster = "dzlog";

    private String kafkaTopic;

    private Integer flowStatus;

    private Integer runStatus;
}
