package com.finityx.common;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class SparkStreamingContext<TConfig extends BaseConfig> {

    private static Logger logger = Logger.getLogger("com.finityx.logging.AppLog");

    protected final SparkConf conf;

    protected final TConfig config;
    protected final SparkSession sparkSession;

    public SparkStreamingContext(String appName, String master, String configPath, BaseConfigProvider configProvider) throws IOException {

        this.conf = new SparkConf().setAppName(appName).setMaster(master);
        this.sparkSession = SparkSession.builder().config(conf).getOrCreate();

        logger.info(String.format("Created spark App. Application Name: %s | Application Id: %s",
                appName,
                sparkSession.sparkContext().applicationId()));

        this.config = (TConfig) configProvider.create(loadConfig(configPath));
    }

    //For local config file only
    private Properties loadConfig(String configPath) throws IOException {
        Properties props = new Properties();
        FileInputStream ip = new FileInputStream(configPath);
        props.load(ip);
        return props;
    }

    void process() {
        processFrame();
        this.sparkSession.stop();
    }

    protected void processFrame() {
    }
}
