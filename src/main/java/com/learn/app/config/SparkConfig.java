package com.learn.app.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {

    @Value("${spark.app.name}")
    private String appName;
    @Value("${spark.master}")
    private String masterUri;

    @Bean
    public SparkConf conf() {
        return new SparkConf().setAppName(appName).setSparkHome("/usr/local/Cellar/apache-spark/3.5.4").setMaster(masterUri);
    }

    @Bean
    public JavaSparkContext sc(SparkConf sc) {
        return new JavaSparkContext(sc);
    }
}
