package com.sparkstreaming.streamingengine;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
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
        return new SparkConf().setAppName(appName).setMaster(masterUri);
    }

//    @Bean
//    public JavaSparkContext sc() {
//        return new JavaSparkContext(conf());
//    }

    @Bean
    public SparkSession sparkSession(){
        return new SparkSession(new SparkContext(conf()));
    }
}
