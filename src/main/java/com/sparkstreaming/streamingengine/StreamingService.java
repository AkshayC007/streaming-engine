package com.sparkstreaming.streamingengine;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class StreamingService {

//    @Autowired
//    JavaSparkContext javaSparkContext;

    @Autowired
    SparkSession sparkSession;

    public  void wordCount() throws StreamingQueryException {

        Dataset<Row> df = sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094")
                .option("subscribe", "testTopic")
                .load();
        df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

        df.writeStream()
                .format("console")
                .start().awaitTermination();

    }


    public static void main(String[] args) throws StreamingQueryException {

        SparkSession sparkSession = new SparkSession(new SparkContext(new SparkConf().setAppName("appName").setMaster("local[*]")));
        Dataset<Row> df = sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094")
                .option("subscribe", "testTopic")
                .load();
        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

        df.writeStream()
                .format("console")
                .start().awaitTermination();



    }
}
