package com.sparkstreaming.streamingengine;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.AnyVal;
import scala.Option;
import scala.util.parsing.json.JSON;
import twitter4j.Status;

import java.util.*;

public class StructuredStreamingTwitter {

    public static void main(String[] args) {

        SparkSession sparkSession = new SparkSession(new SparkContext(new SparkConf().setAppName("test").setMaster("local[*]")));
        String[] filters = {"corona"};
        System.setProperty("twitter4j.oauth.consumerKey", "");
        System.setProperty("twitter4j.oauth.consumerSecret", "");
        System.setProperty("twitter4j.oauth.accessToken", "-");
        System.setProperty("twitter4j.oauth.accessTokenSecret", "");

        JavaSparkContext ctx = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
        JavaStreamingContext jssc = new JavaStreamingContext(ctx, new Duration(2000));

        JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jssc, filters);

        Set<String> topicsSet = new HashSet<>(Arrays.asList("testTopic".split(",")));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "groupId");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        JavaInputDStream<ConsumerRecord<String, String>> streaming = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topicsSet, kafkaParams));

        streaming.foreachRDD((v1, v2) -> {
            v1.foreach(objectObjectConsumerRecord -> {
                String value = objectObjectConsumerRecord.value();
                Option<Object> tweet = JSON.parseFull(value);
                scala.collection.immutable.Map<String, Object> t = (scala.collection.immutable.Map<String, Object>) tweet.get();
                System.out.println(t.get("text"));
            });
        });


//Gson gson = new Gson();
//        stream.foreachRDD((v1, v2) -> {
//
//            JavaRDD<Status> statusJavaRdd = v1.rdd().toJavaRDD();
//
//            System.out.println(statusJavaRdd.count());
//            StructType structType = Encoders.bean(StatusBean.class).schema();
//            Dataset<Row> df = sparkSession.sqlContext().read().json(jssc.sparkContext().parallelize(test.collect()));
//
//            df.show();
//            StructType structType1 = ExpressionEncoder.javaBean(StatusBean.class).schema();
//            JavaRDD<Row> rowRdd = v1.map(RowFactory::create);
//            Dataset<Row> streamDF =sparkSession.sqlContext().createDataFrame(rowRdd, structType1);
////            }
////            Dataset<Row> streamDF = ref.df;
//
//            streamDF.show(5);
        // });


        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
