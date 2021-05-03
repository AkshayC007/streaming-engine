package com.sparkstreaming.streamingengine;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import scala.Tuple2;
import twitter4j.Status;

import java.util.Arrays;

@Service
public class StreamingService {

//    @Autowired
//    JavaSparkContext javaSparkContext;

    @Autowired
    SparkSession sparkSession;

    @Value("${twitter4j.oauth.accessToken}")
    public String accessToken;

    @Value("${twitter4j.oauth.accessTokenSecret}")
    public String accessTokenSecret;

    @Value("${twitter4j.oauth.consumerKey}")
    public String consumerKey;

    @Value("${twitter4j.oauth.consumerSecret}")
    public String consumerSecret;

    public void wordCount() throws StreamingQueryException {

//        Dataset<Row> df = sparkSession
//                .readStream()
//                .format("kafka")
//                .option("kafka.bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094")
//                .option("subscribe", "testTopic")
//                .load();
//        df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
//
//        df.writeStream()
//                .format("console")
//                .start().awaitTermination();

        String[] filters = {"corona"};
        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
        System.setProperty("twitter4j.oauth.accessToken", accessToken);
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

        JavaSparkContext ctx = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
        JavaStreamingContext jssc = new JavaStreamingContext(ctx, new Duration(2000));

        JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jssc, filters);

        JavaDStream<String> words = stream.flatMap(status -> Arrays.stream(status.getText().split(" ")).iterator());

        JavaDStream<String> hash = words.filter(s -> s.startsWith("#"));


        JavaPairDStream<String, Integer> hashPair = hash.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> finalPair = hashPair.reduceByKeyAndWindow(Integer::sum, new Duration(10000));

        // finalPair = finalPair.mapToPair(Tuple2::swap).transformToPair((v1, v2) -> v1.sortByKey(false)).mapToPair(Tuple2::swap);
//        finalPair.foreachRDD((stringIntegerJavaPairRDD, time) -> stringIntegerJavaPairRDD.foreach(stringIntegerTuple2 -> System.out.println(stringIntegerTuple2._1+"\t"+stringIntegerTuple2._2)));

        StructType schema = DataTypes.createStructType(new StructField[]{DataTypes.createStructField("Hashtag", DataTypes.StringType, true), DataTypes.createStructField("Count", DataTypes.IntegerType, true)});
        finalPair.foreachRDD((v1, v2) -> {
            System.out.println(v2);
            JavaRDD<Row> rowRdd = v1.map(v11 -> RowFactory.create(v11._1, v11._2));
            Dataset<Row> msgDataFrame = sparkSession.sqlContext().createDataFrame(rowRdd, schema);
            msgDataFrame.sort(functions.desc("Count")).show();
        });

//        finalPair.print(15);
        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

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
