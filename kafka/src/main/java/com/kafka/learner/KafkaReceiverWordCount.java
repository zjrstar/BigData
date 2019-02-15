package com.kafka.learner;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

public class KafkaReceiverWordCount {

  // ./bin/kafka-topics.sh --zookeeper spark001:2181,spark002:2181,spark003:2181 --topic wordcount --replication-factor 1 --partitions 1 --create
  // ./bin/kafka-console-producer.sh --topic wordcount --broker-list spark001:9092,spark002:9092,spark003:9092
  public static void main(String[] args) throws InterruptedException {
    /**
     * setMaster("local[2]"),至少要指定两个线程，一条用于接收消息，一条线程用于处理消息
     * Durations.seconds(2)每两秒读取一次kafka
     */
    SparkConf conf = new SparkConf().setAppName("wordcount").setMaster("local[2]");
    JavaStreamingContext context = new JavaStreamingContext(conf, Durations.seconds(2));

    //这个比较重要， 是对应你给topic用几个线程去拉取数据
    HashMap<String, Integer> topicThreadMap = new HashMap<>();
    topicThreadMap.put("wordcountReceiv20160618", 1);

    //kafka这种创建的流，是pair的形式，有两个值，但第一个值通常都是Null
    JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(
        context,
        "192.168.80.201:2181,192.168.80.202:2181,192.168.80.203:2181",
        "WordCountConsumerGroup",
        topicThreadMap);

    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {

      public static final long serialVersionUID = 1L;

      @Override
      public Iterator<String> call(Tuple2<String, String> tuple2) throws Exception {
        return Arrays.asList(tuple2._2.split(" ")).iterator();
      }
    });

    JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
      private static final long serialVersionUID = 1L;

      @Override
      public Tuple2<String, Integer> call(String word) throws Exception {
        return new Tuple2<String, Integer>(word, 1);
      }

    });


    JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer v1, Integer v2) throws Exception {
        return v1 + v2;
      }
    });

    wordCounts.print();

    context.start();
    context.awaitTermination();
    context.close();

  }
}





























