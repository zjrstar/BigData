package com.kafka.learner;


import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

public class KafkaDirectWordCount {

  public static void main(String[] args) throws InterruptedException {
    /**
     * setMaster("local[2]"),至少要指定两个线程，一条用于接收消息，一条线程用于处理消息
     * Durations.seconds(2)每两秒读取一次kafka
     */
    SparkConf conf = new SparkConf().setAppName("wordcount").setMaster("local[2]");
    JavaStreamingContext context = new JavaStreamingContext(conf, Durations.seconds(2));

    //首先要创建一个kafka参数map
    HashMap<String, String> kafkaParams = new HashMap<String, String>();
    //我们这里是不需要zookeeper节点时间，所以我们这里放broker.list
    kafkaParams.put("metadata.broker.list", "192.168.80.201:9092,192.168.80.202:9092,192.168.80.203:9092");

    HashSet<String> topics = new HashSet<>();
    topics.add("wordcountDirect20160618");

    //kafka这种创建的流，是pair的形式，有两个值，但第一个值通常都是Null啊
    JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(
        context,
        String.class, //key类型
        String.class, //value类型
        StringDecoder.class, //解码器
        StringDecoder.class,
        kafkaParams,
        topics);

    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {

      public static final long serialVersionUID = 1L;

      @Override
      public Iterator<String> call(Tuple2<String, String> tuple2) throws Exception {
        return Arrays.asList(tuple2._2.split(" ")).iterator();
      }
    });

    JavaPairDStream<String, Integer> pair = words.mapToPair(new PairFunction<String, String, Integer>() {

      private static final long serialVersionUID = 1L;

      @Override
      public Tuple2<String, Integer> call(String word) throws Exception {
        return new Tuple2<String, Integer>(word, 1);
      }
    });

    JavaPairDStream<String, Integer> wordcounts = pair.reduceByKey(new Function2<Integer, Integer, Integer>() {

      private static final long serialVersionUID = 1L;

      @Override
      public Integer call(Integer v1, Integer v2) throws Exception {
        return v1 + v2;
      }
    });

    wordcounts.print();

    context.start();
    context.awaitTermination();
    context.close();
  }
}





























