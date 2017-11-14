package com.spark.sample

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}

object SimpleApp {

  def main(args: Array[String]): Unit = {
    val logFile = new File(this.getClass.getClassLoader.getResource("README.MD").toURI).getPath
    //val logFile = "hdfs://master:9000/wordcount/README.md"
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
//    val conf = new SparkConf().setAppName("Simple Application").setMaster("spark://master:7077")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile)
    val counts = logData.flatMap(line => line.split(" "))
      .map(str => (str, 1))
      .reduceByKey(_ + _)

    counts.collect().foreach(println)
//    counts.saveAsTextFile("hdfs://master:9000/wordcount/out.txt")
  }
}
