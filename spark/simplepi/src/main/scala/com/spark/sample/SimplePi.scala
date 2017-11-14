package com.spark.sample

import java.lang.Math.random

import org.apache.spark.{SparkConf, SparkContext}

object SimplePi {

  def main(args: Array[String]): Unit = {
    val n = if (args.length <= 0 ) 1000000000 else args(0).toInt

    val conf = new SparkConf().setAppName("Simple Pi").setMaster("local")
    val sc = new SparkContext(conf)

    val seq = 0 until n

    val count = sc.parallelize(seq).map(s => {
      val x = 2 * random - 1
      val y = 2 * random - 1
      if (x*x + y*y <= 1) 1 else 0
    }).reduce(_ + _)

    println( 4.0 * count / (n-1))
  }

}
