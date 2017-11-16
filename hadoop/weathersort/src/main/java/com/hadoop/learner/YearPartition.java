package com.hadoop.learner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class YearPartition extends Partitioner<YearTemperatureKeyPair, Text> {

  //自定义分区方法
  //num是reduce的数量
  @Override
  public int getPartition(YearTemperatureKeyPair key, Text text, int num) {
    //按照年份分区，年份保存在key中
    //年份乘以常数，再对num取模
    return (key.getYear() * 127) % num;
  }
}
