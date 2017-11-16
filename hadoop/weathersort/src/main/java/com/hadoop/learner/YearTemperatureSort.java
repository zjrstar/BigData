package com.hadoop.learner;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class YearTemperatureSort extends WritableComparator {

  public YearTemperatureSort() {
    super(YearTemperatureKeyPair.class, true);
  }

  @Override
  public int compare(WritableComparable a, WritableComparable b) {
    YearTemperatureKeyPair k1 = (YearTemperatureKeyPair) a;
    YearTemperatureKeyPair k2 = (YearTemperatureKeyPair) b;
    //年份升序排列
    int result = Integer.compare(k1.getYear(), k2.getYear());
    if (result != 0) {
      return result;
    }
    //温度降序排列
    return Integer.compare(k2.getTemperature(), k1.getTemperature());
  }
}
