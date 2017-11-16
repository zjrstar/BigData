package com.hadoop.learner;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class YearGroup extends WritableComparator {

  public YearGroup() {
    super(YearTemperatureKeyPair.class, true);
  }

  //reduce的二次排序阶段，根据year值进行分组
  @Override
  public int compare(WritableComparable a, WritableComparable b) {
    YearTemperatureKeyPair k1 = (YearTemperatureKeyPair) a;
    YearTemperatureKeyPair k2 = (YearTemperatureKeyPair) b;
    return Integer.compare(k1.getYear(), k2.getYear());
  }
}
