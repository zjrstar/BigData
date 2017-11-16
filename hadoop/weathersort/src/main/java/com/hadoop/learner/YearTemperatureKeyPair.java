package com.hadoop.learner;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class YearTemperatureKeyPair implements WritableComparable<YearTemperatureKeyPair> {

  private int year;
  private int temperature;

  //提供比较方法
  public int compareTo(YearTemperatureKeyPair yearTemperatureKeyPair) {
    System.out.println("YearTemperatureKeyPair compareTo " + yearTemperatureKeyPair.getYear() + " : " + yearTemperatureKeyPair.getTemperature());
    int result = Integer.compare(year, yearTemperatureKeyPair.getYear());
    if (result != 0) {
      return result;
    } else {
      return Integer.compare(temperature, yearTemperatureKeyPair.getTemperature());
    }
  }

  //使用PRC协议写入二进制流，序列化过程
  public void write(DataOutput dataOutput) throws IOException {
    System.out.println("YearTemperatureKeyPair write " + year + " : " + temperature);
    dataOutput.writeInt(year);
    dataOutput.writeInt(temperature);
  }

  //使用RPC协议读取二进制流，反序列化过程
  public void readFields(DataInput dataInput) throws IOException {
    this.year = dataInput.readInt();
    this.temperature = dataInput.readInt();
    System.out.println("YearTemperatureKeyPair readFields " + year + " : " + temperature);
  }

  public int getYear() {
    return year;
  }

  public void setYear(int year) {
    this.year = year;
  }

  public Integer getTemperature() {
    return temperature;
  }

  public void setTemperature(int temperature) {
    this.temperature = temperature;
  }

  //需要重写toString，以便Job输出调用
  @Override
  public String toString() {
    return year + " " + temperature;
  }

  @Override
  public int hashCode() {
    int result = year;
    result = 31 * result + temperature;
    return result;
  }
}
