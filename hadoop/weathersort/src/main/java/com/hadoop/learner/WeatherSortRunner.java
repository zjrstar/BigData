package com.hadoop.learner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class WeatherSortRunner {

  static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  public static void main(String[] args) {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://localhost:9000/");
    try {
      Job job = new Job(conf);
      job.setJobName("Temperature Sort");
      job.setJarByClass(WeatherSortRunner.class);

      job.setMapperClass(DataMapper.class);
      job.setMapOutputKeyClass(YearTemperatureKeyPair.class);
      job.setMapOutputValueClass(Text.class);

      job.setReducerClass(DataReducer.class);

      job.setNumReduceTasks(3);
      job.setPartitionerClass(YearPartition.class);
      job.setSortComparatorClass(YearTemperatureSort.class);
      job.setGroupingComparatorClass(YearGroup.class);


      FileInputFormat.addInputPath(job, new Path(args[0]));

      Path path = new Path(args[1]);
      FileSystem fs = FileSystem.get(conf);
      if (fs.exists(path)) {
        fs.delete(path, true);
      }

      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      System.exit(job.waitForCompletion(true) ? 0 : 1);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  static class DataMapper extends Mapper<LongWritable, Text, YearTemperatureKeyPair, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

      String line = value.toString();

      String[] ss = line.split("\t");
      if (ss.length == 2) {
        try {
          Date date = sdf.parse(ss[0]);
          Calendar calendar = Calendar.getInstance();
          calendar.setTime(date);
          //读取年份
          int year = calendar.get(1);
          String temperature = ss[1].substring(0, ss[1].indexOf("C"));
          System.out.println("Mapper "+year + " : " + temperature);
          YearTemperatureKeyPair keyPair = new YearTemperatureKeyPair();
          keyPair.setYear(year);
          keyPair.setTemperature(Integer.parseInt(temperature));
          context.write(keyPair, new Text(temperature));
        } catch (ParseException e) {
          e.printStackTrace();
        }
      }
    }
  }

  static class DataReducer extends Reducer<YearTemperatureKeyPair, Text, YearTemperatureKeyPair, Text> {

    @Override
    protected void reduce(YearTemperatureKeyPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      for (Text v : values) {
        System.out.println("Reduce " + key.toString() + " : " + v.toString());
        context.write(key, v);
      }
    }
  }


}
