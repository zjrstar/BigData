package com.zjrstar.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

public class Recommend {

  private static class RecommendMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      String[] ss = line.split("\t");
      context.write(new Text(ss[0]), new Text(ss[1]));
      context.write(new Text(ss[1]), new Text(ss[0]));
    }
  }

  private static class RecommendReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      HashSet<String> set = new HashSet<String>();
      for (Text value: values) {
        set.add(value.toString());
      }

      if (set.size() > 1) {
        for (Iterator i = set.iterator(); i.hasNext();) {
          String name = (String)i.next();
          for (Iterator j = set.iterator(); j.hasNext();) {
            String otherName = (String) j.next();
            if (!name.equalsIgnoreCase(otherName)) {
              context.write(new Text(name), new Text(otherName));
            }
          }
        }
      }
    }
  }
}
