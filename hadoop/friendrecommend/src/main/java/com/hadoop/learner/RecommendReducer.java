package com.hadoop.learner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class RecommendReducer extends Reducer<Text, Text, Text, Text> {

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    Set<String> ss = new HashSet<String>();

    for (Text value : values) {
      ss.add(value.toString());
    }

    Iterator<String> iterator = ss.iterator();
    while (iterator.hasNext()) {
      String name = iterator.next();
      System.out.println(name);
    }

    if (ss.size() > 1) {
      for (Iterator<String> i = ss.iterator(); i.hasNext(); ) {
        String first = i.next();
        for (Iterator<String> j = ss.iterator(); j.hasNext(); ) {
          String second = j.next();
          if (!first.equalsIgnoreCase(second)) {
            context.write(new Text(first), new Text(second));
          }
        }
      }
    }
  }
}
