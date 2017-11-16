package com.hadoop.learner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FriendRecommendRunner extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    int run = ToolRunner.run(new Configuration(), new FriendRecommendRunner(), args);
    System.exit(run);
  }

  public int run(String[] args) throws Exception {
    Configuration config = new Configuration();
    //如果在HFDS云端运行MapReduce，需要加上fs.defaultFs的set，相应的路径加hdfs上的路径
    //config.set("fs.defaultFS", "hdfs://myhadoop/");

    Job job = Job.getInstance(config);

    job.setJarByClass(FriendRecommendRunner.class);

    job.setMapperClass(FriendMapper.class);
    job.setReducerClass(RecommendReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));

    Path output = new Path(args[1]);
    FileSystem fs = FileSystem.get(config);
    if (fs.exists(output)) {
      fs.delete(output, true);
    }
    FileOutputFormat.setOutputPath(job, output);

    return job.waitForCompletion(true) ? 0 : 1;
  }
}
