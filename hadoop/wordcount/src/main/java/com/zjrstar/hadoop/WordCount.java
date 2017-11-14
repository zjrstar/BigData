package com.zjrstar.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class WordCount {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "hdfs://master:9000");
    conf.set("mapred.job.tracker", "master:9001");
    //本地提交
    //conf.set("mapred.jar", "/Users/jerry/Workspaces/hadoop-samples/hadoop/target/hadoop-1.0-SNAPSHOT.jar");
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: WordCount <in> <out>");
      System.exit(2);
    }

    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
//    job.setNumReduceTasks(1); //设置reduce任务的个数
    //mapreduce输入数据所在目录或文件，可以写目录
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    //mapred执行之后的输出数据的目录
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }

  public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    //每次调用Map方法会传入split中一行数据key：该行数据所在文件的位置下标， value：这行数据
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      int idx = value.toString().indexOf(" ");
      if (idx > 0) {
        String e = value.toString().substring(0, idx);
        word.set(e);
        context.write(word, one);
      }
    }
//    @Override
//    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//    final StringTokenizer tokenizer = new StringTokenizer(value.toString());
//      while (tokenizer.hasMoreTokens()) {
//      String word = tokenizer.nextToken();
//      context.write(new Text(word), one);
//    }
//  }

  }

  public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
}
