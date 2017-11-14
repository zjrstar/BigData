package com.zjrstar.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.net.URI;

public class HelloWorld {
  public static void main(String[] args) throws Exception {
    String uri = "hdfs://master:9000";
    Configuration config = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(uri), config);

    FileStatus[] statuses = fs.listStatus(new Path("/"));
    for (FileStatus status : statuses) {
      System.out.println(status);
    }

    FSDataOutputStream os = fs.create(new Path("/wordcount/wc.text"));
    os.write("Hello World!".getBytes());
    os.flush();
    os.close();

    FSDataInputStream is = fs.open(new Path("/wordcount/wc.text"));
    IOUtils.copyBytes(is, System.out, 1024, true);
  }
}
