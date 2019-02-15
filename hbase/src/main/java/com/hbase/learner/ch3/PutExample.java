package com.hbase.learner.ch3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PutExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.ZOOKEEPER_QUORUM, "localhost");
    conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, 2181);

    Connection conn = ConnectionFactory.createConnection(conf);

    Table table = conn.getTable(TableName.valueOf("test_table"));

//    basicPut(table);
//    //使用客户端写缓冲区
//    putWithBuffer(table);
//    //batchPut
//    batchPuts(table);
    //给不存在的familyColumn增加数据
    errorFamilyColumn(table);

  }

  private static void errorFamilyColumn(Table table) throws IOException {
    List<Put> puts = new ArrayList<Put>();

    Put put3 = new Put(Bytes.toBytes("row1"));
    put3.addColumn(Bytes.toBytes("testfml"), Bytes.toBytes("qual1"), Bytes.toBytes("value1"));
    puts.add(put3);

    Put put4 = new Put(Bytes.toBytes("row2"));
    put4.addColumn(Bytes.toBytes("fml"), Bytes.toBytes("qual1"), Bytes.toBytes("value1"));
    puts.add(put4);

    Put put5 = new Put(Bytes.toBytes("row2"));
    put5.addColumn(Bytes.toBytes("testfml"), Bytes.toBytes("qual2"), Bytes.toBytes("value2"));
    puts.add(put5);

    table.put(puts);
  }

  private static void batchPuts(Table table) throws IOException {
    List<Put> puts = new ArrayList<Put>();

    Put put3 = new Put(Bytes.toBytes("row1"));
    put3.addColumn(Bytes.toBytes("testfml"), Bytes.toBytes("qual1"), Bytes.toBytes("value1"));
    puts.add(put3);

    Put put4 = new Put(Bytes.toBytes("row2"));
    put4.addColumn(Bytes.toBytes("testfml"), Bytes.toBytes("qual1"), Bytes.toBytes("value1"));
    puts.add(put4);

    Put put5 = new Put(Bytes.toBytes("row2"));
    put5.addColumn(Bytes.toBytes("testfml"), Bytes.toBytes("qual2"), Bytes.toBytes("value2"));
    puts.add(put5);

    table.put(puts);
  }

  private static void basicPut(Table table) throws IOException {
    Put put = new Put(Bytes.toBytes("row1"));
    //列fml1:qual1
    put.addColumn(Bytes.toBytes("testfml"), Bytes.toBytes("qual1"), Bytes.toBytes("value1"));
    //列fml1:qual2
    put.addColumn(Bytes.toBytes("testfml"), Bytes.toBytes("qual2"), Bytes.toBytes("val2"));

    table.put(put);
  }

  private static void putWithBuffer(Table table) throws IOException {
    ((HTable) table).setAutoFlush(false);
    Put put3 = new Put(Bytes.toBytes("row3"));
    put3.addColumn(Bytes.toBytes("testfml"), Bytes.toBytes("qual1"), Bytes.toBytes("value11"));
    table.put(put3);
    Put put4 = new Put(Bytes.toBytes("row4"));
    put4.addColumn(Bytes.toBytes("testfml"), Bytes.toBytes("qual2"), Bytes.toBytes("value22"));
    table.put(put4);

    Put put5 = new Put(Bytes.toBytes("row5"));
    put5.addColumn(Bytes.toBytes("testfml"), Bytes.toBytes("qual1"), Bytes.toBytes("value5"));
    table.put(put5);

    Get get = new Get(Bytes.toBytes("row3"));
    Result result = table.get(get);
    //keyvalues=NONE
    System.out.println("Result:" + result);

    ((HTable) table).flushCommits();

    Result result1 = table.get(get);
    //keyvalues={row3/testfml:qual1/1546910782905/Put/vlen=7/seqid=0}
    System.out.println("Result:" + result1);
  }

}
