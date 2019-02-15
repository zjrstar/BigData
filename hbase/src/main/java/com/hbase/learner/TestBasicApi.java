package com.hbase.learner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableSet;

public class TestBasicApi {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.ZOOKEEPER_QUORUM, "127.0.0.1");
    conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, 2181);

//    HBaseAdmin ha = new HBaseAdmin(conf);
//    TableName name = TableName.valueOf(Bytes.toBytes("test_table"));
//    HTableDescriptor desc = new HTableDescriptor(name);
//    HColumnDescriptor family = new HColumnDescriptor(Bytes.toBytes("testfml"));
//    desc.addFamily(family);
//    ha.createTable(desc);
//    ha.close();

    HTable table = new HTable(conf, "test_table");
    //PUT是RPC操作
    //Put(byte[] row) row为rowkey,可以是用户名，或者订单号
    Put put = new Put(Bytes.toBytes("testkey1"));
    //addColumn(byte[] family, byte[] qualifier, long ts , byte[] value)
    put.addColumn(Bytes.toBytes("testfml"), Bytes.toBytes("testcl"), 130L, Bytes.toBytes("testvalue1"));
    table.put(put);

    //Get(byte[] rowkey) rowkey，可以使用户名，或者订单号
    Get get = new Get(Bytes.toBytes("testkey1"));
    Map<byte[], NavigableSet<byte[]>> familyMap = get.getFamilyMap();
    for (Map.Entry<byte[], NavigableSet<byte[]>> family : familyMap.entrySet()) {
      System.out.println("family:=" + family.toString());
    }
    get.addColumn(Bytes.toBytes("testfml"), Bytes.toBytes("testcl"));
    Result result = table.get(get);
    String key = Bytes.toString(result.getRow());
    String value = Bytes.toString(result.getValue(Bytes.toBytes("testfml"), Bytes.toBytes("testcl")));
    System.out.println("key:" + key);
    System.out.println("value:" + value);

    Scan scan = new Scan();
    scan.addColumn(Bytes.toBytes("testfml"), Bytes.toBytes("testcl"));
    scan.setCaching(100);
    scan.setTimeRange(120L, 200L);
    ResultScanner scanner = table.getScanner(scan);
    Result resultScan = null;
    while ((resultScan = scanner.next()) != null) {
      String scanKey = Bytes.toString(resultScan.getRow());
      String scanValue = Bytes.toString(resultScan.getValue(Bytes.toBytes("testfml"), Bytes.toBytes("testcl")));
      System.out.println("scankey:" + scanKey);
      System.out.println("scanvalue:" + scanValue);
    }
    scanner.close();
    Delete delete = new Delete(Bytes.toBytes("testkey1"));
    delete.deleteColumn(Bytes.toBytes("testfml"), Bytes.toBytes("testcl"), 130L);
    table.delete(delete);

    Get newGet = new Get(Bytes.toBytes("testkey1"));
    newGet.addColumn(Bytes.toBytes("testfml"), Bytes.toBytes("testcl"));
    boolean exists = table.exists(newGet);
    System.out.println("is exist:" + exists);
    table.close();
  }
}
