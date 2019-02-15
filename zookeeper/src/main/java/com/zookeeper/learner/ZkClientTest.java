package com.zookeeper.learner;

import org.I0Itec.zkclient.ZkClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ZkClientTest {

  private static final String zkServers = "localhost:2181";

  private ZkClient zkClient;

  @Before
  public void setUp() throws Exception {
    zkClient = new ZkClient(zkServers);
  }

  @Test
  public void createNode() {

  }
}
