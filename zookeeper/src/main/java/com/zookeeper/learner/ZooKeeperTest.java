package com.zookeeper.learner;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;


public class ZooKeeperTest {

  public static final int SESSION_TIMEOUT = 30000;

  public static final String zkServers = "localhost:2181";

  public static final Logger LOGGER = LoggerFactory.getLogger(ZooKeeperTest.class);

  private Watcher watcher = new Watcher() {
    public void process(WatchedEvent watchedEvent) {
      LOGGER.info("Default watcher. process: " + watchedEvent.getType());
    }
  };

  private ZooKeeper zooKeeper;

  /**
   * 连接zookeeper
   *
   * @throws IOException
   */
  @Before
  public void connect() throws IOException {
    zooKeeper = new ZooKeeper(zkServers, SESSION_TIMEOUT, watcher);
  }

  /**
   * 关闭连接
   */
  @After
  public void close() {
    try {
      zooKeeper.close();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  /**
   * 创建一个znode 1.createMode 取值PERSISTENT：持久化，这个目录节点存储的数据不会丢失
   * PERSISTENT_SEQUENTIAL:顺序自动编号的目录节点，这种目录节点会根据当前已近存在的节点数自动加
   * 1，然后返回给客户端已经创建的目录节点名；EPHEMERAL:临时目录节点，一点创建这个节点的客户端与服务器端口也就是session过期超时，
   * 这种节点会被自动删除EPTHEMERAL_SEQUENTIAL:临时自动编号节点
   */
  public void testCreate() {
    String result = null;
    try {
      result = zooKeeper.create("/zk001", "zk001data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    } catch (Exception e) {
      LOGGER.error(e.getMessage());
      Assert.fail();
    }
    LOGGER.info("create result : {}", result);
  }

  /**
   * 删除节点，忽视版本
   */
  @Test
  public void testDelete() {
    try {
      zooKeeper.delete("/zk001", -1);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (KeeperException e) {
      LOGGER.error(e.getMessage());
      Assert.fail();
    }
  }

  @Test
  public void testGetData() {
    String result = null;
    try {
      byte[] bytes = zooKeeper.getData("/zk001", null, null);
      result = new String(bytes);
    } catch (Exception e) {
      LOGGER.error(e.getMessage());
      Assert.fail();
    }
    LOGGER.info("getdata result : {}", result);
  }

  /**
   * 获取数据， 设置watcher
   */
  @Test
  public void testGetDataWatch() {
    String result = null;
    try {
      byte[] bytes = zooKeeper.getData("/zk001", new Watcher() {
        public void process(WatchedEvent watchedEvent) {
          LOGGER.info("testGetDataWatch watch: {}", watchedEvent.getType());
        }
      }, null);
      result = new String(bytes);
    } catch (Exception e) {
      LOGGER.error(e.getMessage());
      Assert.fail();
    }
    LOGGER.info("getdata result : {}", result);

    //触发watch NodeDataChanged
    System.out.println("Beigin to change znode");
    try {
      zooKeeper.setData("/zk001", "testSetData".getBytes(), -1);
    } catch (Exception e) {
      LOGGER.error(e.getMessage());
      Assert.fail();
    }
  }

  /**
   * 判断节点是否存在，设置是否监控这个目录节点，这里的watcher是在创建zookeeper实例时指定的watcher
   */
  @Test
  public void testExists() {
    Stat stat = null;
    try {
      stat = zooKeeper.exists("/zk001", false);
    } catch (Exception e) {
      LOGGER.error(e.getMessage());
      Assert.fail();
    }
    Assert.assertNotNull(stat);
    LOGGER.info("exists result : {}", stat.getCzxid());
  }

  /**
   * 判断节点是否存在，这是是否监控这个目录节点，这里的watcher是在创建zookeeper实例时值得的watcher
   * 这个watcher会监控create/delete或者修改了节点值时，触发
   */
  @Test
  public void testExistWatch1() {
    Stat stat = null;
    try {
      stat = zooKeeper.exists("/zk001", true);
    } catch (Exception e) {
      LOGGER.error(e.getMessage());
      Assert.fail();
    }
    Assert.assertNotNull(stat);

    try {
      zooKeeper.delete("/zk001", -1);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * 判断节点是否存在，设置监控这个目录节点的watcher watcher不会触发多次
   */
  @Test
  public void testExistsWatch2() {
    Stat stat = null;
    try {
      stat = zooKeeper.exists("/zk002", new Watcher() {
        public void process(WatchedEvent watchedEvent) {
          LOGGER.info("testExistsWatch2 watch : {}", watchedEvent.getType());
        }
      });
    } catch (Exception e) {
      LOGGER.error(e.getMessage());
      Assert.fail();
    }
    Assert.assertNotNull(stat);

    //触发watch中的process方法NodeDataChanged
    try {
      zooKeeper.setData("/zk002", "testExistsWatch2".getBytes(), -1);
    } catch (Exception e) {
      e.printStackTrace();
    }

    //不会触发watch只会触发一次
    try {
      zooKeeper.delete("/zk002", -1);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * 设置对应的znode下的数据，-1标识匹配所有版本
   */
  @Test
  public void testSetData() {
    Stat stat = null;
    try {
      stat = zooKeeper.setData("/zk001", "testSetData".getBytes(), -1);
    } catch (Exception e) {
      LOGGER.error(e.getMessage());
      Assert.fail();
    }
    Assert.assertNotNull(stat);
    LOGGER.info("exists result : {}", stat.getVersion());

    //触发watch中的process方法NodeDataChanged
    try {
      zooKeeper.setData("/zk002", "testExistsWatch2".getBytes(), -1);
    } catch (Exception e) {
      e.printStackTrace();
    }

    //不会触发watch只会触发一次
    try {
      zooKeeper.delete("/zk002", -1);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * 获取指定节点下的子节点
   */
  @Test
  public void testGetChild() {
    try {
      zooKeeper.create("/zk001/n1", "001".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      zooKeeper.create("/zk001/n2", "002".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      List<String> children = zooKeeper.getChildren("/zk001", true);
      for (String node : children) {
        LOGGER.info("fffffff {}", node);
      }
    } catch (Exception e) {
      LOGGER.error(e.getMessage());
      Assert.fail();
    }
  }

  //=================永久watch开始=======================
  private Watcher setWatcher() {
    return new Watcher() {
      String result = null;

      public void process(WatchedEvent watchedEvent) {
        LOGGER.info("testRecvEvent watch : {}", watchedEvent.getType());
        System.out.println("I can do anything.");

        try {
          byte[] bytes = zooKeeper.getData("/zk001", setWatcher(), null);
          result = new String(bytes);
          System.out.println(result);
        } catch (Exception e) {
          LOGGER.error(e.getMessage());
          Assert.fail();
        }
      }
    };
  }

  /**
   * 永久watch测试
   * 案例：配置管理，配置信息改变，通知客户端
   */
  @Test
  public void testRecvEvent() {
    String result = null;

    try {
      byte[] bytes = zooKeeper.getData("/zk001", setWatcher(), null);
      result = new String(bytes);
      System.out.println(result);
      while (true) {
        Thread.sleep(500);
      }
    } catch (Exception e) {
      LOGGER.error(e.getMessage());
      Assert.fail();
    }
  }
  //==================永久watcher结束======================
}
