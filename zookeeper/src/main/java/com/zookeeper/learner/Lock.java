package com.zookeeper.learner;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class Lock {

  private ZooKeeper zooKeeper;
  private String path;

  public Lock(String path) {
    this.path = path;
  }

  /**
   * 方法描述：上锁lock it
   *
   * @throws Exception
   */
  public synchronized void lock() throws Exception {
    Stat stat = zooKeeper.exists(path, true);
    String data = InetAddress.getLocalHost().getHostAddress() + ":lock";
    zooKeeper.setData(path, data.getBytes(), stat.getVersion());
  }

  /**
   * 解锁
   *
   * @throws Exception
   */
  public synchronized void unLock() throws Exception {
    Stat stat = zooKeeper.exists(path, true);
    String data = InetAddress.getLocalHost().getHostAddress() + ":unlock";
    zooKeeper.setData(path, data.getBytes(), stat.getVersion());
  }

  /**
   * 是否锁住了，isLocked？
   *
   * @return
   */
  public synchronized boolean isLock() {
    try {
      Stat stat = zooKeeper.exists(path, true);
      String data = InetAddress.getLocalHost().getHostAddress() + ":lock";
      String nodeData = new String(zooKeeper.getData(path, true, stat));
      if (data.equals(nodeData)) {
        return true;
      }
    } catch (KeeperException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    return false;
  }

  public String getPath() {
    return path;
  }

  public void setZookeeper(ZooKeeper zookeeper) {
    this.zooKeeper = zookeeper;
  }
}
