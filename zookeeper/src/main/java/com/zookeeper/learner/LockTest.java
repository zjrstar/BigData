package com.zookeeper.learner;

import java.net.InetAddress;

public class LockTest {

  public static void main(String[] args) throws Exception {
    InetAddress address = InetAddress.getLocalHost();
    Lock lock;
    while (true) {
      //节点存储的地址和传入的当前地址相等，就返回锁，否则返回null
      lock = LockFactory.getLock("/root/test", address.toString());
      if (lock == null) {
        System.out.println("伺机篡位");
      } else {
        System.out.println("我是老大");
        Thread.sleep(60 * 1000);
      }
    }
  }
}
