package com.zhf.zookeeper.lock;


import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * 分布式锁
 */
public class DistributedLock {

    private ZooKeeper client;

    // 连接信息
    private String connectString = "192.168.58.200:2181,192.168.58.200:2182,192.168.58.200:2183";

    // 超时时间
    private int sessionTimeOut = 30000;

    // 等待zk连接成功
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    // 等待节点变化
    private CountDownLatch waitLatch = new CountDownLatch(1);

    //当前节点
    private String currentNode;

    //前一个节点路径
    private String waitPath;

    //1. 在构造方法中获取连接
    public DistributedLock() throws Exception {

        client = new ZooKeeper(connectString, sessionTimeOut, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                //countDownLatch 连上ZK，可以释放
                if(watchedEvent.getState() == Event.KeeperState.SyncConnected){
                    countDownLatch.countDown();
                }

                //waitLatch 需要释放 (节点被删除并且删除的是前一个节点)
                if(watchedEvent.getType() == Event.EventType.NodeDeleted &&
                        watchedEvent.getPath().equals(waitPath)){
                    waitLatch.countDown();
                }
            }
        });

        //等待Zookeeper连接成功，连接完成继续往下走
        countDownLatch.await();

        //2. 判断节点是否存在
        Stat stat = client.exists("/locks", false);
        if(stat == null){
            //创建一下根节点
            client.create("/locks","locks".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

    }

    //3.对ZK加锁
    public void zkLock(){
        //创建 临时带序号节点
        try {
            currentNode = client.create("/locks/" + "seq-", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

            //判断 创建的节点是否是最小序号节点，如果是 就获取到锁；如果不是就监听前一个节点
            List<String> children = client.getChildren("/locks", false);

            //如果创建的节点只有一个值，就直接获取到锁，如果不是，监听它前一个节点
            if(children.size() == 1){
                return;
            }else{
                //先排序
                Collections.sort(children);

                //获取节点名称
                String nodeName = currentNode.substring("/locks/".length());

                //通过名称获取该节点在集合的位置
                int index = children.indexOf(nodeName);

                //判断
                if(index == -1){
                    System.out.println("数据异常");
                }else if(index == 0){
                    //就一个节点，可以获取锁
                    return;
                }else{
                    //需要监听前一个节点变化
                    waitPath = "/locks/" + children.get(index-1);
                    client.getData(waitPath,true,null);

                    //等待监听执行
                    waitLatch.await();
                    return;
                }
            }

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    //4.解锁
    public void unZkLock() throws KeeperException, InterruptedException {

        //删除节点
        client.delete(currentNode,-1);
    }


}