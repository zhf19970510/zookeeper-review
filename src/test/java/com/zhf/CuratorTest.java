package com.zhf;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class CuratorTest {

    private CuratorFramework client;

    /**
     * 建立连接
     */
    @Before
    public void testConnect(){

        /**
         * String connectString     连接字符串。 zk地址和端口： "192.168.58.100:2181,192.168.58.101:2181"
         * int sessionTimeoutMs     会话超时时间 单位ms
         * int connectionTimeoutMs  连接超时时间 单位ms
         * RetryPolicy retryPolicy  重试策略
         */
        //1. 第一种方式

        //重试策略 baseSleepTimeMs 重试之间等待的初始时间，maxRetries 重试的最大次数
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000,10);

//      client   = CuratorFrameworkFactory.newClient("192.168.58.100:2181", 60 * 1000,
//                15 * 1000, retryPolicy);

        //2. 第二种方式，建造者方式创建
        client = CuratorFrameworkFactory.builder()
                .connectString("127.0.0.1:2181")
                .sessionTimeoutMs(60*1000)
                .connectionTimeoutMs(15 * 1000)
                .retryPolicy(retryPolicy)
                .namespace("mashibing")  //根节点名称设置
                .build();

        //开启连接
        client.start();
    }

    /**
     * 创建节点 create 持久 临时 顺序 数据
     */
    //1.创建节点
    @Test
    public void testCreate1() throws Exception {

        // 如果没有创建节点，没有指定数据，则默认将当前客户端的IP 作为数据存储
        String path = client.create().forPath("/app1");
        System.out.println(path);
    }

    //2.创建节点 带有数据
    @Test
    public void testCreate2() throws Exception {
        String path = client.create().forPath("/app2","hehe".getBytes());
        System.out.println(path);
    }

    //3.设置节点类型 默认持久化
    @Test
    public void testCreate3() throws Exception {
        //设置临时节点
        String path = client.create().withMode(CreateMode.EPHEMERAL).forPath("/app3");
        System.out.println(path);
    }

    //1.查询数据 getData
    @Test
    public void testGet1() throws Exception {
        byte[] data = client.getData().forPath("/app1");
        System.out.println(new String(data));
    }

    //2.查询子节点 getChildren()
    @Test
    public void testGet2() throws Exception {
        List<String> path = client.getChildren().forPath("/");
        System.out.println(path);
    }

    //3.查询节点状态信息
    @Test
    public void testGet3() throws Exception {
        Stat status = new Stat();
        System.out.println(status);
        //查询节点状态信息： ls -s
        client.getData().storingStatIn(status).forPath("/app1");
        System.out.println(status);
    }

    //1. 基本数据修改
    @Test
    public void testSet() throws Exception {
        client.setData().forPath("/app1","hahaha".getBytes());
    }

    //根据版本修改（乐观锁）
    @Test
    public void testSetVersion() throws Exception {
        //查询版本
        Stat status = new Stat();
        //查询节点状态信息： ls -s
        client.getData().storingStatIn(status).forPath("/app1");
        int version = status.getVersion();
        System.out.println(version);  //2

        client.setData().withVersion(version).forPath("/app1","hehe".getBytes());
    }

    //1.删除单个节点
    @Test
    public void testDelete1() throws Exception {

        client.delete().forPath("/app4");
    }

    //删除带有子节点的节点
    @Test
    public void testDelete2() throws Exception {

        client.delete().deletingChildrenIfNeeded().forPath("/app4");
    }

    //必须删除成功（超时情况下，重试删除）
    @Test
    public void testDelete3() throws Exception {

        client.delete().guaranteed().forPath("/app2");
    }

    //回调 删除完成后执行
    @Test
    public void testDelete4() throws Exception {

        client.delete().guaranteed().inBackground((curatorFramework, curatorEvent) -> {
            System.out.println("我被删除了");
            System.out.println(curatorEvent);
        }).forPath("/app1");
    }

    @After
    public void close(){
        client.close();
    }
}