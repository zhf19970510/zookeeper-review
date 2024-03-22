package com.zhf;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.junit.Before;
import org.junit.Test;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class TestAcl {
    private CuratorFramework client;

    @Test
    public void createPw() throws NoSuchAlgorithmException {

        String up = "msb:msb";
        byte[] digest = MessageDigest.getInstance("SHA1").digest(up.getBytes());
        String encodeStr = Base64.getEncoder().encodeToString(digest);
        System.out.println(encodeStr);
    }

    //1.创建连接
    @Before
    public void createConnect(){

        client = CuratorFrameworkFactory.builder()
                .connectString("192.168.58.100:2181")
                .sessionTimeoutMs(5000).connectionTimeoutMs(20000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .namespace("msbAcl").build();

        client.start();
    }

    @Test
    public void testCuratorAcl() throws Exception {

        //创建ID，以Digest方式认证，用户名和密码为 msb:msb
        Id id = new Id("digest", DigestAuthenticationProvider.generateDigest("msb:msb"));

        //为ID对象指定权限
        List<ACL> acls = new ArrayList<>();
        acls.add(new ACL(ZooDefs.Perms.ALL,id));

        //创建节点 "auth",设置节点数据，并设置ACL权限
        String node = client.create().creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)  // 设置节点类型是持久节点
                .withACL(acls,false)    //设置节点的ACL权限
                .forPath("/auth","hello".getBytes());   //设置节点的路径和数据

        System.out.println("成功创建带权限的节点： " + node);

        //获取刚刚创建的节点的数据
        byte[] bytes = client.getData().forPath(node);
        System.out.println("获取数据结果： " + new String(bytes));
    }
}
