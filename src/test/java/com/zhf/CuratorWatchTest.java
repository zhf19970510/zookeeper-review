package com.zhf;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.Before;
import org.junit.Test;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class CuratorWatchTest {


    private CuratorFramework client;

    /**
     * 建立连接
     */
    @Before
    public void testConnect() {

        /**
         * String connectString     连接字符串。 zk地址和端口： "192.168.58.100:2181,192.168.58.101:2181"
         * int sessionTimeoutMs     会话超时时间 单位ms
         * int connectionTimeoutMs  连接超时时间 单位ms
         * RetryPolicy retryPolicy  重试策略
         */
        //1. 第一种方式

        //重试策略 baseSleepTimeMs 重试之间等待的初始时间，maxRetries 重试的最大次数
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 10);

//      client   = CuratorFrameworkFactory.newClient("192.168.58.100:2181", 60 * 1000,
//                15 * 1000, retryPolicy);

        //2. 第二种方式，建造者方式创建
        client = CuratorFrameworkFactory.builder()
                .connectString("127.0.0.1:2181")
                .sessionTimeoutMs(60 * 1000)
                .connectionTimeoutMs(15 * 1000)
                .retryPolicy(retryPolicy)
                .namespace("mashibing")  //根节点名称设置
                .build();

        //开启连接
        client.start();
    }

    @Test
    public void testOneListener() throws Exception {
        byte[] data = client.getData().usingWatcher(new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("监听器 watchedEvent" + watchedEvent);
            }
        }).forPath("/test");

        System.out.println("监听器内容： " + new String(data));

        while (true) {

        }
    }

    /**
     * 演示 NodeCache : 给指定一个节点注册监听
     */
    @Test
    public void testNodeCache() throws Exception {

        //1. 创建NodeCache对象
        NodeCache nodeCache = new NodeCache(client, "/app1");  //监听的是 /mashibing和其子目录app1

        //2. 注册监听
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                System.out.println("节点变化了。。。。。。");

                //获取修改节点后的数据
                byte[] data = nodeCache.getCurrentData().getData();
                System.out.println(new String(data));
            }
        });

        //3. 设置为true，开启监听
        nodeCache.start(true);

        while (true) {

        }
    }


    /**
     * 演示 PathChildrenCache: 监听某个节点的所有子节点
     */
    @Test
    public void testPathChildrenCache() throws Exception {

        //1.创建监听器对象 (第三个参数表示缓存每次节点更新后的数据)
        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, "/app2", true);

        //2.绑定监听器
        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
                System.out.println("子节点发生变化了。。。。。。");
                System.out.println(pathChildrenCacheEvent);

                if(PathChildrenCacheEvent.Type.CHILD_UPDATED == pathChildrenCacheEvent.getType()){
                    //更新子节点
                    System.out.println("子节点更新了！");
                    //在一个getData中有很多数据，我们只拿data部分
                    byte[] data = pathChildrenCacheEvent.getData().getData();
                    System.out.println("更新后的值为：" + new String(data));

                }else if(PathChildrenCacheEvent.Type.CHILD_ADDED == pathChildrenCacheEvent.getType()){
                    //添加子节点
                    System.out.println("添加子节点！");
                    String path = pathChildrenCacheEvent.getData().getPath();
                    System.out.println("子节点路径为： " + path);

                }else if(PathChildrenCacheEvent.Type.CHILD_REMOVED == pathChildrenCacheEvent.getType()){
                    //删除子节点
                    System.out.println("删除了子节点");
                    String path = pathChildrenCacheEvent.getData().getPath();
                    System.out.println("子节点路径为： " + path);
                }
            }
        });

        //3. 开启
        pathChildrenCache.start();

        while(true){

        }
    }

    /**
     * 演示 TreeCache: 监听某个节点的所有子节点
     */
    @Test
    public void testCache() throws Exception {

        //1.创建监听器对象
        TreeCache treeCache = new TreeCache(client, "/app2");

        //2.绑定监听器
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {
                System.out.println("节点变化了");
                System.out.println(treeCacheEvent);

                if(TreeCacheEvent.Type.NODE_UPDATED == treeCacheEvent.getType()){
                    //更新节点
                    System.out.println("节点更新了！");
                    //在一个getData中有很多数据，我们只拿data部分
                    byte[] data = treeCacheEvent.getData().getData();
                    System.out.println("更新后的值为：" + new String(data));

                }else if(TreeCacheEvent.Type.NODE_ADDED == treeCacheEvent.getType()){
                    //添加子节点
                    System.out.println("添加节点！");
                    String path = treeCacheEvent.getData().getPath();
                    System.out.println("子节点路径为： " + path);

                }else if(TreeCacheEvent.Type.NODE_REMOVED == treeCacheEvent.getType()){
                    //删除子节点
                    System.out.println("删除节点");
                    String path = treeCacheEvent.getData().getPath();
                    System.out.println("删除节点路径为： " + path);
                }
            }
        });

        //3. 开启
        treeCache.start();

        while(true){

        }
    }

    /**
     * 事务操作
     */
    @Test
    public void TestTransaction() throws Exception {

        //1. 创建Curator对象，用于定义事务操作
        CuratorOp createOp = client.transactionOp().create().forPath("/app3", "app1-data".getBytes());
        CuratorOp setDataOp = client.transactionOp().setData().forPath("/app2", "app2-data".getBytes());
        CuratorOp deleteOp = client.transactionOp().delete().forPath("/app2");

        //2. 添加事务操
        Collection<CuratorTransactionResult> results = client.transaction().forOperations(createOp, setDataOp, deleteOp);

        //3. 遍历事务操作结果
        for (CuratorTransactionResult result : results) {
            System.out.println(result.getForPath() + " - " + result.getType());
        }
    }

    // 异步操作
    @Test
    public void TestAsync() throws Exception {

        while(true){

            // 异步获取子节点列表
            GetChildrenBuilder builder = client.getChildren();
            builder.inBackground(new BackgroundCallback() {
                @Override
                public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
                    System.out.println("子节点列表：" + curatorEvent.getChildren());
                }
            }).forPath("/");

            TimeUnit.SECONDS.sleep(5);
        }

    }

}
