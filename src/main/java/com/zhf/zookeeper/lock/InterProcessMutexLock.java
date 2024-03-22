package com.zhf.zookeeper.lock;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;

/**
 * 使用curator提供的分布式锁实现
 */
@Slf4j
public class InterProcessMutexLock {

    private final static String connectString = "192.168.58.200:2181,192.168.58.200:2182,192.168.58.200:2183";

    /**
     * 获取连接
     */
    public static CuratorFramework getZKClient() {
        //重试策略 baseSleepTimeMs 重试之间等待的初始时间，maxRetries 重试的最大次数
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 10);
        CuratorFramework zkClient = CuratorFrameworkFactory.builder()
                .connectString(connectString)
                .sessionTimeoutMs(60 * 1000)
                .connectionTimeoutMs(60 * 1000)
                .retryPolicy(retryPolicy)
                .build();

        zkClient.start();
        return zkClient;
    }

    private static final String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];

    /**
     * 分布式锁
     */
    public void buyMaskWithLock(CuratorFramework zkClient) throws Exception {

        while (true) {
            // 创建分布式锁，并指定根节点
            InterProcessMutex mutexLock = new InterProcessMutex(zkClient, "/curator");
            log.info("【当前线程：" + pid + "】创建节点成功，尝试获取锁！");
            mutexLock.acquire();
            log.info("【当前线程：" + pid + "】 创建节点成功，尝试获取锁成功！");

            // 做具体的业务逻辑
            TimeUnit.SECONDS.sleep(3);
            mutexLock.release();
            log.info("【当前线程：" + pid + "】释放锁成功！");

        }
    }

    public static void main(String[] args) {
        try(CuratorFramework zkClient = getZKClient()) {
            new InterProcessMutexLock().buyMaskWithLock(zkClient);
        }catch (Exception ex) {
            log.error("抢口罩失败！", ex);
        }finally {

        }
    }

}
