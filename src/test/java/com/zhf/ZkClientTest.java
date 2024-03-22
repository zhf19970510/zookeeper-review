package com.zhf;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class ZkClientTest {

    private String connectString = "192.168.58.200:2181,192.168.58.200:2182,192.168.58.200:2183";
    private int sessionTimeout = 2000;

    private ZkClient zkClient;

    /**
     * 获取zk客户端连接
     */
    @Before
    public void Before(){

        /**
         * 参数1：服务器的IP和端口
         * 参数2：会话的超时时间
         * 参数3：会话的连接时间
         * 参数4：序列化方式
         */
        zkClient = new ZkClient(connectString, sessionTimeout, 1000 * 15, new SerializableSerializer());

    }

    @After
    public void after(){
        zkClient.close();
    }

    /**
     * 创建节点
     */
    @Test
    public void testCreateNode(){

        //创建方式 返回创建节点名称
        String nodeName = zkClient.create("/node1", "lisi", CreateMode.PERSISTENT);
        System.out.println("路径名称为：" + nodeName);

        zkClient.create("/node2","wangwu",CreateMode.PERSISTENT_SEQUENTIAL);
        zkClient.create("/node3","hehe",CreateMode.EPHEMERAL);
        zkClient.create("/node4","haha",CreateMode.EPHEMERAL_SEQUENTIAL);

        while(true){}
    }

    /**
     * 删除节点
     */
    @Test
    public void testDeleteNode(){
        // 删除没有子节点的节点
//        boolean b1 = zkClient.delete("/node2");
//        System.out.println("删除成功： " + b1);

        // 递归删除节点信息
        boolean b2 = zkClient.deleteRecursive("/node2");
        System.out.println("删除成功： " + b2);
    }

    /**
     * 查询节点的子节点
     */
    @Test
    public void testFindNodes(){

        //返回指定路径的节点信息
        List<String> ch = zkClient.getChildren("/");

        for (String c1 : ch) {
            System.out.println(c1);
        }
    }

    /**
     * 获取节点数据
     */
    @Test
    public void testFindNodeData(){
        String nodeName = zkClient.create("/node3", "taotao", CreateMode.PERSISTENT);
        Object data = zkClient.readData("/node3");
        System.out.println(data);
    }

    /**
     * 获取数据以及当前节点状态信息
     */
    @Test
    public void testFindNodeDataAndStat(){
        Stat stat = new Stat();
        Object data = zkClient.readData("/node20000000004", stat);

        System.out.println(data);
        System.out.println(stat);
    }

    /**
     * 修改节点
     */
    @Test
    public void testUpdateNodeData(){

        zkClient.writeData("/node3","123456");
    }

    /**
     * 监听节点数据
     */
    @Test
    public void testNodeChange(){

        zkClient.subscribeDataChanges("/node3", new IZkDataListener() {

            // 当节点的值在修改时，会自动调用这个方法
            @Override
            public void handleDataChange(String nodeName, Object result) throws Exception {
                System.out.println("节点名称： " + nodeName);
                System.out.println("节点数据： " + result);
            }

            // 当节点被删除时，会调用该方法
            @Override
            public void handleDataDeleted(String nodeName) throws Exception {
                System.out.println("节点名称： " + nodeName);
            }
        });

        while(true){}
    }

    /**
     * 监听节点目录的变化
     */
    @Test
    public void testNodesChange(){

        zkClient.subscribeChildChanges("/node3", new IZkChildListener() {

            @Override
            public void handleChildChange(String nodeName, List<String> list) throws Exception {
                System.out.println("父节点名称： " + nodeName);
                System.out.println("发生变更后，所有子节点名称： ");
                for (String name : list) {
                    System.out.println(name);
                }
            }
        });

        while(true){}
    }

    //判断节点是否存在
    @Test
    public void exist(){

        boolean exists = zkClient.exists("/node3");

        System.out.println(exists == true ? "节点存在" : "节点不存在");

    }

}