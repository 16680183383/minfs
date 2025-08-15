//package com.example.dataserver;
//
//
//import org.apache.curator.RetryPolicy;
//import org.apache.curator.framework.CuratorFramework;
//import org.apache.curator.framework.CuratorFrameworkFactory;
//import org.apache.curator.retry.ExponentialBackoffRetry;
//import org.apache.zookeeper.CreateMode;
//import org.testng.annotations.Test;
//
//import java.nio.charset.StandardCharsets;
//
//public class CuratorTest {
//
//    //建立连接
//    @Test
//    public void testConnect() {
//
//        /**
//         * @param connectString         连接字符串，zk server地址和端口 "127.0.0.1:2181"
//         * @param sessionTimeoutMs      会话超时时间，单位是毫秒ms
//         * @param connectionTimeoutMs   连接超时时间，单位是毫秒ms
//         * @param retryPolicy           重试策略
//         */
//        /*
//        //重试策略
//        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 10);
//        //第一种方式
//        CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", 60 * 1000, 15 * 1000, retryPolicy);
//        //开启连接
//        client.start();
//        */
//        //第二种方式,链式创建
//        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 10);
//        CuratorFramework client = CuratorFrameworkFactory.builder()
//                .connectString("127.0.0.1:2181")
//                .sessionTimeoutMs(60 * 1000)
//                .connectionTimeoutMs(15 * 1000)
//                .retryPolicy(retryPolicy).namespace("smsService").build();
//
//    }
//
//    @Test
//    public void testCreate() throws Exception {
//        CuratorFramework client = getClient();
//        //基本创建
//        String path = client.create().forPath("/app1");
//        System.out.println(path);
//
//        //带数据的创建
//        client.create().forPath("/app1","haha".getBytes(StandardCharsets.UTF_8));
//
//        //设置节点类型,临时类型，默认类型持久化
//        client.create().withMode(CreateMode.EPHEMERAL).forPath("/app1");
//
//        //创建多级节点  /app4/ios
//        //creatingParentsIfNeeded,如果父节点不存在则创建父节点
//        client.create().creatingParentsIfNeeded().forPath("/app4/ios");
//
//        client.close();
//
//    }
//
//
//
//}
