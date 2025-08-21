package com.ksyun.campus.client;

import com.ksyun.campus.client.domain.ClusterInfo;
import com.ksyun.campus.client.domain.DataServerMsg;
import com.ksyun.campus.client.domain.MetaServerMsg;
import com.ksyun.campus.client.domain.ReplicaData;
import com.ksyun.campus.client.domain.StatInfo;
import com.ksyun.campus.client.util.HttpClientUtil;
import com.ksyun.campus.client.util.ZkUtil;
import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class EFileSystem extends FileSystem {
    
    private String defaultFileSystemName;
    private ZkUtil zkUtil;
    private HttpClient httpClient;
    private String defaultMetaServerAddress;
    
    public EFileSystem() {
        this("default");
    }

    public EFileSystem(String fileSystemName) {
        this.defaultFileSystemName = fileSystemName;
        initializeComponents();
    }
    
    /**
     * 初始化组件
     */
    private void initializeComponents() {
        try {
            // 初始化Zookeeper工具
            zkUtil = new ZkUtil();
            zkUtil.connectToZookeeper();
            
            // 初始化HTTP客户端
            httpClient = createHttpClient();
            
            // 获取默认MetaServer地址
            List<String> metaServerAddresses = zkUtil.getMetaServerAddresses();
            if (metaServerAddresses != null && !metaServerAddresses.isEmpty()) {
                defaultMetaServerAddress = metaServerAddresses.get(0);
            } else {
                defaultMetaServerAddress = "localhost:8000"; // 默认地址
            }
            
        } catch (Exception e) {
            throw new RuntimeException("初始化文件系统失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * 创建HTTP客户端
     */
    private HttpClient createHttpClient() {
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(100);
        connectionManager.setDefaultMaxPerRoute(20);
        
        return HttpClients.custom()
                .setConnectionManager(connectionManager)
                .build();
    }
    
    /**
     * 获取MetaServer地址
     */
    public String getMetaServerAddress() {
        try {
            // 1) 优先取ZK的leader地址
            String leader = zkUtil.getLeaderAddress();
            if (leader != null && !leader.isEmpty()) {
                return leader;
            }
            // 2) 回退：取metaservers子节点列表的第一个
            List<String> addresses = zkUtil.getMetaServerAddresses();
            if (addresses != null && !addresses.isEmpty()) {
                return addresses.get(0);
            }
        } catch (Exception e) {
            // 忽略，走默认
        }
        return defaultMetaServerAddress;
    }
    
    /**
     * 获取MetaServer信息
     */
    public MetaServerMsg getMetaServer() {
        String address = getMetaServerAddress();
        MetaServerMsg metaServer = new MetaServerMsg();
        if (address.contains(":")) {
            String[] parts = address.split(":", 2);
            metaServer.setHost(parts[0]);
            try {
                metaServer.setPort(Integer.parseInt(parts[1]));
            } catch (NumberFormatException e) {
                metaServer.setPort(8000);
            }
        } else {
            metaServer.setHost(address);
            metaServer.setPort(8000);
        }
        return metaServer;
    }

    /**
     * 获取当前文件系统名称
     */
    public String getFileSystemName() {
        return defaultFileSystemName;
    }

    /**
     * 设置文件系统名称
     */
    public void setFileSystemName(String fileSystemName) {
        this.defaultFileSystemName = fileSystemName;
    }

    /**
     * 打开文件
     */
    @Override
    public FSInputStream open(String path) throws IOException {
        try {
            System.out.println("打开文件: fileSystemName=" + defaultFileSystemName + ", path=" + path);
            
            // 1. 从MetaServer获取文件元数据
            MetaServerMsg metaServer = getMetaServer();
            String url = "http://" + metaServer.getHost() + ":" + metaServer.getPort() + "/open";
            
            // 构建请求头
            String queryParams = "?path=" + URLEncoder.encode(path, StandardCharsets.UTF_8);
            String fullUrl = url + queryParams;
            
            // 添加文件系统名称到请求头
            String response = HttpClientUtil.doGetWithHeader(httpClient, fullUrl, "fileSystemName", defaultFileSystemName);
            
            if (response != null && !response.contains("error")) {
                // 解析JSON响应为StatInfo对象
                com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                com.ksyun.campus.client.domain.StatInfo statInfo = mapper.readValue(response, com.ksyun.campus.client.domain.StatInfo.class);
                if (statInfo == null || statInfo.getType() == null) {
                    throw new IOException("打开文件失败: 返回的元数据无效");
                }
                System.out.println("成功获取文件元数据: fileSystemName=" + defaultFileSystemName + ", path=" + statInfo.getPath() + ", size=" + statInfo.getSize());
                return new FSInputStream(statInfo, this);
            } else {
                throw new IOException("打开文件失败: " + path + ", 响应: " + response);
            }
            
        } catch (Exception e) {
            System.err.println("打开文件异常: fileSystemName=" + defaultFileSystemName + ", path=" + path + ", 错误: " + e.getMessage());
            throw new IOException("打开文件失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * 创建文件
     */
    @Override
    public FSOutputStream create(String path) throws IOException {
        try {
            System.out.println("创建文件: fileSystemName=" + defaultFileSystemName + ", path=" + path);
            
            // 1. 从MetaServer创建文件
            MetaServerMsg metaServer = getMetaServer();
            String url = "http://" + metaServer.getHost() + ":" + metaServer.getPort() + "/create";
            
            String queryParams = "?path=" + URLEncoder.encode(path, StandardCharsets.UTF_8);
            String fullUrl = url + queryParams;
            
            // 添加文件系统名称到请求头
            String response = HttpClientUtil.doGetWithHeader(httpClient, fullUrl, "fileSystemName", defaultFileSystemName);
            
            if (response != null && !response.contains("error")) {
                // 简化处理，直接创建StatInfo
                StatInfo statInfo = new StatInfo();
                statInfo.setPath(path);
                statInfo.setSize(0);
                System.out.println("成功创建文件: fileSystemName=" + defaultFileSystemName + ", path=" + path);
                return new FSOutputStream(path, this);
            } else {
                throw new IOException("创建文件失败: " + path + ", 响应: " + response);
            }
            
        } catch (Exception e) {
            System.err.println("创建文件异常: fileSystemName=" + defaultFileSystemName + ", path=" + path + ", 错误: " + e.getMessage());
            throw new IOException("创建文件失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * 创建目录
     */
    @Override
    public boolean mkdir(String path) throws IOException {
        try {
            System.out.println("创建目录: fileSystemName=" + defaultFileSystemName + ", path=" + path);
            
            // 从MetaServer创建目录
            MetaServerMsg metaServer = getMetaServer();
            String url = "http://" + metaServer.getHost() + ":" + metaServer.getPort() + "/mkdir";
            
            String queryParams = "?path=" + URLEncoder.encode(path, StandardCharsets.UTF_8);
            String fullUrl = url + queryParams;
            
            // 添加文件系统名称到请求头
            String response = HttpClientUtil.doGetWithHeader(httpClient, fullUrl, "fileSystemName", defaultFileSystemName);
            
            if (response != null && !response.contains("error")) {
                System.out.println("成功创建目录: fileSystemName=" + defaultFileSystemName + ", path=" + path);
                return true;
            } else {
                throw new IOException("创建目录失败: " + path + ", 响应: " + response);
            }
            
        } catch (Exception e) {
            System.err.println("创建目录异常: fileSystemName=" + defaultFileSystemName + ", path=" + path + ", 错误: " + e.getMessage());
            throw new IOException("创建目录失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * 删除文件或目录
     */
    @Override
    public boolean delete(String path) throws IOException {
        try {
            System.out.println("删除文件/目录: fileSystemName=" + defaultFileSystemName + ", path=" + path);
            
            // 从MetaServer删除文件/目录
            MetaServerMsg metaServer = getMetaServer();
            String url = "http://" + metaServer.getHost() + ":" + metaServer.getPort() + "/delete";
            
            String queryParams = "?path=" + URLEncoder.encode(path, StandardCharsets.UTF_8);
            String fullUrl = url + queryParams;
            
            // 添加文件系统名称到请求头
            String response = HttpClientUtil.doGetWithHeader(httpClient, fullUrl, "fileSystemName", defaultFileSystemName);
            
            if (response != null && !response.contains("error")) {
                // 检查响应中的success字段
                if (response.contains("\"success\":true")) {
                    System.out.println("成功删除文件/目录: fileSystemName=" + defaultFileSystemName + ", path=" + path);
                    return true;
                } else {
                    throw new IOException("删除失败: " + response);
                }
            } else {
                throw new IOException("删除失败: " + path + ", 响应: " + response);
            }
            
        } catch (Exception e) {
            System.err.println("删除文件/目录异常: fileSystemName=" + defaultFileSystemName + ", path=" + path + ", 错误: " + e.getMessage());
            throw new IOException("删除失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * 获取文件状态
     */
    @Override
    public StatInfo getFileStats(String path) throws IOException {
        try {
            System.out.println("获取文件状态: fileSystemName=" + defaultFileSystemName + ", path=" + path);
            
            // 从MetaServer获取文件状态
            MetaServerMsg metaServer = getMetaServer();
            String url = "http://" + metaServer.getHost() + ":" + metaServer.getPort() + "/stats";
            
            String queryParams = "?path=" + URLEncoder.encode(path, StandardCharsets.UTF_8);
            String fullUrl = url + queryParams;
            
            // 添加文件系统名称到请求头
            String response = HttpClientUtil.doGetWithHeader(httpClient, fullUrl, "fileSystemName", defaultFileSystemName);
            
            if (response != null && !response.contains("error")) {
                com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                StatInfo statInfo = mapper.readValue(response, StatInfo.class);
                if (statInfo == null) {
                    throw new IOException("获取文件状态失败: 返回为空");
                }
                System.out.println("成功获取文件状态: fileSystemName=" + defaultFileSystemName + ", path=" + statInfo.getPath() + ", size=" + statInfo.getSize() + ", type=" + statInfo.getType());
                return statInfo;
            } else {
                throw new IOException("获取文件状态失败: " + path + ", 响应: " + response);
            }
            
        } catch (Exception e) {
            System.err.println("获取文件状态异常: fileSystemName=" + defaultFileSystemName + ", path=" + path + ", 错误: " + e.getMessage());
            throw new IOException("获取文件状态失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * 列出目录内容
     */
    @Override
    public List<StatInfo> listFileStats(String path) throws IOException {
        try {
            System.out.println("列出目录内容: fileSystemName=" + defaultFileSystemName + ", path=" + path);
            
            // 从MetaServer列出目录内容
            MetaServerMsg metaServer = getMetaServer();
            String url = "http://" + metaServer.getHost() + ":" + metaServer.getPort() + "/listdir";
            
            String queryParams = "?path=" + URLEncoder.encode(path, StandardCharsets.UTF_8);
            String fullUrl = url + queryParams;
            
            // 添加文件系统名称到请求头
            String response = HttpClientUtil.doGetWithHeader(httpClient, fullUrl, "fileSystemName", defaultFileSystemName);
            
            if (response != null && !response.contains("error")) {
                com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                java.util.List<StatInfo> list = mapper.readValue(
                        response,
                        new com.fasterxml.jackson.core.type.TypeReference<java.util.List<StatInfo>>(){}
                );
                if (list == null) list = new java.util.ArrayList<>();
                System.out.println("成功列出目录内容: fileSystemName=" + defaultFileSystemName + ", path=" + path + ", count=" + list.size());
                return list;
            } else {
                throw new IOException("列出目录内容失败: " + path + ", 响应: " + response);
            }
            
        } catch (Exception e) {
            System.err.println("列出目录内容异常: fileSystemName=" + defaultFileSystemName + ", path=" + path + ", 错误: " + e.getMessage());
            throw new IOException("列出目录内容失败: " + e.getMessage(), e);
        }
    }

    /**
     * 检查文件是否存在
     */
    @Override
    public boolean exists(String path) throws IOException {
        try {
            System.out.println("检查文件是否存在: fileSystemName=" + defaultFileSystemName + ", path=" + path);
            
            // 从MetaServer检查文件是否存在
            MetaServerMsg metaServer = getMetaServer();
            String url = "http://" + metaServer.getHost() + ":" + metaServer.getPort() + "/exists";
            
            String queryParams = "?path=" + URLEncoder.encode(path, StandardCharsets.UTF_8);
            String fullUrl = url + queryParams;
            
            // 添加文件系统名称到请求头
            String response = HttpClientUtil.doGetWithHeader(httpClient, fullUrl, "fileSystemName", defaultFileSystemName);
            
            if (response != null && !response.contains("error")) {
                // 检查响应中的exists字段
                boolean exists = response.contains("\"exists\":true");
                System.out.println("文件存在检查结果: fileSystemName=" + defaultFileSystemName + ", path=" + path + ", exists=" + exists);
                return exists;
            } else {
                throw new IOException("检查文件存在性失败: " + path + ", 响应: " + response);
            }
            
        } catch (Exception e) {
            System.err.println("检查文件存在性异常: fileSystemName=" + defaultFileSystemName + ", path=" + path + ", 错误: " + e.getMessage());
            throw new IOException("检查文件存在性失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * 获取集群信息
     */
    @Override
    public ClusterInfo getClusterInfo() throws IOException {
        try {
            System.out.println("获取集群信息: fileSystemName=" + defaultFileSystemName);
            
            // 从MetaServer获取集群信息
            MetaServerMsg metaServer = getMetaServer();
            String url = "http://" + metaServer.getHost() + ":" + metaServer.getPort() + "/cluster/info";
            
            String response = HttpClientUtil.doGetWithHeader(httpClient, url, "fileSystemName", defaultFileSystemName);
            
            if (response == null || response.contains("error")) {
                throw new IOException("获取集群信息失败, 响应: " + response);
            }
            
            // 解析JSON并映射到ClusterInfo
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            @SuppressWarnings("unchecked")
            java.util.Map<String, Object> payload = mapper.readValue(response, java.util.Map.class);

            ClusterInfo clusterInfo = new ClusterInfo();
            
            // 1) MetaServer: leader / followers
            Object metaServersObj = payload.get("metaServers");
            if (metaServersObj instanceof java.util.Map) {
                java.util.Map<String, Object> metaServers = (java.util.Map<String, Object>) metaServersObj;
                // leaderAddress -> masterMetaServer
                Object leaderAddrObj = metaServers.get("leaderAddress");
                if (leaderAddrObj instanceof String) {
                    String leaderAddr = (String) leaderAddrObj;
                    MetaServerMsg master = new MetaServerMsg();
                    if (leaderAddr.contains(":")) {
                        String[] parts = leaderAddr.split(":", 2);
                        master.setHost(parts[0]);
                        try { master.setPort(Integer.parseInt(parts[1])); } catch (NumberFormatException ignore) { master.setPort(8000); }
                    } else {
                        master.setHost(leaderAddr);
                        master.setPort(8000);
                    }
                    clusterInfo.setMasterMetaServer(master);
                }
                // followerAddresses -> slaveMetaServer
                Object followersObj = metaServers.get("followerAddresses");
                if (followersObj instanceof java.util.List) {
                    java.util.List<?> arr = (java.util.List<?>) followersObj;
                    java.util.List<MetaServerMsg> followers = new java.util.ArrayList<>();
                    for (Object o : arr) {
                        if (o instanceof String) {
                            String addr = (String) o;
                            MetaServerMsg m = new MetaServerMsg();
                            if (addr.contains(":")) {
                                String[] parts = addr.split(":", 2);
                                m.setHost(parts[0]);
                                try { m.setPort(Integer.parseInt(parts[1])); } catch (NumberFormatException ignore) { m.setPort(8000); }
                            } else { m.setHost(addr); m.setPort(8000); }
                            followers.add(m);
                        }
                    }
                    clusterInfo.setSlaveMetaServer(followers);
                }
            }

            // 2) DataServers -> dataServer
            Object dsObj = payload.get("dataServers");
            if (dsObj instanceof java.util.List) {
                java.util.List<?> arr = (java.util.List<?>) dsObj;
                java.util.List<DataServerMsg> dsList = new java.util.ArrayList<>();
                for (Object o : arr) {
                    if (o instanceof java.util.Map) {
                        java.util.Map<String, Object> m = (java.util.Map<String, Object>) o;
                        DataServerMsg d = new DataServerMsg();
                        Object host = m.get("host");
                        if (host == null) host = m.get("ip");
                        d.setHost(host != null ? String.valueOf(host) : "");
                        Object port = m.get("port");
                        if (port instanceof Number) d.setPort(((Number) port).intValue());
                        else if (port != null) { try { d.setPort(Integer.parseInt(String.valueOf(port))); } catch (Exception ignore) { d.setPort(0); } }
                        Object total = m.get("totalCapacity");
                        if (total instanceof Number) d.setCapacity(((Number) total).longValue());
                        else if (total != null) { try { d.setCapacity(Long.parseLong(String.valueOf(total))); } catch (Exception ignore) { d.setCapacity(0); } }
                        Object used = m.get("usedCapacity");
                        if (used instanceof Number) d.setUseCapacity(((Number) used).longValue());
                        else if (used != null) { try { d.setUseCapacity(Long.parseLong(String.valueOf(used))); } catch (Exception ignore) { d.setUseCapacity(0); } }
                        Object fileTotal = m.get("fileTotal");
                        if (fileTotal instanceof Number) d.setFileTotal(((Number) fileTotal).longValue());
                        dsList.add(d);
                    }
                }
                clusterInfo.setDataServer(dsList);
            }

            // 3) replicaDistribution
            Object rep = payload.get("replicaDistribution");
            if (rep instanceof java.util.Map) {
                clusterInfo.setReplicaDistribution((java.util.Map<String, Object>) rep);
            }

            // 4) healthStatus
            Object health = payload.get("healthStatus");
            if (health instanceof java.util.Map) {
                clusterInfo.setHealthStatus((java.util.Map<String, Object>) health);
            }

            System.out.println("成功获取并解析集群信息: fileSystemName=" + defaultFileSystemName);
            return clusterInfo;
        } catch (Exception e) {
            System.err.println("获取集群信息异常: fileSystemName=" + defaultFileSystemName + ", 错误: " + e.getMessage());
            throw new IOException("获取集群信息失败: " + e.getMessage(), e);
        }
    }

    /**
     * 获取文件系统统计信息
     */
    public Map<String, Object> getFileSystemStats() throws IOException {
        try {
            System.out.println("获取文件系统统计信息: fileSystemName=" + defaultFileSystemName);
            
            // 从MetaServer获取文件系统统计信息
            MetaServerMsg metaServer = getMetaServer();
            String url = "http://" + metaServer.getHost() + ":" + metaServer.getPort() + "/filesystem/stats";
            
            // 添加文件系统名称到请求头
            String response = HttpClientUtil.doGetWithHeader(httpClient, url, "fileSystemName", defaultFileSystemName);
            
            if (response != null && !response.contains("error")) {
                com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                @SuppressWarnings("unchecked")
                java.util.Map<String, Object> stats = mapper.readValue(response, java.util.Map.class);
                System.out.println("成功获取文件系统统计信息: fileSystemName=" + defaultFileSystemName + ", totalFiles=" + stats.get("totalFiles"));
                return stats;
            } else {
                throw new IOException("获取文件系统统计信息失败, 响应: " + response);
            }
            
        } catch (Exception e) {
            System.err.println("获取文件系统统计信息异常: fileSystemName=" + defaultFileSystemName + ", 错误: " + e.getMessage());
            throw new IOException("获取文件系统统计信息失败: " + e.getMessage(), e);
        }
    }

    /**
     * 获取全局统计信息
     */
    public Map<String, Object> getGlobalStats() throws IOException {
        try {
            System.out.println("获取全局统计信息: fileSystemName=" + defaultFileSystemName);
            
            // 从MetaServer获取全局统计信息
            MetaServerMsg metaServer = getMetaServer();
            String url = "http://" + metaServer.getHost() + ":" + metaServer.getPort() + "/filesystem/global-stats";
            
            String response = HttpClientUtil.doGetWithHeader(httpClient, url, "fileSystemName", defaultFileSystemName);
            
            if (response != null && !response.contains("error")) {
                com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                @SuppressWarnings("unchecked")
                java.util.Map<String, Object> stats = mapper.readValue(response, java.util.Map.class);
                System.out.println("成功获取全局统计信息: totalFiles=" + stats.get("totalFiles"));
                return stats;
            } else {
                throw new IOException("获取全局统计信息失败, 响应: " + response);
            }
            
        } catch (Exception e) {
            System.err.println("获取全局统计信息异常: fileSystemName=" + defaultFileSystemName + ", 错误: " + e.getMessage());
            throw new IOException("获取全局统计信息失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * 写入文件内容
     */
    public void writeFile(String path, byte[] data) throws IOException {
        try {
            System.out.println("写入文件: fileSystemName=" + defaultFileSystemName + ", path=" + path + ", size=" + data.length);
            
            // 从MetaServer写入文件
            MetaServerMsg metaServer = getMetaServer();
            String url = "http://" + metaServer.getHost() + ":" + metaServer.getPort() + "/write";
            
            String queryParams = "?path=" + URLEncoder.encode(path, StandardCharsets.UTF_8) + "&offset=0&length=" + data.length;
            String fullUrl = url + queryParams;
            
            // 添加文件系统名称到请求头
            String response = HttpClientUtil.doPostWithHeader(httpClient, fullUrl, data, "fileSystemName", defaultFileSystemName);
            
            if (response != null && !response.contains("error")) {
                System.out.println("成功写入文件: fileSystemName=" + defaultFileSystemName + ", path=" + path);
            } else {
                throw new IOException("写入文件失败: " + path + ", 响应: " + response);
            }
            
        } catch (Exception e) {
            System.err.println("写入文件异常: fileSystemName=" + defaultFileSystemName + ", path=" + path + ", 错误: " + e.getMessage());
            throw new IOException("写入文件失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * 获取HTTP客户端实例
     */
    public HttpClient getHttpClient() {
        return httpClient;
    }
    
    /**
     * 关闭资源
     */
    public void close() {
        try {
            if (zkUtil != null) {
                zkUtil.close();
            }
            // HttpClient不需要手动关闭，它会自动管理连接池
        } catch (Exception e) {
            // 忽略关闭时的异常
        }
    }

    
}
