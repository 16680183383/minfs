package com.ksyun.campus.client;

import com.ksyun.campus.client.domain.ClusterInfo;
import com.ksyun.campus.client.domain.StatInfo;
import com.ksyun.campus.client.util.HttpClientUtil;
import com.ksyun.campus.client.util.ZkUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * easyClient 文件系统实现类
 * 提供完整的分布式文件系统操作接口
 */
public class EFileSystem extends FileSystem {
    
    private ZkUtil zkUtil;
    private HttpClient httpClient;
    private ObjectMapper objectMapper;
    private String defaultMetaServerAddress;
    
    /**
     * 构造函数
     * @param defaultFileSystemName 默认文件系统名称
     */
    public EFileSystem(String defaultFileSystemName) {
        super.defaultFileSystemName = defaultFileSystemName;
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
            
            // 初始化JSON处理器
            objectMapper = new ObjectMapper();
            objectMapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
            objectMapper.disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
            
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
    
    @Override
    public FSInputStream open(String path) throws IOException {
        // 参数验证
        if (path == null || path.trim().isEmpty()) {
            throw new IllegalArgumentException("文件路径不能为空");
        }
        if (!path.startsWith("/")) {
            throw new IllegalArgumentException("文件路径必须以/开头: " + path);
        }
        
        try {
            // 获取文件状态
            StatInfo statInfo = getFileStats(path);
            if (statInfo == null) {
                throw new IOException("文件不存在: " + path);
            }
            
            if (statInfo.getType() != com.ksyun.campus.client.domain.FileType.File) {
                throw new IOException("路径不是文件: " + path);
            }
            
            return new FSInputStream(statInfo, this);
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("打开文件失败: " + path, e);
        }
    }
    
    @Override
    public FSOutputStream create(String path) throws IOException {
        // 参数验证
        if (path == null || path.trim().isEmpty()) {
            throw new IllegalArgumentException("文件路径不能为空");
        }
        if (!path.startsWith("/")) {
            throw new IllegalArgumentException("文件路径必须以/开头: " + path);
        }
        if (path.endsWith("/")) {
            throw new IllegalArgumentException("文件路径不能以/结尾: " + path);
        }
        
        try {
            // 检查文件是否已存在
            if (exists(path)) {
                throw new IOException("文件已存在: " + path);
            }
            
            // 创建空文件
            String metaServerAddress = getMetaServerAddress();
            String url = "http://" + metaServerAddress + "/create?path=" + URLEncoder.encode(path, StandardCharsets.UTF_8);
            
            String response = HttpClientUtil.doGet(httpClient, url);
            if (response == null || response.contains("error")) {
                throw new IOException("创建文件失败: " + path + ", 响应: " + response);
            }
            
            return new FSOutputStream(path, this);
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("创建文件失败: " + path, e);
        }
    }
    
    @Override
    public boolean mkdir(String path) throws IOException {
        // 参数验证
        if (path == null || path.trim().isEmpty()) {
            throw new IllegalArgumentException("目录路径不能为空");
        }
        if (!path.startsWith("/")) {
            throw new IllegalArgumentException("目录路径必须以/开头: " + path);
        }
        if (path.endsWith("/")) {
            throw new IllegalArgumentException("目录路径不能以/结尾: " + path);
        }
        
        try {
            // 检查目录是否已存在
            if (exists(path)) {
                return true; // 目录已存在，返回成功
            }
            
            String metaServerAddress = getMetaServerAddress();
            String url = "http://" + metaServerAddress + "/mkdir?path=" + URLEncoder.encode(path, StandardCharsets.UTF_8);
            
            String response = HttpClientUtil.doGet(httpClient, url);
            return response != null && !response.contains("error");
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("创建目录失败: " + path, e);
        }
    }
    
    @Override
    public boolean delete(String path) throws IOException {
        // 参数验证
        if (path == null || path.trim().isEmpty()) {
            throw new IllegalArgumentException("路径不能为空");
        }
        if (!path.startsWith("/")) {
            throw new IllegalArgumentException("路径必须以/开头: " + path);
        }
        
        try {
            // 检查路径是否存在
            if (!exists(path)) {
                return false; // 路径不存在，返回false
            }
            
            String metaServerAddress = getMetaServerAddress();
            String url = "http://" + metaServerAddress + "/delete?path=" + URLEncoder.encode(path, StandardCharsets.UTF_8);
            
            String response = HttpClientUtil.doDelete(httpClient, url);
            return response != null && !response.contains("error");
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("删除失败: " + path, e);
        }
    }
    
    @Override
    public StatInfo getFileStats(String path) throws IOException {
        // 参数验证
        if (path == null || path.trim().isEmpty()) {
            throw new IllegalArgumentException("文件路径不能为空");
        }
        if (!path.startsWith("/")) {
            throw new IllegalArgumentException("文件路径必须以/开头: " + path);
        }
        
        try {
            String metaServerAddress = getMetaServerAddress();
            String url = "http://" + metaServerAddress + "/stats?path=" + URLEncoder.encode(path, StandardCharsets.UTF_8);
            
            String response = HttpClientUtil.doGet(httpClient, url);
            if (response != null && !response.contains("error")) {
                return objectMapper.readValue(response, StatInfo.class);
            }
            return null;
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IOException("解析文件状态信息失败: " + path, e);
        } catch (Exception e) {
            throw new IOException("获取文件状态失败: " + path, e);
        }
    }
    
    @Override
    public List<StatInfo> listFileStats(String path) throws IOException {
        // 参数验证
        if (path == null || path.trim().isEmpty()) {
            throw new IllegalArgumentException("目录路径不能为空");
        }
        if (!path.startsWith("/")) {
            throw new IllegalArgumentException("目录路径必须以/开头: " + path);
        }
        if (path.endsWith("/")) {
            throw new IllegalArgumentException("目录路径不能以/结尾: " + path);
        }
        
        try {
            String metaServerAddress = getMetaServerAddress();
            String url = "http://" + metaServerAddress + "/listdir?path=" + URLEncoder.encode(path, StandardCharsets.UTF_8);
            
            String response = HttpClientUtil.doGet(httpClient, url);
            if (response != null && !response.contains("error")) {
                return objectMapper.readValue(response, 
                    objectMapper.getTypeFactory().constructCollectionType(List.class, StatInfo.class));
            }
            return null;
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IOException("解析目录列表信息失败: " + path, e);
        } catch (Exception e) {
            throw new IOException("列出文件失败: " + path, e);
        }
    }
    
    @Override
    public ClusterInfo getClusterInfo() throws IOException {
        try {
            String metaServerAddress = getMetaServerAddress();
            String url = "http://" + metaServerAddress + "/cluster/info";
            String response = HttpClientUtil.doGet(httpClient, url);
            if (response == null || response.contains("error")) {
                return null;
            }
            java.util.Map<?, ?> raw = objectMapper.readValue(response, java.util.Map.class);
            ClusterInfo ci = new ClusterInfo();

            // metaServers: leader + followers
            Object metaServersObj = raw.get("metaServers");
            if (metaServersObj instanceof java.util.Map) {
                java.util.Map<?, ?> ms = (java.util.Map<?, ?>) metaServersObj;
                Object leaderAddrObj = ms.get("leaderAddress");
                String leaderAddr = leaderAddrObj == null ? null : String.valueOf(leaderAddrObj);
                if (leaderAddr != null && leaderAddr.contains(":")) {
                    String[] hp = leaderAddr.split(":", 2);
                    com.ksyun.campus.client.domain.MetaServerMsg master = new com.ksyun.campus.client.domain.MetaServerMsg();
                    master.setHost(hp[0]);
                    try { master.setPort(Integer.parseInt(hp[1])); } catch (Exception ignore) { master.setPort(0); }
                    ci.setMasterMetaServer(master);
                }
                // followers -> List<MetaServerMsg>
                java.util.List<com.ksyun.campus.client.domain.MetaServerMsg> slaves = new java.util.ArrayList<>();
                Object followersObj = ms.get("followerAddresses");
                if (followersObj instanceof java.util.List) {
                    for (Object f : (java.util.List<?>) followersObj) {
                        String addr = String.valueOf(f);
                        if (addr.contains(":")) {
                            String[] hp2 = addr.split(":", 2);
                            com.ksyun.campus.client.domain.MetaServerMsg slave = new com.ksyun.campus.client.domain.MetaServerMsg();
                            slave.setHost(hp2[0]);
                            try { slave.setPort(Integer.parseInt(hp2[1])); } catch (Exception ignore) { slave.setPort(0); }
                            slaves.add(slave);
                        }
                    }
                }
                ci.setSlaveMetaServer(slaves);
            }

            // dataServers
            Object dsObj = raw.get("dataServers");
            java.util.List<com.ksyun.campus.client.domain.DataServerMsg> dsList = new java.util.ArrayList<>();
            if (dsObj instanceof java.util.List) {
                for (Object o : (java.util.List<?>) dsObj) {
                    if (!(o instanceof java.util.Map)) continue;
                    java.util.Map<?, ?> m = (java.util.Map<?, ?>) o;
                    String host = null; int port = 0;
                    Object addressObj = m.get("address");
                    if (addressObj != null) {
                        String addr = String.valueOf(addressObj);
                        if (addr.contains(":")) {
                            String[] hp = addr.split(":", 2);
                            host = hp[0];
                            try { port = Integer.parseInt(hp[1]); } catch (Exception ignore) { port = 0; }
                        }
                    }
                    Object totalObj = m.get("totalCapacity");
                    Object usedObj = m.get("usedCapacity");
                    long cap = totalObj instanceof Number ? ((Number) totalObj).longValue() : 0L;
                    long use = usedObj instanceof Number ? ((Number) usedObj).longValue() : 0L;
                    // 兼容 fileTotal 字段
                    Object fileTotalObj = m.get("fileTotal");
                    if (!(fileTotalObj instanceof Number)) {
                        fileTotalObj = m.get("files");
                    }
                    long fileTotal = fileTotalObj instanceof Number ? ((Number) fileTotalObj).longValue() : 0L;
                    com.ksyun.campus.client.domain.DataServerMsg dm = new com.ksyun.campus.client.domain.DataServerMsg();
                    dm.setHost(host);
                    dm.setPort(port);
                    dm.setCapacity(cap);
                    dm.setUseCapacity(use);
                    dm.setFileTotal(fileTotal);
                    dsList.add(dm);
                }
            }
            ci.setDataServer(dsList);

            // 直接传递 replicaDistribution 与 healthStatus
            Object rep = raw.get("replicaDistribution");
            if (rep instanceof java.util.Map) {
                ci.setReplicaDistribution((java.util.Map<String, Object>) rep);
            }
            Object health = raw.get("healthStatus");
            if (health instanceof java.util.Map) {
                ci.setHealthStatus((java.util.Map<String, Object>) health);
            }
            return ci;
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IOException("解析集群信息失败", e);
        } catch (Exception e) {
            throw new IOException("获取集群信息失败", e);
        }
    }
    
    @Override
    public boolean exists(String path) throws IOException {
        // 参数验证
        if (path == null || path.trim().isEmpty()) {
            return false;
        }
        if (!path.startsWith("/")) {
            return false;
        }
        
        try {
            StatInfo statInfo = getFileStats(path);
            return statInfo != null;
        } catch (Exception e) {
            return false; // 任何异常都认为文件不存在
        }
    }
    
    /**
     * 写入文件内容
     * @param path 文件路径
     * @param data 文件数据
     * @throws IOException 写入失败时抛出
     */
    public void writeFile(String path, byte[] data) throws IOException {
        // 参数验证
        if (path == null || path.trim().isEmpty()) {
            throw new IllegalArgumentException("文件路径不能为空");
        }
        if (!path.startsWith("/")) {
            throw new IllegalArgumentException("文件路径必须以/开头: " + path);
        }
        if (data == null) {
            throw new IllegalArgumentException("文件数据不能为空");
        }
        // 允许写入空文件，所以移除对data.length == 0的检查
        
        try {
            String metaServerAddress = getMetaServerAddress();
            String url = "http://" + metaServerAddress + "/write?path=" + URLEncoder.encode(path, StandardCharsets.UTF_8) + "&offset=0&length=" + data.length;
            
            String response = HttpClientUtil.doPost(httpClient, url, data);
            if (response == null || response.contains("error")) {
                throw new IOException("写入文件失败: " + path + ", 响应: " + response);
            }
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("写入文件失败: " + path, e);
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
