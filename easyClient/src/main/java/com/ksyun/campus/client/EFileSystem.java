package com.ksyun.campus.client;

import com.ksyun.campus.client.domain.ClusterInfo;
import com.ksyun.campus.client.domain.StatInfo;
import com.ksyun.campus.client.util.HttpClientUtil;
import com.ksyun.campus.client.util.ZkUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.core5.http.io.SocketConfig;
import org.apache.hc.core5.util.Timeout;

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
            List<String> addresses = zkUtil.getMetaServerAddresses();
            if (addresses != null && !addresses.isEmpty()) {
                return addresses.get(0);
            }
        } catch (Exception e) {
            // 使用默认地址
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
        if (!path.endsWith("/")) {
            path = path + "/"; // 确保目录路径以/结尾
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
        if (!path.endsWith("/")) {
            path = path + "/"; // 确保目录路径以/结尾
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
            if (response != null && !response.contains("error")) {
                return objectMapper.readValue(response, ClusterInfo.class);
            }
            return null;
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
        if (data.length == 0) {
            throw new IllegalArgumentException("文件数据不能为空");
        }
        
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
