package com.ksyun.campus.client;

import com.ksyun.campus.client.domain.ReplicaData;
import com.ksyun.campus.client.domain.StatInfo;
import com.ksyun.campus.client.util.HttpClientUtil;
import org.apache.hc.client5.http.classic.HttpClient;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * 文件系统输入流
 * 用于从分布式文件系统读取文件数据
 */
public class FSInputStream extends InputStream {
    
    private final StatInfo statInfo;
    private final EFileSystem fileSystem;
    private final HttpClient httpClient;
    private byte[] fileData;
    private int position = 0;
    private boolean dataLoaded = false;
    
    /**
     * 构造函数
     * @param statInfo 文件状态信息
     * @param fileSystem 文件系统实例
     */
    public FSInputStream(StatInfo statInfo, EFileSystem fileSystem) {
        this.statInfo = statInfo;
        this.fileSystem = fileSystem;
        this.httpClient = fileSystem.getHttpClient();
    }
    
    /**
     * 加载文件数据
     */
    private void loadFileData() throws IOException {
        if (dataLoaded) {
            return;
        }
        
        try {
            // 从DataServer读取文件数据
            if (statInfo.getReplicaData() != null && !statInfo.getReplicaData().isEmpty()) {
                // 使用第一个副本
                ReplicaData replica = statInfo.getReplicaData().get(0);
                String replicaAddress = replica.dsNode; // 格式为 ip:port
                String url = "http://" + replicaAddress + "/read?path=" + statInfo.getPath();
                
                String response = HttpClientUtil.doGet(httpClient, url);
                if (response != null && !response.contains("error")) {
                    fileData = response.getBytes("UTF-8");
                } else {
                    throw new IOException("从DataServer读取文件失败: " + statInfo.getPath());
                }
            } else {
                throw new IOException("文件没有可用的副本: " + statInfo.getPath());
            }
            
            dataLoaded = true;
        } catch (Exception e) {
            throw new IOException("加载文件数据失败: " + statInfo.getPath(), e);
        }
    }
    
    @Override
    public int read() throws IOException {
        if (fileData == null) {
            loadFileData();
        }
        
        if (position >= fileData.length) {
            return -1;
        }
        
        return fileData[position++] & 0xFF;
    }
    
    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }
    
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (fileData == null) {
            loadFileData();
        }
        
        if (position >= fileData.length) {
            return -1;
        }
        
        int bytesToRead = Math.min(len, fileData.length - position);
        System.arraycopy(fileData, position, b, off, bytesToRead);
        position += bytesToRead;
        
        return bytesToRead;
    }
    
    @Override
    public long skip(long n) throws IOException {
        if (fileData == null) {
            loadFileData();
        }
        
        long bytesToSkip = Math.min(n, fileData.length - position);
        position += bytesToSkip;
        
        return bytesToSkip;
    }
    
    @Override
    public int available() throws IOException {
        if (fileData == null) {
            loadFileData();
        }
        
        return fileData.length - position;
    }
    
    @Override
    public void close() throws IOException {
        // 清理资源
        fileData = null;
        position = 0;
        dataLoaded = false;
    }
    
    @Override
    public boolean markSupported() {
        return false;
    }
    
    /**
     * 获取文件大小
     */
    public long getFileSize() {
        return statInfo != null ? statInfo.getSize() : 0;
    }
    
    /**
     * 获取文件路径
     */
    public String getFilePath() {
        return statInfo != null ? statInfo.getPath() : null;
    }
}
