package com.ksyun.campus.client;

import com.ksyun.campus.client.domain.MetaServerMsg;
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
            // 参数验证
            if (statInfo == null) {
                throw new IOException("文件状态信息为空");
            }
            if (statInfo.getPath() == null || statInfo.getPath().trim().isEmpty()) {
                throw new IOException("文件路径为空");
            }
            
            // 若元数据 size==0，直接返回空内容（按你的要求保留早退逻辑）
            if (statInfo.getSize() == 0) {
                fileData = new byte[0];
                dataLoaded = true;
                return;
            }
            
            // 从MetaServer读取文件数据（支持分块读取）
            MetaServerMsg metaServer = fileSystem.getMetaServer();
            String url = "http://" + metaServer.getHost() + ":" + metaServer.getPort() + "/read";
            
            String queryParams = "?path=" + java.net.URLEncoder.encode(statInfo.getPath(), "UTF-8") + "&offset=0&length=-1";
            String fullUrl = url + queryParams;
            
            // 添加文件系统名称到请求头
            String response = HttpClientUtil.doGetWithHeader(httpClient, fullUrl, "fileSystemName", fileSystem.getFileSystemName());
            if (response == null) {
                throw new IOException("从MetaServer读取文件失败: 响应为空");
            }
            if (response.contains("error")) {
                throw new IOException("从MetaServer读取文件失败: " + response);
            }
            
            fileData = response.getBytes("UTF-8");
            if (fileData == null) {
                throw new IOException("文件数据为空");
            }
            
            dataLoaded = true;
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("加载文件数据失败: " + (statInfo != null ? statInfo.getPath() : "未知路径"), e);
        }
    }
    
    @Override
    public int read() throws IOException {
        try {
            if (fileData == null) {
                loadFileData();
            }
            
            if (position >= fileData.length) {
                return -1;
            }
            
            return fileData[position++] & 0xFF;
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("读取文件数据失败", e);
        }
    }
    
    @Override
    public int read(byte[] b) throws IOException {
        if (b == null) {
            throw new NullPointerException("缓冲区不能为空");
        }
        return read(b, 0, b.length);
    }
    
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        try {
            // 参数验证
            if (b == null) {
                throw new NullPointerException("缓冲区不能为空");
            }
            if (off < 0 || len < 0 || off + len > b.length) {
                throw new IndexOutOfBoundsException("缓冲区参数无效: off=" + off + ", len=" + len + ", buffer.length=" + b.length);
            }
            
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
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("读取文件数据失败", e);
        }
    }
    
    @Override
    public long skip(long n) throws IOException {
        try {
            if (n < 0) {
                return 0;
            }
            
            if (fileData == null) {
                loadFileData();
            }
            
            long bytesToSkip = Math.min(n, fileData.length - position);
            position += bytesToSkip;
            
            return bytesToSkip;
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("跳过文件数据失败", e);
        }
    }
    
    @Override
    public int available() throws IOException {
        try {
            if (fileData == null) {
                loadFileData();
            }
            
            return Math.max(0, fileData.length - position);
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("获取可用数据失败", e);
        }
    }
    
    @Override
    public void close() throws IOException {
        try {
            // 清理资源
            fileData = null;
            position = 0;
            dataLoaded = false;
        } catch (Exception e) {
            // 忽略关闭时的异常
        }
    }
    
    @Override
    public boolean markSupported() {
        return false;
    }
    
    /**
     * 获取文件大小
     */
    public long getFileSize() {
        try {
            return statInfo != null ? statInfo.getSize() : 0;
        } catch (Exception e) {
            return 0;
        }
    }
    
    /**
     * 获取文件路径
     */
    public String getFilePath() {
        try {
            return statInfo != null ? statInfo.getPath() : null;
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * 重置到文件开头
     */
    public void reset() throws IOException {
        try {
            position = 0;
        } catch (Exception e) {
            throw new IOException("重置文件位置失败", e);
        }
    }
    
    /**
     * 获取当前位置
     */
    public long getPosition() {
        return position;
    }
    
    /**
     * 设置位置
     */
    public void seek(long pos) throws IOException {
        try {
            if (pos < 0) {
                throw new IllegalArgumentException("位置不能为负数: " + pos);
            }
            
            if (fileData == null) {
                loadFileData();
            }
            
            if (pos > fileData.length) {
                throw new IllegalArgumentException("位置超出文件大小: " + pos + " > " + fileData.length);
            }
            
            position = (int) pos;
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("设置文件位置失败", e);
        }
    }
}
