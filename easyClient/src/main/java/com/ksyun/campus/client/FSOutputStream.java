package com.ksyun.campus.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * 文件系统输出流
 * 用于写入文件数据到分布式文件系统
 */
public class FSOutputStream extends OutputStream {
    
    private final String filePath;
    private final EFileSystem fileSystem;
    private final ByteArrayOutputStream buffer;
    private boolean closed = false;
    
    /**
     * 构造函数
     * @param filePath 文件路径
     * @param fileSystem 文件系统实例
     */
    public FSOutputStream(String filePath, EFileSystem fileSystem) {
        this.filePath = filePath;
        this.fileSystem = fileSystem;
        this.buffer = new ByteArrayOutputStream();
    }
    
    @Override
    public void write(int b) throws IOException {
        if (closed) {
            throw new IOException("输出流已关闭");
        }
        buffer.write(b);
    }
    
    @Override
    public void write(byte[] b) throws IOException {
        if (closed) {
            throw new IOException("输出流已关闭");
        }
        buffer.write(b);
    }
    
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (closed) {
            throw new IOException("输出流已关闭");
        }
        buffer.write(b, off, len);
    }
    
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        
        try {
            // 将缓冲的数据写入文件系统
            byte[] data = buffer.toByteArray();
            if (data.length > 0) {
                fileSystem.writeFile(filePath, data);
            }
        } finally {
            closed = true;
            buffer.close();
        }
    }
    
    @Override
    public void flush() throws IOException {
        if (closed) {
            throw new IOException("输出流已关闭");
        }
        buffer.flush();
    }
    
    /**
     * 获取文件路径
     */
    public String getFilePath() {
        return filePath;
    }
    
    /**
     * 获取已写入的数据大小
     */
    public int getWrittenSize() {
        return buffer.size();
    }
}
