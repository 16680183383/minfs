package com.example.easyclient;

import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class FSOutputStream extends OutputStream {
    private final String dataServerUrl;
    private final String filePath;
    private final byte[] buffer; // 64MB缓冲区（与服务端块大小一致）
    private int bufferPos;
    private long currentOffset;
    private final RestTemplate restTemplate;

    public FSOutputStream(String dataServerUrl, String filePath) {
        this.dataServerUrl = dataServerUrl;
        this.filePath = filePath;
        this.buffer = new byte[64 * 1024 * 1024]; // 64MB
        this.bufferPos = 0;
        this.currentOffset = 0;
        this.restTemplate = new RestTemplate(); // 使用Spring的RestTemplate
    }

    @Override
    public void write(int b) throws IOException {
        if (bufferPos >= buffer.length) {
            flushBuffer();
        }
        buffer[bufferPos++] = (byte) b;
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        int remaining = len;
        int pos = off;
        while (remaining > 0) {
            int copyLen = Math.min(remaining, buffer.length - bufferPos);
            System.arraycopy(b, pos, buffer, bufferPos, copyLen);
            bufferPos += copyLen;
            pos += copyLen;
            remaining -= copyLen;

            if (bufferPos == buffer.length) {
                flushBuffer();
            }
        }
    }

    /**
     * 刷新缓冲区到服务端（调用/write接口）
     */
    private void flushBuffer() throws IOException {
        if (bufferPos == 0) {
            return;
        }

        try {
            // 编码文件路径
            String encodedPath = URLEncoder.encode(filePath, StandardCharsets.UTF_8.name());
            String url = dataServerUrl + "/write?path=" + encodedPath + "&offset=" + currentOffset + "&length=" + bufferPos;

            // 准备请求数据（截取缓冲区中的有效数据）
            byte[] data = Arrays.copyOfRange(buffer, 0, bufferPos);

            // 设置请求头（与服务端DataService的syncToReplica方法匹配）
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
            headers.set("fileSystemName", "minfs");
            HttpEntity<byte[]> request = new HttpEntity<>(data, headers);

            // 发送POST请求
            ResponseEntity<String> response = restTemplate.postForEntity(url, request, String.class);

            // 处理响应状态
            if (response.getStatusCode() != HttpStatus.OK) {
                throw new IOException("写入失败，服务端响应：" + response.getStatusCode());
            }

            // 更新偏移量并重置缓冲区
            currentOffset += bufferPos;
            bufferPos = 0;
        } catch (Exception e) {
            throw new IOException("写入数据异常", e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            flushBuffer(); // 关闭前确保剩余数据写入
        } finally {
            super.close();
        }
    }
}