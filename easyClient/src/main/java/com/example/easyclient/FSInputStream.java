package com.example.easyclient;

import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class FSInputStream extends InputStream {
    private final String dataServerUrl;
    private final String filePath;
    private long currentOffset;
    private final RestTemplate restTemplate;
    private ByteArrayInputStream buffer;

    public FSInputStream(String dataServerUrl, String filePath) {
        this.dataServerUrl = dataServerUrl;
        this.filePath = filePath;
        this.currentOffset = 0;
        this.restTemplate = new RestTemplate(); // 使用Spring的RestTemplate
        this.buffer = new ByteArrayInputStream(new byte[0]);
    }

    @Override
    public int read() throws IOException {
        byte[] b = new byte[1];
        int len = read(b, 0, 1);
        return len == -1 ? -1 : b[0] & 0xFF;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        // 先从缓存读取
        int bytesRead = buffer.read(b, off, len);
        if (bytesRead != -1) {
            return bytesRead;
        }

        // 缓存为空，从服务器读取（64MB块大小，与服务端一致）
        int readLength = Math.min(len, 64 * 1024 * 1024);
        byte[] data = fetchDataFromServer(currentOffset, readLength);
        if (data.length == 0) {
            return -1; // 读取完毕
        }

        // 更新缓存和偏移量
        buffer = new ByteArrayInputStream(data);
        currentOffset += data.length;
        return buffer.read(b, off, len);
    }

    /**
     * 调用dataServer的/read接口读取数据（使用RestTemplate）
     */
    private byte[] fetchDataFromServer(long offset, int length) throws IOException {
        try {
            // 编码文件路径，避免特殊字符问题
            String encodedPath = URLEncoder.encode(filePath, StandardCharsets.UTF_8.name());
            String url = dataServerUrl + "/read?path=" + encodedPath + "&offset=" + offset + "&length=" + length;

            // 设置请求头（与服务端DataService的接口要求匹配）
            HttpHeaders headers = new HttpHeaders();
            headers.set("fileSystemName", "minfs");
            HttpEntity<Void> request = new HttpEntity<>(headers);

            // 发送GET请求并获取响应
            ResponseEntity<byte[]> response = restTemplate.exchange(
                    url,
                    HttpMethod.GET,
                    request,
                    byte[].class
            );

            // 处理响应状态（替代CloseableHttpResponse的getStatusLine()）
            if (response.getStatusCode() != HttpStatus.OK) {
                throw new IOException("读取失败，服务端响应：" + response.getStatusCode());
            }
            return response.getBody() != null ? response.getBody() : new byte[0];
        } catch (Exception e) {
            throw new IOException("读取数据异常", e);
        }
    }

    @Override
    public void close() throws IOException {
        buffer.close();
        super.close();
    }
}