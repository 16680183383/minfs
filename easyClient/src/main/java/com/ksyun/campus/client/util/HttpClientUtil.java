package com.ksyun.campus.client.util;

import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.DefaultHttpRequestRetryStrategy;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.socket.ConnectionSocketFactory;
import org.apache.hc.client5.http.socket.PlainConnectionSocketFactory;
import org.apache.hc.core5.http.config.Registry;
import org.apache.hc.core5.http.config.RegistryBuilder;
import org.apache.hc.core5.http.io.SocketConfig;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.apache.hc.core5.http.io.entity.EntityUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.hc.core5.http.ParseException;

public class HttpClientUtil {
    private static HttpClient httpClient;
    
    public static HttpClient createHttpClient(HttpClientConfig config) {

        int socketSendBufferSizeHint = config.getSocketSendBufferSizeHint();
        int socketReceiveBufferSizeHint = config.getSocketReceiveBufferSizeHint();
        int buffersize = 0;
        if (socketSendBufferSizeHint > 0 || socketReceiveBufferSizeHint > 0) {
            buffersize = Math.max(socketSendBufferSizeHint, socketReceiveBufferSizeHint);
        }
        SocketConfig soConfig = SocketConfig.custom()
                .setTcpNoDelay(true).setSndBufSize(buffersize)
                .setSoTimeout(Timeout.ofMilliseconds(config.getSocketTimeOut()))
                .build();
        ConnectionConfig coConfig = ConnectionConfig.custom()
                .setConnectTimeout(Timeout.ofMilliseconds(config.getConnectionTimeOut()))
                .build();
        RequestConfig reConfig;
        RequestConfig.Builder builder= RequestConfig.custom()
                .setConnectTimeout(Timeout.ofMilliseconds(config.getConnectionTimeOut()))
                .setResponseTimeout(Timeout.ofMilliseconds(config.getSocketTimeOut()))
                ;
        reConfig=builder.build();
        PlainConnectionSocketFactory sf = PlainConnectionSocketFactory.getSocketFactory();
        Registry<ConnectionSocketFactory> r = RegistryBuilder.<ConnectionSocketFactory> create().register("http", sf).build();
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager(r);
        connectionManager.setMaxTotal(config.getMaxConnections());
        connectionManager.setDefaultMaxPerRoute(connectionManager.getMaxTotal());
        connectionManager.setDefaultConnectionConfig(coConfig);
        connectionManager.setDefaultSocketConfig(soConfig);


        httpClient = HttpClients.custom().setConnectionManager(connectionManager).setRetryStrategy(new DefaultHttpRequestRetryStrategy(config.getMaxRetry(), TimeValue.ZERO_MILLISECONDS))
                .setDefaultRequestConfig(reConfig)
                .build();
        return httpClient;

    }
    
    /**
     * 执行GET请求
     */
    public static String doGet(HttpClient client, String url) throws IOException, ParseException {
        ClassicHttpRequest request = ClassicRequestBuilder.get(url).build();
        try (ClassicHttpResponse response = client.executeOpen(null, request, null)) {
            HttpEntity entity = response.getEntity();
            return entity != null ? EntityUtils.toString(entity, StandardCharsets.UTF_8) : null;
        }
    }
    
    /**
     * 执行带请求头的GET请求
     */
    public static String doGetWithHeader(HttpClient client, String url, String headerName, String headerValue) throws IOException, ParseException {
        ClassicHttpRequest request = ClassicRequestBuilder.get(url)
                .addHeader(headerName, headerValue)
                .build();
        try (ClassicHttpResponse response = client.executeOpen(null, request, null)) {
            HttpEntity entity = response.getEntity();
            return entity != null ? EntityUtils.toString(entity, StandardCharsets.UTF_8) : null;
        }
    }
    
    /**
     * 执行POST请求，发送字节数组
     */
    public static String doPost(HttpClient client, String url, byte[] data) throws IOException, ParseException {
        ClassicHttpRequest request = ClassicRequestBuilder.post(url)
                .setEntity(new ByteArrayEntity(data, null))
                .build();
        try (ClassicHttpResponse response = client.executeOpen(null, request, null)) {
            HttpEntity entity = response.getEntity();
            return entity != null ? EntityUtils.toString(entity, StandardCharsets.UTF_8) : null;
        }
    }
    
    /**
     * 执行带请求头的POST请求，发送字节数组
     */
    public static String doPostWithHeader(HttpClient client, String url, byte[] data, String headerName, String headerValue) throws IOException, ParseException {
        ClassicHttpRequest request = ClassicRequestBuilder.post(url)
                .setEntity(new ByteArrayEntity(data, null))
                .addHeader(headerName, headerValue)
                .build();
        try (ClassicHttpResponse response = client.executeOpen(null, request, null)) {
            HttpEntity entity = response.getEntity();
            return entity != null ? EntityUtils.toString(entity, StandardCharsets.UTF_8) : null;
        }
    }
    
    /**
     * 执行DELETE请求
     */
    public static String doDelete(HttpClient client, String url) throws IOException, ParseException {
        ClassicHttpRequest request = ClassicRequestBuilder.delete(url).build();
        try (ClassicHttpResponse response = client.executeOpen(null, request, null)) {
            HttpEntity entity = response.getEntity();
            return entity != null ? EntityUtils.toString(entity, StandardCharsets.UTF_8) : null;
        }
    }
    
    /**
     * 构建带查询参数的URL
     */
    public static String buildUrl(String baseUrl, Map<String, String> params) {
        if (params == null || params.isEmpty()) {
            return baseUrl;
        }
        
        StringBuilder urlBuilder = new StringBuilder(baseUrl);
        if (!baseUrl.contains("?")) {
            urlBuilder.append("?");
        } else if (!baseUrl.endsWith("&") && !baseUrl.endsWith("?")) {
            urlBuilder.append("&");
        }
        
        boolean first = true;
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (!first) {
                urlBuilder.append("&");
            }
            urlBuilder.append(entry.getKey()).append("=").append(entry.getValue());
            first = false;
        }
        
        return urlBuilder.toString();
    }
}
