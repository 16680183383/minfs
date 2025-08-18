package com.example.easyclient;

import com.example.easyclient.domain.ClusterInfo;
import com.example.easyclient.domain.StatInfo;
import com.example.easyclient.util.HttpClientConfig;
import com.example.easyclient.util.HttpClientUtil;
import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * 基类：复用HttpClientUtil工具类，支撑minFS SDK远程调用（符合文档A1-A6、2-18要求）
 */
public abstract class FileSystem {
    private static final Logger LOG = LoggerFactory.getLogger(FileSystem.class);
    protected String defaultFileSystemName;
    // 复用工具类创建的HTTP客户端（替换原静态客户端）
    protected static HttpClient httpClient;
    protected String metaServerBaseUrl = "http://localhost:8000"; // 符合2-16端口范围


    // 静态初始化：通过工具类创建客户端，配置符合文档2-18“超时<30s”要求
    static {
        // 1. 构建配置（超时设为25s<30s，重试2次适配分布式调用稳定性）
        HttpClientConfig httpConfig = new HttpClientConfig()
                .withSocketTimeOut(25000)       // 响应超时25s（符合2-18）
                .withConnectionTimeOut(25000)   // 连接超时25s（符合2-18）
                .withMaxConnections(100)        // 最大连接数100，支撑多副本并发调用
                .withMaxRetry(2);               // 重试2次，减少网络波动影响

        // 2. 调用工具类创建HTTP客户端
        httpClient = HttpClientUtil.createHttpClient(httpConfig);
        LOG.info("通过HttpClientUtil初始化HTTP客户端（超时25s，符合文档心跳要求）");
    }


    // ---------------------- 保留原远程调用逻辑，仅适配HttpClient5 API ----------------------
    /**
     * 调用metaServer POST接口（如create、mkdir，符合A1要求）
     */
    protected String callMetaPost(String endpoint, String requestBody) throws IOException {
        String url = metaServerBaseUrl + endpoint;
        HttpPost httpPost = new HttpPost(url);

        // 设置请求头：必带fileSystemName（符合文档命名空间要求）+ 副本同步标识
        httpPost.setHeader("Content-Type", "application/json; charset=utf-8");
        httpPost.setHeader("fileSystemName", defaultFileSystemName);
        httpPost.setHeader("X-Is-Replica-Sync", "false");

        // 设置请求体（工具类已处理编码，直接复用）
        if (requestBody != null && !requestBody.isEmpty()) {
            httpPost.setEntity(new StringEntity(requestBody, StandardCharsets.UTF_8));
        }

        LOG.info("调用metaServer POST接口：url={}", url);
        // 执行请求（适配HttpClient5的响应处理）
        return httpClient.execute(httpPost, response -> {
            int statusCode = response.getCode();
            if (statusCode < 200 || statusCode >= 300) {
                throw new IOException(String.format(
                        "metaServer调用失败：url=%s, 状态码=%d", url, statusCode));
            }
            // 复用HttpClient5的EntityUtils读取响应（避免流关闭问题）
            return EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
        });
    }


    /**
     * 调用metaServer GET接口（如getStatus、getClusterInfo，符合A2、A5要求）
     */
    protected String callMetaGet(String endpoint, String params) throws IOException {
        // 拼接URL（含文件系统名称参数，符合文档多命名空间要求）
        StringBuilder urlBuilder = new StringBuilder(metaServerBaseUrl + endpoint)
                .append("?fileSystemName=").append(defaultFileSystemName);
        if (params != null && !params.isEmpty()) {
            urlBuilder.append("&").append(params);
        }
        String url = urlBuilder.toString();

        HttpGet httpGet = new HttpGet(url);
        LOG.info("调用metaServer GET接口：url={}", url);

        // 执行请求（适配HttpClient5 API）
        return httpClient.execute(httpGet, response -> {
            if (response.getCode() < 200 || response.getCode() >= 300) {
                throw new IOException("metaServer GET调用失败，状态码：" + response.getCode());
            }
            return EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
        });
    }


    /**
     * 调用metaServer DELETE接口（如delete，符合A3要求）
     */
    protected String callMetaDelete(String endpoint, String params, boolean isReplicaSync) throws IOException {
        String url = new StringBuilder(metaServerBaseUrl + endpoint)
                .append("?fileSystemName=").append(defaultFileSystemName)
                .append(params != null ? "&" + params : "")
                .toString();

        HttpDelete httpDelete = new HttpDelete(url);
        httpDelete.setHeader("X-Is-Replica-Sync", String.valueOf(isReplicaSync));
        LOG.info("调用metaServer DELETE接口：url={}", url);

        return httpClient.execute(httpDelete, response -> {
            if (response.getCode() != 200) {
                throw new IOException("metaServer DELETE调用失败，状态码：" + response.getCode());
            }
            return EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
        });
    }


    /**
     * 调用dataServer POST接口（如write、read，符合A4、A6三副本要求）
     */
    protected String callDataPost(String dataServerUrl, String endpoint, String requestBody) throws IOException {
        String url = dataServerUrl + endpoint;
        HttpPost httpPost = new HttpPost(url);
        httpPost.setHeader("Content-Type", "application/json; charset=utf-8");
        httpPost.setHeader("fileSystemName", defaultFileSystemName);

        if (requestBody != null) {
            httpPost.setEntity(new StringEntity(requestBody, StandardCharsets.UTF_8));
        }

        LOG.info("调用dataServer POST接口：url={}", url);
        return httpClient.execute(httpPost, response -> {
            if (response.getCode() < 200 || response.getCode() >= 300) {
                throw new IOException("dataServer调用失败，状态码：" + response.getCode());
            }
            return EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
        });
    }


    // ---------------------- 抽象方法（与原逻辑一致，供EFileSystem实现） ----------------------
    public abstract FSInputStream open(String path);
    public abstract FSOutputStream create(String path);
    public abstract boolean mkdir(String path);
    public abstract boolean delete(String path);
    public abstract StatInfo getFileStats(String path);
    public abstract List<StatInfo> listFileStats(String path);
    public abstract ClusterInfo getClusterInfo();

    // 设置metaServer地址（符合2-17“SDK暴露metaServer地址”要求）
    public void setMetaServerBaseUrl(String metaServerBaseUrl) {
        this.metaServerBaseUrl = metaServerBaseUrl;
        LOG.info("更新metaServer地址：{}", metaServerBaseUrl);
    }
}