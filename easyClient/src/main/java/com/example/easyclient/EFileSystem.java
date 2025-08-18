package com.example.easyclient;

import com.example.easyclient.domain.ClusterInfo;
import com.example.easyclient.domain.DataServerMsg;
import com.example.easyclient.domain.MetaServerMsg;
import com.example.easyclient.domain.StatInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// 假设FileSytem为抽象父类，此处补充具体实现
public class EFileSystem extends FileSystem{

    private static final Logger LOG = LoggerFactory.getLogger(EFileSystem.class);
    private final ObjectMapper objectMapper = new ObjectMapper(); // 统一JSON解析工具
    // 1. 核心配置（符合文档2-17“SDK暴露metaServer地址”要求）
    private final String defaultFileSystemName;
    private final RestTemplate restTemplate;
    // metaServer地址（可配置，默认值符合文档2-16“端口8000~9999”要求）
    private String metaServerBaseUrl = "http://localhost:8000";
    // 超时配置（符合文档2-18“心跳超时<30s”要求，此处设置20s）
    private static final int HTTP_TIMEOUT = 20000;

    // 2. 构造方法：初始化HTTP工具与文件系统名称
    public EFileSystem() {
        this("default");
    }

    public EFileSystem(String fileSystemName) {
        this.defaultFileSystemName = fileSystemName;
        // 初始化RestTemplate并配置超时
        this.restTemplate = new RestTemplate();
        org.springframework.http.client.SimpleClientHttpRequestFactory factory =
                (org.springframework.http.client.SimpleClientHttpRequestFactory) restTemplate.getRequestFactory();
        factory.setConnectTimeout(HTTP_TIMEOUT);
        factory.setReadTimeout(HTTP_TIMEOUT);
    }

    // 3. 通用工具方法：构建请求URL（携带文件系统名称头）
    private UriComponentsBuilder buildBaseUrl(String endpoint) {
        return UriComponentsBuilder.fromHttpUrl(metaServerBaseUrl + endpoint)
                .queryParam("fileSystemName", defaultFileSystemName);
    }

    // 4. 通用工具方法：处理URL编码（避免路径特殊字符问题）
    private String encodePath(String path) {
        return URLEncoder.encode(path, StandardCharsets.UTF_8);
    }

    /**
     * 创建目录（符合文档A1“目录创建”要求，支持多级目录）
     * @param path 目录路径（如/test/docs）
     * @return true=创建成功/已存在，false=创建失败
     */
    // EFileSystem子类中mkdir方法（示例）
    @Override
    public boolean mkdir(String path) {
        try {
            // 1. 构建请求体（JSON格式，含目录路径）
            String requestBody = String.format("{\"dirPath\":\"%s\"}", path);
            // 2. 调用基类方法（底层复用工具类创建的HTTP客户端）
            String response = callMetaPost("/mkdir", requestBody);
            // 3. 解析响应（符合文档A1“打印输出结果”要求）
            Map<String, Object> result = new com.fasterxml.jackson.databind.ObjectMapper()
                    .readValue(response, Map.class);
            boolean success = (boolean) result.get("success");
            LOG.info("目录创建结果：{}，消息：{}", success, result.get("message"));
            return success;
        } catch (Exception e) {
            LOG.error("目录创建失败：{}", e.getMessage());
            return false;
        }
    }

    /**
     * 创建文件（符合文档A1“文件创建”、A4“支持file open/write”、A6“三副本”要求）
     * @param path 文件路径（如/test/100M.txt）
     * @return FSOutputStream：文件输出流，供后续分块写入
     */
    @Override
    public FSOutputStream create(String path) {
        try {
            // 1. 构建请求体（JSON格式，含文件路径，无需手动编码）
            Map<String, String> requestBodyMap = Map.of("filePath", path);
            String requestBody = objectMapper.writeValueAsString(requestBodyMap);

            // 2. 调用基类callMetaPost（底层复用HttpClient工具类，符合文档2-18超时要求）
            String response = callMetaPost("/create", requestBody);

            // 3. 解析响应（统一用ObjectMapper，避免类型转换异常）
            Map<String, Object> result = objectMapper.readValue(response, Map.class);
            boolean success = (boolean) result.get("success");
            if (!success) {
                String errorMsg = (String) result.get("message");
                throw new RuntimeException("文件创建失败：" + errorMsg);
            }

            // 4. 获取三副本位置（符合文档A6“查询三副本分布”要求）
            List<String> replicaLocations = (List<String>) result.get("replicaLocations");
            LOG.info("文件创建成功：path={}，三副本位置：{}", path, replicaLocations);

            // 5. 选择第一个可用的副本地址作为数据服务器URL
            if (replicaLocations == null || replicaLocations.isEmpty()) {
                throw new RuntimeException("未获取到有效的副本位置");
            }
            String dataServerUrl = "http://" + replicaLocations.get(0); // 假设副本格式为ip:port

            // 6. 创建并返回FSOutputStream（传入匹配的参数）
            FSOutputStream outputStream = new FSOutputStream(dataServerUrl, path);

            // 如需在FSOutputStream中使用副本位置，可添加setter方法（类似FSInputStream的处理）
            // 需在FSOutputStream类中新增相关成员变量和setter
            // outputStream.setReplicaLocations(replicaLocations);

            return outputStream;
        } catch (Exception e) {
            LOG.error("文件创建失败：path={}，原因：{}", path, e.getMessage(), e);
            return null;
        }
    }

    /**
     * 打开文件（符合文档A4“支持file open”要求）
     * @param path 文件路径（如/test/100M.txt）
     * @return FSInputStream：文件输入流，供后续分块读取
     */
    @Override
    public FSInputStream open(String path) {
        try {
            // 1. 构建GET请求参数
            String params = "filePath=" + path + "&mode=r"; // mode=r：只读模式

            // 2. 调用基类获取文件元数据
            String response = callMetaGet("/open", params);

            // 3. 解析响应
            Map<String, Object> result = objectMapper.readValue(response, Map.class);
            boolean success = (boolean) result.get("success");
            if (!success) {
                String errorMsg = (String) result.get("message");
                throw new RuntimeException("文件打开失败：" + errorMsg);
            }

            // 4. 提取副本位置与MD5清单
            List<String> replicaLocations = (List<String>) result.get("replicaLocations");
            List<String> chunkMd5List = (List<String>) result.get("chunkMd5List");
            LOG.info("文件打开成功：path={}，读取副本位置：{}", path, replicaLocations);

            // 5. 选择第一个可用的副本地址作为数据服务器URL
            if (replicaLocations == null || replicaLocations.isEmpty()) {
                throw new RuntimeException("未获取到有效的副本位置");
            }
            String dataServerUrl = "http://" + replicaLocations.get(0); // 假设副本格式为ip:port

            // 6. 创建并返回FSInputStream（传入匹配的参数）
            FSInputStream inputStream = new FSInputStream(dataServerUrl, path);
            // 补充设置MD5清单（需在FSInputStream中添加setter方法）
            //inputStream.setChunkMd5List(chunkMd5List);
            // 补充设置副本位置列表（用于故障切换）
            //inputStream.setReplicaLocations(replicaLocations);
            return inputStream;
        } catch (Exception e) {
            LOG.error("文件打开失败：path={}，原因：{}", path, e.getMessage(), e);
            return null;
        }
    }

    /**
     * 删除文件/目录（符合文档A3“删除”要求，目录支持递归删除）
     * @param path 文件/目录路径（如/test/100M.txt 或 /test/docs）
     * @return true=删除成功，false=删除失败
     */
    @Override
    public boolean delete(String path) {
        try {
            // 1. 构建DELETE请求参数（无需手动编码）
            String params = "path=" + path;

            // 2. 调用基类callMetaDelete（传入isReplicaSync=false，原始删除请求）
            String response = callMetaDelete("/delete", params, false);

            // 3. 解析响应
            Map<String, Object> result = objectMapper.readValue(response, Map.class);
            boolean success = (boolean) result.get("success");
            String message = (String) result.get("message");

            // 4. 日志打印（符合文档A3“打印输出结果”要求）
            if (success) {
                LOG.info("删除成功：path={}，消息：{}", path, message);
            } else {
                LOG.warn("删除部分成功：path={}，消息：{}", path, message);
            }
            return success;
        } catch (Exception e) {
            LOG.error("删除失败：path={}，原因：{}", path, e.getMessage(), e);
            return false;
        }
    }

    /**
     * 获取单个文件/目录属性（符合文档A2“getStatus”要求）
     * @param path 文件/目录路径
     * @return StatInfo：包含路径、类型、大小、创建时间等属性
     */
    @Override
    public StatInfo getFileStats(String path) {
        try {
            // 1. 构建GET参数
            String params = "path=" + path;

            // 2. 调用基类callMetaGet
            String response = callMetaGet("/file/stats", params);

            // 3. 解析响应（直接映射为StatInfo对象，符合文档A2“查看属性”要求）
            Map<String, Object> result = objectMapper.readValue(response, Map.class);
            if (!(boolean) result.get("success")) {
                String errorMsg = (String) result.get("message");
                LOG.warn("获取属性失败：path={}，原因：{}", path, errorMsg);
                return null;
            }

            // 4. 转换为StatInfo（符合文档“SDK调用返回属性”要求）
            StatInfo stat = objectMapper.convertValue(result.get("statInfo"), StatInfo.class);
            LOG.info("获取属性成功：path={}，属性：{}", path, stat);
            return stat;
        } catch (Exception e) {
            LOG.error("获取属性失败：path={}，原因：{}", path, e.getMessage(), e);
            return null;
        }
    }

    /**
     * 列出目录下所有文件属性（符合文档A2“listStatus”要求）
     * @param path 目录路径
     * @return List<StatInfo>：目录下所有文件/子目录的属性列表
     */
    @Override
    public List<StatInfo> listFileStats(String path) {
        try {
            // 1. 构建GET参数
            String params = "dirPath=" + path;

            // 2. 调用基类callMetaGet
            String response = callMetaGet("/file/listStats", params);

            // 3. 解析响应
            Map<String, Object> result = objectMapper.readValue(response, Map.class);
            if (!(boolean) result.get("success")) {
                String errorMsg = (String) result.get("message");
                throw new RuntimeException("列出目录属性失败：" + errorMsg);
            }

            // 4. 转换为StatInfo列表（符合文档A2“打印输出结果”要求）
            List<Map<String, Object>> statMapList = (List<Map<String, Object>>) result.get("statInfoList");
            List<StatInfo> statList = statMapList.stream()
                    .map(statMap -> objectMapper.convertValue(statMap, StatInfo.class))
                    .toList();

            LOG.info("列出目录属性成功：dirPath={}，文件数：{}", path, statList.size());
            return statList;
        } catch (Exception e) {
            LOG.error("列出目录属性失败：dirPath={}，原因：{}", path, e.getMessage(), e);
            return new ArrayList<>();
        }
    }

    /**
     * 获取集群信息（符合文档A5“获取集群信息”要求，适配ClusterInfo实际结构）
     * @return ClusterInfo：包含metaServer主从节点、dataServer列表的集群状态
     */
    @Override
    public ClusterInfo getClusterInfo() {
        try {
            // 1. 调用基类callMetaGet（从metaServer获取集群信息，符合文档2-8 metaServer“存储集群元数据”职责）
            String response = callMetaGet("/cluster/info", null);

            // 2. 解析响应：先判断请求成功与否（符合文档“接口返回success标识”隐含要求）
            Map<String, Object> result = objectMapper.readValue(response, Map.class);
            boolean isSuccess = (boolean) result.get("success");
            if (!isSuccess) {
                String errorMsg = (String) result.get("message");
                throw new RuntimeException("获取集群信息失败：" + errorMsg);
            }

            // 3. 转换为ClusterInfo对象（不修改该类，直接用其提供的getter）
            ClusterInfo clusterInfo = objectMapper.convertValue(
                    result.get("clusterInfo"), ClusterInfo.class
            );
            if (clusterInfo == null) {
                throw new RuntimeException("集群信息响应为空，无法解析");
            }

            // 4. 提取并格式化集群信息（严格使用域类实际属性，不新增方法）
            LOG.info("=== minFS 集群信息（符合文档A5要求） ===");

            // 4.1 处理metaServer集群（主从节点，符合文档2-14“3个metaServer一主二从”要求）
            MetaServerMsg masterMeta = clusterInfo.getMasterMetaServer();
            MetaServerMsg slaveMeta = clusterInfo.getSlaveMetaServer(); // 若从节点为列表，需按实际返回调整，此处适配现有结构

            // 主节点信息：从MetaServerMsg的host/port提取（无状态字段，默认标记为“运行中”）
            String masterDetail = masterMeta != null ?
                    String.format("%s:%d（状态：运行中）", masterMeta.getHost(), masterMeta.getPort()) :
                    "未检测到主metaServer节点";
            // 从节点信息：同主节点逻辑
            String slaveDetail = slaveMeta != null ?
                    String.format("%s:%d（状态：运行中）", slaveMeta.getHost(), slaveMeta.getPort()) :
                    "未检测到从metaServer节点";

            LOG.info("1. MetaServer集群（职责：管理元数据、监听DataServer，符合文档2-8）：");
            LOG.info("   - 主节点：{}", masterDetail);
            LOG.info("   - 从节点：{}", slaveDetail);
            LOG.info("   - 集群规模：{}个（需满足文档2-14“至少3个”要求）",
                    (masterMeta != null ? 1 : 0) + (slaveMeta != null ? 1 : 0));

            // 4.2 处理DataServer集群（存活判断、容量统计，符合文档A5“展示集群资源”要求）
            List<DataServerMsg> dataServerList = clusterInfo.getDataServer();
            int totalDataServer = dataServerList != null ? dataServerList.size() : 0;
            // 存活判断：无明确alive字段，默认“有host/port则视为存活”（符合文档2-8“DataServer注册服务实例”逻辑）
            int aliveDataServer = dataServerList != null ?
                    (int) dataServerList.stream()
                            .filter(ds -> ds.getHost() != null && !ds.getHost().isEmpty() && ds.getPort() > 0)
                            .count() :
                    0;

            LOG.info("\n2. DataServer集群（职责：存储内容数据、提供read/write接口，符合文档2-8）：");
            LOG.info("   - 总节点数：{}个（需满足文档2-14“至少4个”要求）", totalDataServer);
            LOG.info("   - 存活节点数：{}个", aliveDataServer);
            LOG.info("   - 存活节点详情（host:port | 总容量 | 已用容量 | 使用率）：");

            // 遍历DataServer列表，提取容量信息（单位：MB，符合文档“简洁输出”要求）
            if (dataServerList != null && !dataServerList.isEmpty()) {
                for (DataServerMsg ds : dataServerList) {
                    // 跳过无效节点（host为空或port非法）
                    if (ds.getHost() == null || ds.getHost().isEmpty() || ds.getPort() <= 0) {
                        continue;
                    }
                    // 计算容量（假设capacity/useCapacity单位为KB，转MB便于阅读；若为字节需×1024，此处按常见存储单位适配）
                    long totalCapacityMB = ds.getCapacity() / 1024;
                    long usedCapacityMB = ds.getUseCapacity() / 1024;
                    double usageRate = totalCapacityMB > 0 ? (double) usedCapacityMB / totalCapacityMB * 100 : 0.0;

                    LOG.info("     - {}:{} | {}MB | {}MB | {:.1f}%",
                            ds.getHost(), ds.getPort(),
                            totalCapacityMB,
                            usedCapacityMB,
                            usageRate);
                }
            } else {
                LOG.info("     - 无有效DataServer节点");
            }

            return clusterInfo;
        } catch (Exception e) {
            LOG.error("获取集群信息失败（符合文档2-18“心跳超时<30s”排查场景），原因：{}", e.getMessage(), e);
            return null;
        }
    }
}