# MinFS MetaServer集群配置说明

## 概述

本配置实现了三个MetaServer的集群部署（一主二从），满足作业要求中的集群规模要求。

## 集群架构

```
MetaServer集群 (一主二从)
├── 主节点: localhost:8000 (master)
├── 从节点1: localhost:8001 (slave)
└── 从节点2: localhost:8002 (slave)

Zookeeper: localhost:2181
集群根路径: /minfs
```

## 配置文件说明

### 1. application-8000.yml (主节点)
- 端口: 8000
- 角色: master
- 启用Leader选举
- 心跳间隔: 10秒
- 超时时间: 30秒

### 2. application-8001.yml (从节点1)
- 端口: 8001
- 角色: slave
- 启用Leader选举
- 心跳间隔: 10秒
- 超时时间: 30秒

### 3. application-8002.yml (从节点2)
- 端口: 8002
- 角色: slave
- 启用Leader选举
- 心跳间隔: 10秒
- 超时时间: 30秒

## 关键配置项

### Zookeeper配置
```yaml
metaserver:
  zk:
    connect:
      string: localhost:2181
    root:
      path: /minfs
```

### 集群配置
```yaml
metaserver:
  cluster:
    role: master/slave
    election:
      enabled: true
    heartbeat:
      interval: 10000  # 10秒
      timeout: 30000   # 30秒
```

### 元数据存储配置
```yaml
metadata:
  storage:
    type: rocksdb
    path: ./data/metaserver-{port}/rocksdb
    backup:
      enabled: true
      path: ./data/metaserver-{port}/backup
      interval: 300000  # 5分钟
```

## 启动脚本

### 启动集群
```bash
# 启动所有MetaServer
./bin/start-metaservers.sh

# 或者单独启动
cd workpublish/metaServer
java -jar metaserver-1.0.jar --spring.profiles.active=8000
java -jar metaserver-1.0.jar --spring.profiles.active=8001
java -jar metaserver-1.0.jar --spring.profiles.active=8002
```

### 停止集群
```bash
# 停止所有MetaServer
./bin/stop-metaservers.sh
```

## 目录结构

```
workpublish/
├── metaServer/
│   ├── data/
│   │   ├── metaserver-8000/
│   │   │   ├── rocksdb/
│   │   │   ├── backup/
│   │   │   └── logs/
│   │   ├── metaserver-8001/
│   │   │   ├── rocksdb/
│   │   │   ├── backup/
│   │   │   └── logs/
│   │   └── metaserver-8002/
│   │       ├── rocksdb/
│   │       ├── backup/
│   │       └── logs/
│   ├── logs/
│   └── metaserver-1.0.jar
└── bin/
    ├── start-metaservers.sh
    └── stop-metaservers.sh
```

## 健康检查

### 检查单个服务
```bash
curl http://localhost:8000/health
curl http://localhost:8001/health
curl http://localhost:8002/health
```

### 检查集群状态
```bash
curl http://localhost:8000/zk/cluster
```

### 检查ZK注册状态
```bash
curl http://localhost:8000/zk/check
```

## 故障处理

### 1. 主节点故障
- 从节点自动进行Leader选举
- 新的主节点接管服务
- 元数据同步自动进行

### 2. 从节点故障
- 主节点继续提供服务
- 故障节点自动从集群中移除
- 新节点加入时自动同步元数据

### 3. Zookeeper故障
- 服务继续运行（使用本地缓存）
- 连接恢复后自动重新注册
- 支持自动重连机制

## 监控和日志

### 日志文件
- 每个MetaServer有独立的日志文件
- 日志路径: `./logs/metaserver-{port}.log`
- 支持日志轮转和备份

### 监控指标
- 服务健康状态
- 集群节点状态
- 元数据操作统计
- 副本分布情况

## 部署要求

### 系统要求
- Java 8+
- Zookeeper 3.4+
- 内存: 每个MetaServer至少512MB
- 磁盘: 每个MetaServer至少1GB可用空间

### 网络要求
- 端口8000-8002可用
- 端口2181 (Zookeeper) 可用
- 本地网络访问正常

## 测试验证

### 1. 基础功能测试
```bash
# 创建文件
curl -X POST "http://localhost:8000/create?path=/test.txt" \
  -H "fileSystemName: minfs"

# 查看文件状态
curl "http://localhost:8000/stats?path=/test.txt" \
  -H "fileSystemName: minfs"

# 删除文件
curl -X DELETE "http://localhost:8000/delete?path=/test.txt" \
  -H "fileSystemName: minfs"
```

### 2. 集群功能测试
```bash
# 检查集群状态
curl http://localhost:8000/zk/cluster

# 模拟主节点故障
kill -9 $(cat workpublish/metaServer/logs/metaserver-8000.pid)

# 检查从节点是否接管
curl http://localhost:8001/zk/cluster
```

## 注意事项

1. **端口冲突**: 确保8000-8002端口未被占用
2. **Zookeeper**: 必须先启动Zookeeper服务
3. **数据目录**: 确保有足够的磁盘空间
4. **权限**: 确保脚本有执行权限
5. **网络**: 确保本地网络访问正常

## 故障排查

### 常见问题
1. **端口被占用**: 检查端口使用情况 `netstat -tlnp | grep :8000`
2. **Zookeeper连接失败**: 检查Zookeeper服务状态
3. **权限不足**: 检查目录和文件权限
4. **内存不足**: 调整JVM参数

### 日志分析
- 查看启动日志: `tail -f logs/metaserver-8000.log`
- 查看错误日志: `grep ERROR logs/metaserver-8000.log`
- 查看集群日志: `grep "cluster\|election" logs/metaserver-8000.log`
