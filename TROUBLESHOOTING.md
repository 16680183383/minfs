# MinFS MetaServer 故障排查指南

## 🚨 常见问题及解决方案

### 1. RocksDB初始化失败

#### 问题描述
```
ERROR c.k.c.m.s.MetadataStorageService - RocksDB初始化失败
org.rocksdb.RocksDBException: Failed to create lock file: ./rocksdb_metadata/LOCK: 另一个程序正在使用此文件，进程无法访问
```

#### 原因分析
- 多个MetaServer实例使用了相同的RocksDB路径
- 之前的进程没有正常关闭，锁文件未释放
- 配置文件中的路径配置错误

#### 解决方案

**方案1: 使用清理脚本（推荐）**
```bash
# 清理所有MetaServer相关进程和文件
./bin/cleanup-metaservers.sh

# 重新启动集群
./bin/start-metaservers.sh
```

**方案2: 手动清理**
```bash
# 1. 停止所有Java进程
pkill -f "metaserver"

# 2. 清理端口占用
lsof -ti:8000 | xargs kill -9
lsof -ti:8001 | xargs kill -9
lsof -ti:8002 | xargs kill -9

# 3. 删除锁文件
find . -name "LOCK" -delete
find . -name "rocksdb_metadata" -type d -exec rm -rf {} +

# 4. 重新启动
./bin/start-metaservers.sh
```

**方案3: 检查配置文件**
确保每个MetaServer使用不同的RocksDB路径：
```yaml
# application-8000.yml
metadata:
  storage:
    path: ./data/metaserver-8000/rocksdb

# application-8001.yml
metadata:
  storage:
    path: ./data/metaserver-8001/rocksdb

# application-8002.yml
metadata:
  storage:
    path: ./data/metaserver-8002/rocksdb
```

### 2. Zookeeper Leader选举问题

#### 问题描述
```
[zk: localhost:2181(CONNECTED) 6] ls /minfs/leader
[meta8000, meta8001, meta8002]
[zk: localhost:2181(CONNECTED) 7] ls /minfs/metaservers
[meta8000, meta8001, meta8002]
```

**问题分析**：
- Leader选举路径下出现了多个节点，这是错误的
- 正确的结构应该是：`/minfs/leader/leader`（只有一个节点）
- 每个MetaServer都创建了自己的leader节点，导致选举混乱

#### 解决方案

**方案1: 快速修复（推荐）**
```bash
# 使用快速修复脚本
./bin/quick-fix-zk.sh

# 重新启动集群
./bin/start-metaservers.sh
```

**方案2: 手动修复**
```bash
# 1. 停止所有MetaServer
pkill -f "metaserver"

# 2. 清理ZK中的错误节点
echo "ls /minfs/leader" | zkCli.sh -server localhost:2181 | grep -E "meta[0-9]+" | while read node; do
    echo "delete /minfs/leader/$node" | zkCli.sh -server localhost:2181
done

# 3. 删除根路径
echo "rmr /minfs/leader" | zkCli.sh -server localhost:2181
echo "rmr /minfs/metaservers" | zkCli.sh -server localhost:2181

# 4. 重新启动
./bin/start-metaservers.sh
```

**方案3: 完整修复脚本**
```bash
# 使用完整的修复脚本
./bin/fix-zk-leader.sh
```

#### 正确的ZK结构
```
/minfs/
├── /metaservers/          # MetaServer注册节点
│   ├── meta8000          # 端口8000的MetaServer（临时节点）
│   ├── meta8001          # 端口8001的MetaServer（临时节点）
│   └── meta8002          # 端口8002的MetaServer（临时节点）
└── /leader/               # Leader选举节点
    └── leader             # 只有一个Leader节点（临时节点）
```

### 3. 端口被占用

#### 问题描述
```
BindException: Address already in use
```

#### 解决方案
```bash
# 检查端口占用
lsof -i :8000
lsof -i :8001
lsof -i :8002

# 清理端口占用
./bin/cleanup-metaservers.sh
```

### 4. Zookeeper连接失败

#### 问题描述
```
Connection refused
Cannot connect to Zookeeper
```

#### 解决方案
```bash
# 1. 检查Zookeeper状态
zkServer.sh status

# 2. 启动Zookeeper
zkServer.sh start

# 3. 验证连接
echo "ls /" | zkCli.sh -server localhost:2181
```

### 5. 进程启动失败

#### 问题描述
```
MetaServer启动失败
进程异常退出
```

#### 解决方案
```bash
# 1. 检查日志
tail -f workpublish/metaServer/logs/metaserver-8000.log
tail -f workpublish/metaServer/logs/metaserver-8001.log
tail -f workpublish/metaServer/logs/metaserver-8002.log

# 2. 检查Java环境
java -version

# 3. 检查内存
free -h

# 4. 重新启动
./bin/stop-metaservers.sh
./bin/start-metaservers.sh
```

## 🔧 诊断工具

### 1. 系统状态检查
```bash
# 检查端口状态
./bin/check-status.sh

# 检查进程状态
ps aux | grep metaserver

# 检查网络连接
netstat -tlnp | grep -E ":(8000|8001|8002)"
```

### 2. 日志分析
```bash
# 查看错误日志
grep ERROR workpublish/metaServer/logs/*.log

# 查看启动日志
grep "启动\|启动完成" workpublish/metaServer/logs/*.log

# 查看选举日志
grep "leader\|election" workpublish/metaServer/logs/*.log
```

### 3. Zookeeper状态检查
```bash
# 检查ZK节点
echo "ls /minfs" | zkCli.sh -server localhost:2181
echo "ls /minfs/metaservers" | zkCli.sh -server localhost:2181
echo "ls /minfs/leader" | zkCli.sh -server localhost:2181

# 检查节点数据
echo "get /minfs/metaservers/meta8000" | zkCli.sh -server localhost:2181
```

## 📋 启动检查清单

### 启动前检查
- [ ] Zookeeper服务运行正常
- [ ] 端口8000-8002未被占用
- [ ] Java环境正确安装
- [ ] 配置文件路径正确
- [ ] 数据目录有足够权限

### 启动后验证
- [ ] 所有MetaServer进程正常运行
- [ ] 端口监听正常
- [ ] ZK节点注册成功
- [ ] Leader选举完成
- [ ] 健康检查接口响应正常

## 🚀 快速恢复流程

### 1. 紧急恢复
```bash
# 停止所有服务
./bin/stop-metaservers.sh

# 清理环境
./bin/cleanup-metaservers.sh

# 重新启动
./bin/start-metaservers.sh
```

### 2. 逐步恢复
```bash
# 1. 检查环境
./bin/check-environment.sh

# 2. 清理问题
./bin/cleanup-metaservers.sh

# 3. 启动服务
./bin/start-metaservers.sh

# 4. 验证状态
./bin/check-status.sh
```

## 📞 获取帮助

如果以上方案无法解决问题，请：

1. **收集日志信息**
   ```bash
   tar -czf metaserver-logs-$(date +%Y%m%d-%H%M%S).tar.gz workpublish/metaServer/logs/
   ```

2. **收集系统信息**
   ```bash
   # 系统信息
   uname -a
   java -version
   
   # 进程信息
   ps aux | grep metaserver
   
   # 端口信息
   netstat -tlnp | grep -E ":(8000|8001|8002)"
   ```

3. **提供错误详情**
   - 完整的错误日志
   - 配置文件内容
   - 系统环境信息
   - 问题复现步骤

## 🔍 预防措施

### 1. 定期维护
- 定期清理日志文件
- 监控磁盘空间使用
- 检查进程状态

### 2. 配置优化
- 使用绝对路径避免相对路径问题
- 设置合理的超时时间
- 配置日志轮转

### 3. 监控告警
- 设置进程监控
- 配置端口监控
- 建立健康检查机制
