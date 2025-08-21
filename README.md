# MinFS 分布式文件系统

## 项目概述

MinFS是一个分布式文件系统，采用元数据服务器(MetaServer)和数据服务器(DataServer)分离的架构设计。

## 架构设计

### 服务职责分离

#### MetaServer (元数据服务器)
- **元数据管理**: 文件路径、大小、时间、类型等元数据信息
- **文件系统操作**: 创建、删除、重命名、列表等操作（包括递归删除）
- **集群管理**: DataServer状态监控、Leader选举
- **副本信息管理**: 记录DataServer返回的副本位置信息
- **文件写入协调**: 选择DataServer并调用其write接口

#### DataServer (数据服务器)  
- **数据存储**: 实际文件数据的存储和管理
- **三副本管理**: 负责三副本的创建、同步、删除等操作
- **数据一致性**: 确保副本间的数据一致性
- **故障恢复**: 副本损坏时的自动修复

### 操作流程

#### 创建文件/目录
```
Client → MetaServer → 创建元数据 → 返回成功
```

#### 写入文件数据
```
Client → MetaServer → 选择DataServer → 调用DataServer.write接口 → DataServer自动创建三副本 → 返回副本位置 → MetaServer记录副本信息 → 返回成功
```

#### 删除文件/目录
```
Client → MetaServer → 删除元数据 → 返回成功
(DataServer自动管理副本删除)
```

## 最新变更记录

### 2025-08-18: 删除MetaServer中的三副本写代码

#### 删除的内容
- **Controller层**: 删除`/write`接口（文件写入提交）
- **Service层**: 删除`createReplicas()`方法（三副本创建逻辑）
- **Service层**: 删除`updateFileSize()`方法（文件大小更新）
- **Service层**: 删除`getReplicaDistribution()`方法（副本分布统计）
- **Service层**: 删除副本相关的DataServer调用逻辑

#### 修改的内容
- **createFile()**: 简化逻辑，只创建元数据，不处理副本
- **deleteFile()**: 简化逻辑，只删除元数据，不调用DataServer删除
- **deleteDirectoryRecursively()**: 移除副本删除逻辑
- **getDataServerStatus()**: 改为从ZK服务获取状态信息

#### 架构调整
```
原架构: MetaServer → 创建元数据 → 通知DataServer → DataServer创建副本
新架构: MetaServer → 创建元数据 → 通知DataServer → DataServer创建副本

原架构: MetaServer → 删除元数据 → 通知DataServer → DataServer删除副本  
新架构: MetaServer → 删除元数据 → 通知DataServer → DataServer删除副本
```

#### 优势
- **职责清晰**: MetaServer负责元数据管理和写入协调，DataServer负责数据存储和三副本管理
- **协作机制**: MetaServer选择DataServer并调用其接口，DataServer自动管理副本
- **递归删除**: MetaServer实现目录递归删除，确保文件系统一致性
- **符合设计**: 三副本写由DataServer负责，MetaServer负责协调和记录

## 技术栈

- **Java 8+**
- **Spring Boot**
- **RocksDB** (元数据存储)
- **Zookeeper** (服务注册、Leader选举)
- **RESTful API** (服务间通信)

## 部署要求

- 最小集群规模: 3个MetaServer + 4个DataServer
- 端口范围: 8000-9999
- 心跳超时: <30秒
- FSCK周期: ≤120秒

## 启动方式

```bash
# 启动MetaServer集群
./bin/start-metaservers.sh

# 启动DataServer集群  
./bin/start-dataservers.sh

# 一键启动整个系统
./bin/start.sh
```
