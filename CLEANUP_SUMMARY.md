# Metaserver 清理总结

## 清理目标
根据项目架构分析，metaserver主要负责元数据管理，而三副本写入和删除操作应该由dataserver负责。因此删除了metaserver中与数据操作相关的代码。

## 已删除的不必要部分

### 1. Controller层删除的接口
- `writeData` - 直接写入文件数据到DataServer（三副本同步）
- `readData` - 从DataServer读取文件数据  
- `deleteData` - 删除文件数据（从所有DataServer删除）

### 2. Service层删除的方法
- `writeFileData()` - 写入文件数据到DataServer（三副本同步）
- `readFileData()` - 从DataServer读取文件数据
- `deleteFileData()` - 删除文件数据（从所有DataServer删除）

### 3. 删除的服务类
- ~~`DataServerClientService` - 负责与DataServer通信进行数据操作的服务~~ (已重新设计)
- ~~`RegistService` - 基础注册服务~~ (已删除，统一使用ZkMetaServerService)

### 4. 简化的服务
- `FsckServices` - 简化了FSCK检查逻辑，删除了副本修复功能，只保留元数据完整性检查

## 保留的必要部分

### 1. 元数据管理
- 文件/目录的创建、删除、重命名
- 文件状态信息管理（大小、时间、类型等）
- 副本位置信息管理（三副本的分布信息）

### 2. 集群管理
- DataServer状态监控
- 副本分布统计
- 集群信息查询

### 3. 存储服务
- `MetadataStorageService` - RocksDB元数据持久化
- `ZkDataServerService` - DataServer注册和状态管理
- `ZkMetaServerService` - MetaServer集群管理 (增强版，统一注册服务)
- ~~`RegistService` - MetaServer向Zookeeper注册~~ (已删除)

## 重新设计的架构

### 1. 统一MetaServer注册服务
- **删除RegistService**：避免重复注册和节点混乱
- **增强ZkMetaServerService**：成为唯一的MetaServer注册服务
- **固定命名节点**：使用`meta{port}`格式，如`meta8000`、`meta8001`

### 2. 节点注册结构优化
```
/minfs (根路径)
├── /metaservers (MetaServer注册路径)
│   ├── /meta8000 (端口8000的MetaServer)
│   ├── /meta8001 (端口8001的MetaServer)
│   └── /meta8002 (端口8002的MetaServer)
└── /leader (Leader选举路径)
    └── /meta8000 (当前Leader节点)
```

### 3. 关键操作流程
```
创建文件/目录：
1. MetaServer创建元数据
2. 调用DataServerClientService通知相关DataServer
3. DataServer在磁盘上创建实际的文件/目录结构

删除文件/目录：
1. MetaServer调用DataServerClientService删除实际数据
2. DataServer从磁盘删除文件/目录
3. MetaServer删除元数据
```

## 职责分工

### Metaserver职责
- 元数据管理（文件路径、大小、时间、副本位置等）
- 文件系统操作（创建、删除、重命名等）
- 集群管理（DataServer状态监控、副本分布）
- 元数据持久化存储
- **协调DataServer操作**：确保文件系统操作落实到磁盘
- **ZK集群管理**：统一的注册、Leader选举、状态监控

### Dataserver职责  
- 实际文件数据的存储
- 三副本写入和同步
- 文件删除（包括三副本删除）
- 数据读取
- **响应MetaServer通知**：执行文件/目录的创建和删除操作

## 清理后的架构优势

1. **职责清晰** - metaserver专注于元数据管理和协调，dataserver专注于数据存储
2. **操作一致性** - 元数据操作和磁盘操作保持同步
3. **维护性提升** - 减少了服务间的复杂依赖
4. **性能优化** - 避免了metaserver作为数据中转的性能瓶颈
5. **可靠性提升** - 通过ZK获取DataServer地址，支持动态发现
6. **节点管理简化** - 统一使用固定命名节点，避免重复注册和混乱

## 注意事项

1. **创建操作**：metaserver先创建元数据，再通知dataserver创建磁盘结构
2. **删除操作**：metaserver先通知dataserver删除磁盘数据，再删除元数据
3. **地址获取**：所有DataServer地址都从ZK获取，支持动态注册和发现
4. **错误处理**：如果DataServer操作失败，metaserver会记录日志并返回失败状态
5. **并发支持**：支持并发调用多个DataServer，提高操作效率
6. **ZK节点管理**：只使用固定命名节点，避免临时顺序节点的混乱

## 技术实现细节

### 1. 从ZK获取DataServer地址
```java
@Autowired
private ZkDataServerService zkDataServerService;

// 获取活跃的DataServer列表
List<Map<String, Object>> activeServers = zkDataServerService.getActiveDataServers();
```

### 2. 调用DataServer接口
```java
// 创建文件/目录
dataServerClient.createOnMultipleDataServers(replicas, isDirectory);

// 删除文件/目录  
dataServerClient.deleteFromMultipleDataServers(replicas);
```

### 3. 并发处理
```java
// 使用CompletableFuture并发调用多个DataServer
List<CompletableFuture<Boolean>> futures = replicas.stream()
    .map(replica -> CompletableFuture.supplyAsync(() -> 
        operation(replica), executorService))
    .toList();
```

### 4. ZK状态监控接口
```java
// 检查ZK注册状态
GET /zk/check

// 获取ZK集群状态  
GET /zk/cluster
```

## 修改后的ZK节点结构

### 之前的结构（已删除）
```
/minfs/metaservers/
├── meta-0000000024 (临时顺序节点 - 已删除)
└── meta8000 (固定命名节点 - 保留)
```

### 现在的结构（统一）
```
/minfs/metaservers/
├── meta8000 (端口8000的MetaServer)
├── meta8001 (端口8001的MetaServer)
└── meta8002 (端口8002的MetaServer)
```

### 节点信息格式
```
localhost:8000:active:1703123456789
├── localhost (主机名)
├── 8000 (端口号)
├── active (状态)
└── 1703123456789 (注册时间戳)
```

这样的设计使得ZK节点管理更加清晰，避免了重复注册和节点混乱的问题。
