# minFS Python客户端项目结构

## 项目概述

这是一个完整的minFS分布式文件系统Python客户端实现，基于Java示例代码重新设计，提供了完整的文件操作接口和分布式存储功能。

## 目录结构

```
easyClient_python/
├── __init__.py                 # 模块初始化文件
├── core/                       # 核心模块
│   ├── __init__.py
│   ├── file_system.py          # 文件系统抽象基类
│   ├── e_file_system.py        # 主要的文件系统客户端类
│   ├── fs_input_stream.py      # 文件输入流
│   └── fs_output_stream.py     # 文件输出流
├── domain/                     # 数据模型模块
│   ├── __init__.py
│   ├── stat_info.py            # 文件状态信息
│   ├── cluster_info.py         # 集群信息
│   ├── file_type.py            # 文件类型枚举
│   ├── replica_data.py         # 副本数据
│   ├── meta_server_msg.py      # MetaServer信息
│   └── data_server_msg.py      # DataServer信息
├── util/                       # 工具模块
│   ├── __init__.py
│   ├── http_client_util.py     # HTTP客户端工具
│   └── zk_util.py             # ZooKeeper工具
├── tests/                      # 测试模块
│   ├── __init__.py
│   └── test_basic_operations.py # 基础操作测试
├── examples/                   # 示例代码
│   ├── basic_usage.py          # 基础使用示例
│   └── large_file_test.py      # 大文件测试示例
├── cli.py                      # 命令行接口
├── setup.py                    # 安装配置
├── requirements.txt             # 依赖列表
├── README.md                   # 项目文档
└── PROJECT_STRUCTURE.md        # 项目结构说明
```

## 核心模块说明

### 1. 核心模块 (core/)

#### FileSystem (file_system.py)
- **功能**: 文件系统抽象基类
- **职责**: 定义通用的文件系统方法和变量
- **特点**: 支持命名空间隔离，封装HTTP远程调用

#### EFileSystem (e_file_system.py)
- **功能**: 主要的文件系统客户端类
- **职责**: 实现所有文件操作接口
- **核心方法**:
  - `open()`: 打开文件用于读取
  - `create()`: 创建文件用于写入
  - `mkdir()`: 创建目录
  - `delete()`: 删除文件或目录（支持递归删除）
  - `get_file_stats()`: 获取文件状态信息
  - `list_file_stats()`: 列出目录内容
  - `get_cluster_info()`: 获取集群信息

#### FSInputStream (fs_input_stream.py)
- **功能**: 文件输入流
- **职责**: 从分布式文件系统读取文件数据
- **特点**: 
  - 支持大文件分块读取
  - 多副本故障转移
  - MD5校验功能
  - 流式读取，内存友好

#### FSOutputStream (fs_output_stream.py)
- **功能**: 文件输出流
- **职责**: 向分布式文件系统写入文件数据
- **特点**:
  - 支持大文件分块写入
  - 三副本同时写入
  - 实时MD5计算
  - 缓冲区管理

### 2. 数据模型模块 (domain/)

#### StatInfo (stat_info.py)
- **功能**: 文件状态信息
- **字段**: 路径、大小、修改时间、类型、副本数据
- **方法**: 提供getter/setter和类型判断方法

#### ClusterInfo (cluster_info.py)
- **功能**: 集群信息
- **字段**: 主从MetaServer、DataServer列表
- **方法**: 集群状态查询和统计

#### FileType (file_type.py)
- **功能**: 文件类型枚举
- **类型**: UNKNOWN、VOLUME、FILE、DIRECTORY
- **方法**: 根据代码获取类型

#### ReplicaData (replica_data.py)
- **功能**: 副本数据
- **字段**: 副本ID、数据节点、路径
- **方法**: 解析主机和端口信息

#### MetaServerMsg (meta_server_msg.py)
- **功能**: MetaServer信息
- **字段**: 主机、端口
- **方法**: 获取完整URL

#### DataServerMsg (data_server_msg.py)
- **功能**: DataServer信息
- **字段**: 主机、端口、文件总数、容量、已用容量
- **方法**: 容量统计和使用率计算

### 3. 工具模块 (util/)

#### HttpClientUtil (http_client_util.py)
- **功能**: HTTP客户端工具
- **特点**:
  - 连接池管理
  - 自动重试机制
  - 超时控制
  - 文件上传下载支持
- **方法**: GET、POST、PUT、DELETE、文件操作

#### ZkUtil (zk_util.py)
- **功能**: ZooKeeper工具
- **特点**:
  - 服务发现
  - 集群状态监听
  - 自动故障转移
  - 节点注册管理
- **方法**: 连接管理、节点监听、集群信息获取

## 测试和示例

### 测试模块 (tests/)
- **test_basic_operations.py**: 基础操作测试
- 覆盖所有核心功能的单元测试
- 使用Mock模拟外部依赖

### 示例模块 (examples/)
- **basic_usage.py**: 基础使用示例
- **large_file_test.py**: 大文件测试示例
- 演示完整的API使用流程

## 命令行工具

### CLI (cli.py)
提供命令行接口，支持以下命令：
- `ls`: 列出目录内容
- `stat`: 显示文件状态
- `cat`: 显示文件内容
- `mkdir`: 创建目录
- `rm`: 删除文件或目录
- `cluster`: 显示集群信息

## 技术特性

### 1. 分布式特性
- **三副本存储**: 确保数据安全性
- **服务发现**: 通过ZooKeeper自动发现服务
- **故障转移**: 自动切换到可用节点
- **负载均衡**: 多副本均匀分布

### 2. 大文件支持
- **分块传输**: 支持大文件的分块读写
- **MD5校验**: 确保数据完整性
- **进度显示**: 大文件操作进度反馈
- **内存优化**: 流式处理，避免内存溢出

### 3. 高可用性
- **自动重试**: 网络异常自动重试
- **连接池**: HTTP连接复用
- **超时控制**: 合理的超时设置
- **错误处理**: 完善的异常处理机制

### 4. 开发友好
- **类型注解**: 完整的类型提示
- **文档完善**: 详细的API文档
- **测试覆盖**: 全面的单元测试
- **示例丰富**: 多种使用示例

## 部署和使用

### 安装依赖
```bash
pip install -r requirements.txt
```

### 开发安装
```bash
pip install -e .
```

### 运行测试
```bash
pytest tests/
```

### 运行示例
```bash
python examples/basic_usage.py
python examples/large_file_test.py
```

### 使用命令行工具
```bash
python cli.py ls /
python cli.py stat /test.txt
python cli.py cluster
```

## 与Java版本的对比

### 相似之处
- **架构设计**: 保持相同的模块化设计
- **核心功能**: 实现相同的文件操作接口
- **数据模型**: 保持相同的数据结构
- **分布式特性**: 支持相同的分布式功能

### 改进之处
- **类型安全**: 完整的Python类型注解
- **异常处理**: 更完善的错误处理机制
- **文档完善**: 详细的文档和示例
- **测试覆盖**: 全面的单元测试
- **命令行工具**: 提供便捷的命令行接口
- **大文件优化**: 更好的大文件处理能力

## 总结

这个Python版本的minFS客户端完全基于Java示例代码重新设计，保持了原有的架构和功能，同时充分利用了Python的优势，提供了更友好的开发体验和更完善的功能特性。代码结构清晰，文档完善，测试覆盖全面，是一个高质量的分布式文件系统客户端实现。
