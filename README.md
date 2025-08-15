# minFS分布式文件系统 - Python客户端SDK

这是一个用于minFS分布式文件系统的Python客户端SDK，提供了完整的文件操作接口。

## 功能特性

- **基础文件操作**: 创建、删除、读写文件和目录
- **分布式支持**: 支持多副本数据存储和读取
- **服务发现**: 通过ZooKeeper进行服务发现和配置管理
- **大文件支持**: 支持大文件的分块传输和MD5校验
- **高可用性**: 支持故障转移和自动重试
- **类型安全**: 完整的类型注解支持

## 安装

```bash
pip install easyclient
```

或者从源码安装：

```bash
git clone https://github.com/minfs/easyclient-python.git
cd easyclient-python
pip install -e .
```

## 快速开始

### 基本使用

```python
from easyClient_python import EFileSystem

# 创建文件系统客户端
fs = EFileSystem("my_namespace")

# 创建目录
fs.mkdir("/test_dir")

# 创建并写入文件
with fs.create("/test_dir/test.txt") as f:
    f.write_string("Hello, minFS!")

# 读取文件
with fs.open("/test_dir/test.txt") as f:
    content = f.read()
    print(content.decode('utf-8'))  # 输出: Hello, minFS!

# 获取文件信息
stats = fs.get_file_stats("/test_dir/test.txt")
print(f"文件大小: {stats.get_size()} bytes")

# 列出目录内容
items = fs.list_file_stats("/test_dir")
for item in items:
    print(f"文件: {item.get_path()}, 大小: {item.get_size()} bytes")

# 删除文件
fs.delete("/test_dir/test.txt")

# 获取集群信息
cluster_info = fs.get_cluster_info()
print(f"集群中有 {len(cluster_info.get_data_servers())} 个数据服务器")
```

### 大文件操作

```python
# 写入大文件
with fs.create("/large_file.dat") as f:
    f.write_large_file("/path/to/local/large_file.dat")

# 读取大文件并计算MD5
with fs.open("/large_file.dat") as f:
    md5_hash = f.calculate_md5()
    print(f"文件MD5: {md5_hash}")
```

### 错误处理

```python
try:
    with fs.open("/nonexistent/file.txt") as f:
        content = f.read()
except FileNotFoundError:
    print("文件不存在")
except Exception as e:
    print(f"发生错误: {e}")
```

## API文档

### EFileSystem

主要的文件系统客户端类。

#### 方法

- `open(path: str) -> FSInputStream`: 打开文件用于读取
- `create(path: str) -> FSOutputStream`: 创建文件用于写入
- `mkdir(path: str) -> bool`: 创建目录
- `delete(path: str) -> bool`: 删除文件或目录（支持递归删除）
- `get_file_stats(path: str) -> Optional[StatInfo]`: 获取文件状态信息
- `list_file_stats(path: str) -> List[StatInfo]`: 列出目录下的文件状态信息
- `get_cluster_info() -> ClusterInfo`: 获取集群信息
- `exists(path: str) -> bool`: 检查文件或目录是否存在
- `is_file(path: str) -> bool`: 检查是否为文件
- `is_directory(path: str) -> bool`: 检查是否为目录

### FSInputStream

文件输入流，用于读取文件数据。

#### 方法

- `read(size: int = -1) -> bytes`: 读取数据
- `seek(offset: int, whence: int = 0)`: 设置读取位置
- `tell() -> int`: 获取当前读取位置
- `close()`: 关闭流
- `calculate_md5() -> str`: 计算文件的MD5值

### FSOutputStream

文件输出流，用于写入文件数据。

#### 方法

- `write(data: bytes)`: 写入数据
- `write_string(text: str, encoding: str = 'utf-8')`: 写入字符串
- `flush()`: 刷新缓冲区
- `seek(offset: int, whence: int = 0)`: 设置写入位置
- `tell() -> int`: 获取当前写入位置
- `close()`: 关闭流
- `get_md5() -> str`: 获取当前数据的MD5值
- `write_file(file_path: str, chunk_size: int = 8192)`: 写入本地文件
- `write_large_file(file_path: str, chunk_size: int = 1024 * 1024)`: 写入大文件

## 配置

### ZooKeeper配置

默认连接到 `localhost:2181`，可以通过以下方式自定义：

```python
from easyClient_python.util import ZkUtil

zk_util = ZkUtil(hosts="zk1:2181,zk2:2181,zk3:2181", timeout=30)
```

### HTTP客户端配置

```python
from easyClient_python.util import HttpClientUtil

http_client = HttpClientUtil(
    max_retries=3,
    timeout=30,
    max_connections=100,
    socket_timeout=30
)
```

## 测试

运行测试：

```bash
pytest tests/
```

运行覆盖率测试：

```bash
pytest --cov=easyClient_python tests/
```

## 开发

### 安装开发依赖

```bash
pip install -e ".[dev]"
```

### 代码格式化

```bash
black easyClient_python/
isort easyClient_python/
```

### 类型检查

```bash
mypy easyClient_python/
```

## 许可证

MIT License

## 贡献

欢迎提交Issue和Pull Request！

## 联系方式

- 项目主页: https://github.com/minfs/easyclient-python
- 问题反馈: https://github.com/minfs/easyclient-python/issues
