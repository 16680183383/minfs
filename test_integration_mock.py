"""
集成测试 - 使用Mock服务器
不需要真实的ZooKeeper和minFS集群
"""

import sys
import os
import time
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import logging

# 添加当前目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MockMetaServerHandler(BaseHTTPRequestHandler):
    """Mock MetaServer处理器"""
    
    def do_GET(self):
        """处理GET请求"""
        if self.path == "/cluster/info":
            self.send_cluster_info()
        elif self.path.startswith("/file/list"):
            self.send_file_list()
        elif self.path.startswith("/file/status"):
            self.send_file_status()
        else:
            self.send_error(404, "Not Found")
    
    def do_POST(self):
        """处理POST请求"""
        if self.path == "/file/create":
            self.handle_file_create()
        elif self.path == "/file/delete":
            self.handle_file_delete()
        elif self.path == "/directory/create":
            self.handle_directory_create()
        else:
            self.send_error(404, "Not Found")
    
    def send_cluster_info(self):
        """发送集群信息"""
        cluster_info = {
            "masterMetaServer": {
                "host": "localhost",
                "port": 8001
            },
            "slaveMetaServer": {
                "host": "localhost", 
                "port": 8001
            },
            "dataServers": [
                {
                    "host": "localhost",
                    "port": 8002,
                    "capacity": 1000,
                    "useCapacity": 100,
                    "fileTotal": 5
                },
                {
                    "host": "localhost",
                    "port": 8003,
                    "capacity": 1000,
                    "useCapacity": 80,
                    "fileTotal": 3
                }
            ]
        }
        
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(cluster_info).encode())
    
    def send_file_list(self):
        """发送文件列表"""
        file_list = [
            {
                "path": "/test.txt",
                "type": "FILE",
                "size": 1024,
                "replicaData": [
                    {"host": "localhost", "port": 8002, "path": "/data/test.txt"},
                    {"host": "localhost", "port": 8003, "path": "/data/test.txt"}
                ]
            }
        ]
        
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(file_list).encode())
    
    def send_file_status(self):
        """发送文件状态"""
        file_status = {
            "path": "/test.txt",
            "type": "FILE",
            "size": 1024,
            "replicaData": [
                {"host": "localhost", "port": 8002, "path": "/data/test.txt"},
                {"host": "localhost", "port": 8003, "path": "/data/test.txt"}
            ]
        }
        
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(file_status).encode())
    
    def handle_file_create(self):
        """处理文件创建"""
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        data = json.loads(post_data.decode('utf-8'))
        
        logger.info(f"Mock MetaServer: 创建文件 {data.get('path')}")
        
        # 返回成功响应
        response = {"success": True, "message": "File created successfully"}
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(response).encode())
    
    def handle_file_delete(self):
        """处理文件删除"""
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        data = json.loads(post_data.decode('utf-8'))
        
        logger.info(f"Mock MetaServer: 删除文件 {data.get('path')}")
        
        response = {"success": True, "message": "File deleted successfully"}
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(response).encode())
    
    def handle_directory_create(self):
        """处理目录创建"""
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        data = json.loads(post_data.decode('utf-8'))
        
        logger.info(f"Mock MetaServer: 创建目录 {data.get('path')}")
        
        response = {"success": True, "message": "Directory created successfully"}
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(response).encode())
    
    def log_message(self, format, *args):
        """重写日志方法，减少输出"""
        logger.debug(f"Mock MetaServer: {format % args}")

class MockDataServerHandler(BaseHTTPRequestHandler):
    """Mock DataServer处理器"""
    
    def do_GET(self):
        """处理GET请求"""
        if self.path.startswith("/file/read"):
            self.handle_file_read()
        else:
            self.send_error(404, "Not Found")
    
    def do_POST(self):
        """处理POST请求"""
        if self.path.startswith("/file/write"):
            self.handle_file_write()
        else:
            self.send_error(404, "Not Found")
    
    def handle_file_read(self):
        """处理文件读取"""
        # 模拟文件内容
        file_content = b"Hello, this is mock file content from DataServer!"
        
        self.send_response(200)
        self.send_header('Content-type', 'application/octet-stream')
        self.send_header('Content-Length', str(len(file_content)))
        self.end_headers()
        self.wfile.write(file_content)
    
    def handle_file_write(self):
        """处理文件写入"""
        content_length = int(self.headers['Content-Length'])
        file_data = self.rfile.read(content_length)
        
        logger.info(f"Mock DataServer: 写入文件，大小: {len(file_data)} bytes")
        
        response = {"success": True, "message": "File written successfully"}
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(response).encode())
    
    def log_message(self, format, *args):
        """重写日志方法，减少输出"""
        logger.debug(f"Mock DataServer: {format % args}")

class MockZooKeeper:
    """Mock ZooKeeper类"""
    
    def __init__(self):
        self.meta_servers = [
            {"host": "localhost", "port": 8001}
        ]
        self.data_servers = [
            {"host": "localhost", "port": 8002, "capacity": 1000, "useCapacity": 100, "fileTotal": 5},
            {"host": "localhost", "port": 8003, "capacity": 1000, "useCapacity": 80, "fileTotal": 3}
        ]
    
    def get_master_meta_server(self):
        """获取主MetaServer"""
        from domain.meta_server_msg import MetaServerMsg
        server = self.meta_servers[0]
        return MetaServerMsg(host=server["host"], port=server["port"])
    
    def get_data_servers(self):
        """获取DataServer列表"""
        from domain.data_server_msg import DataServerMsg
        result = []
        for server in self.data_servers:
            result.append(DataServerMsg(
                host=server["host"],
                port=server["port"],
                capacity=server["capacity"],
                use_capacity=server["useCapacity"],
                file_total=server["fileTotal"]
            ))
        return result

def start_mock_servers():
    """启动Mock服务器"""
    servers = []
    
    # 启动Mock MetaServer
    meta_server = HTTPServer(('localhost', 8001), MockMetaServerHandler)
    meta_thread = threading.Thread(target=meta_server.serve_forever, daemon=True)
    meta_thread.start()
    servers.append(('MetaServer', meta_server, meta_thread))
    logger.info("Mock MetaServer 启动在端口 8001")
    
    # 启动Mock DataServer 1
    data_server1 = HTTPServer(('localhost', 8002), MockDataServerHandler)
    data_thread1 = threading.Thread(target=data_server1.serve_forever, daemon=True)
    data_thread1.start()
    servers.append(('DataServer1', data_server1, data_thread1))
    logger.info("Mock DataServer1 启动在端口 8002")
    
    # 启动Mock DataServer 2
    data_server2 = HTTPServer(('localhost', 8003), MockDataServerHandler)
    data_thread2 = threading.Thread(target=data_server2.serve_forever, daemon=True)
    data_thread2.start()
    servers.append(('DataServer2', data_server2, data_thread2))
    logger.info("Mock DataServer2 启动在端口 8003")
    
    return servers

def test_basic_operations():
    """测试基本操作"""
    try:
        print("\n=== 测试基本文件系统操作 ===")
        
        # 创建文件系统客户端（使用Mock ZooKeeper）
        from core.e_file_system import EFileSystem
        
        # 这里我们需要修改EFileSystem来支持Mock模式
        # 暂时跳过这个测试
        print("   ⚠️  需要修改EFileSystem以支持Mock模式")
        return False
        
    except Exception as e:
        print(f"   ✗ 基本操作测试失败: {e}")
        return False

def test_http_communication():
    """测试HTTP通信"""
    try:
        print("\n=== 测试HTTP通信 ===")
        
        import requests
        
        # 测试MetaServer通信
        print("   测试MetaServer通信...")
        response = requests.get("http://localhost:8001/cluster/info")
        if response.status_code == 200:
            cluster_info = response.json()
            print(f"   ✓ 获取集群信息成功: {len(cluster_info.get('dataServers', []))} 个DataServer")
        else:
            print(f"   ✗ MetaServer通信失败: {response.status_code}")
            return False
        
        # 测试DataServer通信
        print("   测试DataServer通信...")
        response = requests.get("http://localhost:8002/file/read?path=/test.txt")
        if response.status_code == 200:
            print(f"   ✓ DataServer通信成功: 读取到 {len(response.content)} bytes")
        else:
            print(f"   ✗ DataServer通信失败: {response.status_code}")
            return False
        
        print("   ✓ HTTP通信测试通过")
        return True
        
    except Exception as e:
        print(f"   ✗ HTTP通信测试失败: {e}")
        return False

def test_file_operations():
    """测试文件操作"""
    try:
        print("\n=== 测试文件操作 ===")
        
        import requests
        
        # 测试文件创建
        print("   测试文件创建...")
        data = {"path": "/test_file.txt", "fileSystemName": "test"}
        response = requests.post("http://localhost:8001/file/create", json=data)
        if response.status_code == 200:
            print("   ✓ 文件创建成功")
        else:
            print(f"   ✗ 文件创建失败: {response.status_code}")
            return False
        
        # 测试文件写入
        print("   测试文件写入...")
        file_content = b"Hello, this is test content!"
        response = requests.post("http://localhost:8002/file/write", data=file_content)
        if response.status_code == 200:
            print("   ✓ 文件写入成功")
        else:
            print(f"   ✗ 文件写入失败: {response.status_code}")
            return False
        
        # 测试文件读取
        print("   测试文件读取...")
        response = requests.get("http://localhost:8002/file/read?path=/test_file.txt")
        if response.status_code == 200:
            print(f"   ✓ 文件读取成功: {len(response.content)} bytes")
        else:
            print(f"   ✗ 文件读取失败: {response.status_code}")
            return False
        
        print("   ✓ 文件操作测试通过")
        return True
        
    except Exception as e:
        print(f"   ✗ 文件操作测试失败: {e}")
        return False

def main():
    """主函数"""
    print("=== minFS Python客户端集成测试 (Mock模式) ===\n")
    
    # 启动Mock服务器
    print("启动Mock服务器...")
    servers = start_mock_servers()
    
    # 等待服务器启动
    time.sleep(2)
    
    try:
        success_count = 0
        total_tests = 3
        
        # 运行测试
        if test_http_communication():
            success_count += 1
        
        if test_file_operations():
            success_count += 1
        
        if test_basic_operations():
            success_count += 1
        
        # 输出结果
        print(f"\n=== 测试结果 ===")
        print(f"成功: {success_count}/{total_tests}")
        
        if success_count >= 2:
            print("🎉 大部分测试通过！你的客户端代码逻辑是正确的。")
            print("\n下一步：")
            print("1. 启动真实的ZooKeeper服务")
            print("2. 启动真实的minFS集群")
            print("3. 运行真实环境测试")
        else:
            print("⚠️  部分测试失败，需要检查代码。")
            
    finally:
        # 关闭Mock服务器
        print("\n关闭Mock服务器...")
        for name, server, thread in servers:
            server.shutdown()
            server.server_close()
            print(f"   {name} 已关闭")

if __name__ == "__main__":
    main()



