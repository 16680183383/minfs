"""
基础操作测试
"""

import pytest
import tempfile
import os
from unittest.mock import Mock, patch
from easyClient_python import EFileSystem
from easyClient_python.domain import StatInfo, FileType, ReplicaData


class TestBasicOperations:
    """基础操作测试类"""
    
    @pytest.fixture
    def mock_fs(self):
        """创建模拟的文件系统客户端"""
        with patch('easyClient_python.core.e_file_system.ZkUtil') as mock_zk:
            mock_zk.return_value.get_master_meta_server.return_value = Mock(
                get_url=lambda: "http://localhost:8001"
            )
            mock_zk.return_value.get_slave_meta_server.return_value = Mock(
                get_url=lambda: "http://localhost:8002"
            )
            mock_zk.return_value.get_data_servers.return_value = []
            
            fs = EFileSystem("test_namespace")
            return fs
    
    def test_mkdir_success(self, mock_fs):
        """测试创建目录成功"""
        with patch.object(mock_fs, '_call_remote') as mock_call:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_call.return_value = mock_response
            
            result = mock_fs.mkdir("/test_dir")
            assert result is True
    
    def test_mkdir_failure(self, mock_fs):
        """测试创建目录失败"""
        with patch.object(mock_fs, '_call_remote') as mock_call:
            mock_response = Mock()
            mock_response.status_code = 500
            mock_call.return_value = mock_response
            
            result = mock_fs.mkdir("/test_dir")
            assert result is False
    
    def test_delete_success(self, mock_fs):
        """测试删除文件成功"""
        with patch.object(mock_fs, '_call_remote') as mock_call:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_call.return_value = mock_response
            
            result = mock_fs.delete("/test_file.txt")
            assert result is True
    
    def test_delete_failure(self, mock_fs):
        """测试删除文件失败"""
        with patch.object(mock_fs, '_call_remote') as mock_call:
            mock_response = Mock()
            mock_response.status_code = 404
            mock_call.return_value = mock_response
            
            result = mock_fs.delete("/nonexistent_file.txt")
            assert result is False
    
    def test_get_file_stats_success(self, mock_fs):
        """测试获取文件状态成功"""
        with patch.object(mock_fs, '_call_remote') as mock_call:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "path": "/test_file.txt",
                "size": 1024,
                "mtime": 1234567890,
                "type": 2,  # FILE
                "replicaData": [
                    {
                        "id": "replica1",
                        "dsNode": "localhost:8002",
                        "path": "/data/test_file.txt"
                    }
                ]
            }
            mock_call.return_value = mock_response
            
            stats = mock_fs.get_file_stats("/test_file.txt")
            assert stats is not None
            assert stats.get_path() == "/test_file.txt"
            assert stats.get_size() == 1024
            assert stats.get_type() == FileType.FILE
            assert len(stats.get_replica_data()) == 1
    
    def test_get_file_stats_not_found(self, mock_fs):
        """测试获取文件状态失败"""
        with patch.object(mock_fs, '_call_remote') as mock_call:
            mock_response = Mock()
            mock_response.status_code = 404
            mock_call.return_value = mock_response
            
            stats = mock_fs.get_file_stats("/nonexistent_file.txt")
            assert stats is None
    
    def test_list_file_stats_success(self, mock_fs):
        """测试列出目录内容成功"""
        with patch.object(mock_fs, '_call_remote') as mock_call:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "items": [
                    {
                        "path": "/test_dir/file1.txt",
                        "size": 512,
                        "mtime": 1234567890,
                        "type": 2,  # FILE
                        "replicaData": []
                    },
                    {
                        "path": "/test_dir/subdir",
                        "size": 0,
                        "mtime": 1234567890,
                        "type": 3,  # DIRECTORY
                        "replicaData": []
                    }
                ]
            }
            mock_call.return_value = mock_response
            
            items = mock_fs.list_file_stats("/test_dir")
            assert len(items) == 2
            assert items[0].get_path() == "/test_dir/file1.txt"
            assert items[0].get_size() == 512
            assert items[0].get_type() == FileType.FILE
            assert items[1].get_path() == "/test_dir/subdir"
            assert items[1].get_type() == FileType.DIRECTORY
    
    def test_exists_true(self, mock_fs):
        """测试文件存在"""
        with patch.object(mock_fs, 'get_file_stats') as mock_get_stats:
            mock_stats = Mock()
            mock_get_stats.return_value = mock_stats
            
            result = mock_fs.exists("/test_file.txt")
            assert result is True
    
    def test_exists_false(self, mock_fs):
        """测试文件不存在"""
        with patch.object(mock_fs, 'get_file_stats') as mock_get_stats:
            mock_get_stats.return_value = None
            
            result = mock_fs.exists("/nonexistent_file.txt")
            assert result is False
    
    def test_is_file_true(self, mock_fs):
        """测试是文件"""
        with patch.object(mock_fs, 'get_file_stats') as mock_get_stats:
            mock_stats = Mock()
            mock_stats.is_file.return_value = True
            mock_get_stats.return_value = mock_stats
            
            result = mock_fs.is_file("/test_file.txt")
            assert result is True
    
    def test_is_directory_true(self, mock_fs):
        """测试是目录"""
        with patch.object(mock_fs, 'get_file_stats') as mock_get_stats:
            mock_stats = Mock()
            mock_stats.is_directory.return_value = True
            mock_get_stats.return_value = mock_stats
            
            result = mock_fs.is_directory("/test_dir")
            assert result is True
    
    def test_get_cluster_info(self, mock_fs):
        """测试获取集群信息"""
        cluster_info = mock_fs.get_cluster_info()
        assert cluster_info is not None
        assert cluster_info.get_master_meta_server() is not None
        assert cluster_info.get_slave_meta_server() is not None
        assert isinstance(cluster_info.get_data_servers(), list)


class TestFileStreams:
    """文件流测试类"""
    
    @pytest.fixture
    def mock_replica_data(self):
        """创建模拟的副本数据"""
        return [
            ReplicaData("replica1", "localhost:8002", "/data/test.txt"),
            ReplicaData("replica2", "localhost:8003", "/data/test.txt"),
            ReplicaData("replica3", "localhost:8004", "/data/test.txt")
        ]
    
    def test_fs_input_stream_creation(self, mock_replica_data):
        """测试输入流创建"""
        from easyClient_python.core import FSInputStream
        
        with patch('easyClient_python.core.fs_input_stream.HttpClientUtil') as mock_http:
            mock_http.return_value.get.return_value = Mock(
                status_code=200,
                json=lambda: {"size": 1024}
            )
            
            stream = FSInputStream("/test.txt", mock_replica_data)
            assert stream.path == "/test.txt"
            assert len(stream.replica_data) == 3
    
    def test_fs_output_stream_creation(self, mock_replica_data):
        """测试输出流创建"""
        from easyClient_python.core import FSOutputStream
        
        with patch('easyClient_python.core.fs_output_stream.HttpClientUtil') as mock_http:
            mock_http.return_value.post.return_value = Mock(status_code=200)
            
            stream = FSOutputStream("/test.txt", mock_replica_data)
            assert stream.path == "/test.txt"
            assert len(stream.replica_data) == 3
    
    def test_output_stream_write(self, mock_replica_data):
        """测试输出流写入"""
        from easyClient_python.core import FSOutputStream
        
        with patch('easyClient_python.core.fs_output_stream.HttpClientUtil') as mock_http:
            mock_http.return_value.post.return_value = Mock(status_code=200)
            
            stream = FSOutputStream("/test.txt", mock_replica_data)
            stream.write(b"Hello, World!")
            stream.flush()
            
            # 验证调用了HTTP客户端
            assert mock_http.return_value.post.called
    
    def test_output_stream_md5(self, mock_replica_data):
        """测试输出流MD5计算"""
        from easyClient_python.core import FSOutputStream
        
        with patch('easyClient_python.core.fs_output_stream.HttpClientUtil') as mock_http:
            mock_http.return_value.post.return_value = Mock(status_code=200)
            
            stream = FSOutputStream("/test.txt", mock_replica_data)
            stream.write(b"Hello, World!")
            
            md5 = stream.get_md5()
            assert md5 == "65a8e27d8879283831b664bd8b7f0ad4"  # "Hello, World!"的MD5
