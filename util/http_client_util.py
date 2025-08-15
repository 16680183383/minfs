"""
HTTP客户端工具类
"""

# 全局类型检查忽略
# type: ignore
# pyright: ignore
# mypy: ignore
# flake8: ignore

# type: ignore
import requests  # type: ignore
from requests.adapters import HTTPAdapter  # type: ignore
from urllib3.util.retry import Retry  # type: ignore
from typing import Optional, Dict, Any, Union
import logging

# 类型检查忽略
# pyright: ignore
# mypy: ignore
# flake8: ignore

logger: logging.Logger = logging.getLogger(__name__)


class HttpClientUtil:
    """HTTP客户端工具类"""
    
    def __init__(self, max_retries: int = 3, timeout: int = 30, 
                 max_connections: int = 100):
        """
        初始化HTTP客户端
        
        Args:
            max_retries: 最大重试次数
            timeout: 连接超时时间（秒）
            max_connections: 最大连接数
        """
        self.max_retries = max_retries
        self.timeout = timeout
        self.max_connections = max_connections
        
        # 创建Session并配置重试策略
        self.session: requests.Session = requests.Session()
        
        # 配置重试策略
        retry_strategy: Retry = Retry(
            total=max_retries,
            backoff_factor=0.1,
            status_forcelist=[500, 502, 503, 504]
        )
        
        # 配置连接适配器
        adapter: HTTPAdapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=max_connections,
            pool_maxsize=max_connections
        )
        
        # 挂载适配器
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # 设置默认超时
        self.session.timeout = timeout
    
    def get(self, url: str, params: Optional[Dict[str, Any]] = None, 
            headers: Optional[Dict[str, str]] = None, **kwargs) -> 'requests.Response':
        """
        发送GET请求
        
        Args:
            url: 请求URL
            params: 查询参数
            headers: 请求头
            **kwargs: 其他参数
            
        Returns:
            requests.Response对象
        """
        try:
            response = self.session.get(url, params=params, headers=headers, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"GET request failed: {url}, error: {e}")
            raise
    
    def post(self, url: str, data: Optional[Dict[str, Any]] = None, 
             json_data: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None, 
             **kwargs) -> 'requests.Response':
        """
        发送POST请求
        
        Args:
            url: 请求URL
            data: 表单数据
            json_data: JSON数据
            headers: 请求头
            **kwargs: 其他参数
            
        Returns:
            requests.Response对象
        """
        try:
            response = self.session.post(url, data=data, json=json_data, headers=headers, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"POST request failed: {url}, error: {e}")
            raise
    
    def put(self, url: str, data: Optional[Dict[str, Any]] = None, 
            json_data: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None, 
            **kwargs) -> 'requests.Response':
        """
        发送PUT请求
        
        Args:
            url: 请求URL
            data: 表单数据
            json_data: JSON数据
            headers: 请求头
            **kwargs: 其他参数
            
        Returns:
            requests.Response对象
        """
        try:
            response = self.session.put(url, data=data, json=json_data, headers=headers, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"PUT request failed: {url}, error: {e}")
            raise
    
    def delete(self, url: str, headers: Optional[Dict[str, str]] = None, **kwargs) -> 'requests.Response':
        """
        发送DELETE请求
        
        Args:
            url: 请求URL
            headers: 请求头
            **kwargs: 其他参数
            
        Returns:
            requests.Response对象
        """
        try:
            response = self.session.delete(url, headers=headers, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"DELETE request failed: {url}, error: {e}")
            raise
    
    def upload_file(self, url: str, file_path: str, field_name: str = 'file',
                   additional_data: Optional[Dict[str, Any]] = None) -> 'requests.Response':
        """
        上传文件
        
        Args:
            url: 上传URL
            file_path: 文件路径
            field_name: 文件字段名
            additional_data: 额外的表单数据
            
        Returns:
            requests.Response对象
        """
        try:
            with open(file_path, 'rb') as f:
                files = {field_name: f}
                data = additional_data or {}
                response = self.session.post(url, files=files, data=data)
                response.raise_for_status()
                return response
        except (requests.exceptions.RequestException, IOError) as e:
            logger.error(f"File upload failed: {url}, file: {file_path}, error: {e}")
            raise
    
    def download_file(self, url: str, file_path: str) -> 'requests.Response':
        """
        下载文件
        
        Args:
            url: 下载URL
            file_path: 保存路径
            
        Returns:
            requests.Response对象
        """
        try:
            response = self.session.get(url, stream=True)
            response.raise_for_status()
            
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            return response
        except (requests.exceptions.RequestException, IOError) as e:
            logger.error(f"File download failed: {url}, file: {file_path}, error: {e}")
            raise
    
    def close(self):
        """关闭Session"""
        self.session.close()
    
    def __enter__(self):
        """上下文管理器入口"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.close()
