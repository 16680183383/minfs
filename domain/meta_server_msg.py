"""
MetaServer消息类
"""


class MetaServerMsg:
    """MetaServer消息类"""
    
    def __init__(self, host: str = "", port: int = 0):
        """
        初始化MetaServer消息
        
        Args:
            host: 主机地址
            port: 端口号
        """
        self.host: str = host
        self.port: int = port
    
    def get_host(self) -> str:
        """获取主机地址"""
        return self.host
    
    def set_host(self, host: str):
        """设置主机地址"""
        self.host = host
    
    def get_port(self) -> int:
        """获取端口号"""
        return self.port
    
    def set_port(self, port: int):
        """设置端口号"""
        self.port = port
    
    def get_url(self) -> str:
        """获取完整URL"""
        return f"http://{self.host}:{self.port}"
    
    def __str__(self):
        return f"MetaServerMsg{{host='{self.host}', port={self.port}}}"
    
    def __repr__(self):
        return self.__str__()
