"""
DataServer消息类
"""


class DataServerMsg:
    """DataServer消息类"""
    
    def __init__(self, host: str = "", port: int = 0, file_total: int = 0, 
                 capacity: int = 0, use_capacity: int = 0):
        """
        初始化DataServer消息
        
        Args:
            host: 主机地址
            port: 端口号
            file_total: 文件总数
            capacity: 总容量
            use_capacity: 已用容量
        """
        self.host: str = host
        self.port: int = port
        self.file_total: int = file_total
        self.capacity: int = capacity
        self.use_capacity: int = use_capacity
    
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
    
    def get_file_total(self) -> int:
        """获取文件总数"""
        return self.file_total
    
    def set_file_total(self, file_total: int):
        """设置文件总数"""
        self.file_total = file_total
    
    def get_capacity(self) -> int:
        """获取总容量"""
        return self.capacity
    
    def set_capacity(self, capacity: int):
        """设置总容量"""
        self.capacity = capacity
    
    def get_use_capacity(self) -> int:
        """获取已用容量"""
        return self.use_capacity
    
    def set_use_capacity(self, use_capacity: int):
        """设置已用容量"""
        self.use_capacity = use_capacity
    
    def get_available_capacity(self) -> int:
        """获取可用容量"""
        return self.capacity - self.use_capacity
    
    def get_usage_percentage(self) -> float:
        """获取使用百分比"""
        if self.capacity == 0:
            return 0.0
        return (self.use_capacity / self.capacity) * 100
    
    def get_url(self) -> str:
        """获取完整URL"""
        return f"http://{self.host}:{self.port}"
    
    def __str__(self):
        return f"DataServerMsg{{host='{self.host}', port={self.port}, file_total={self.file_total}, capacity={self.capacity}, use_capacity={self.use_capacity}}}"
    
    def __repr__(self):
        return self.__str__()
