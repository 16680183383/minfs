#!/bin/bash

# MetaServer清理脚本
# 清理端口占用、进程和临时文件

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 配置变量
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK_DIR="$PROJECT_ROOT/workpublish"

# 清理端口占用
cleanup_ports() {
    echo -e "${YELLOW}清理端口占用...${NC}"
    
    local ports=(8000 8001 8002)
    
    for port in "${ports[@]}"; do
        local pid=$(lsof -ti:$port 2>/dev/null)
        if [ ! -z "$pid" ]; then
            echo -e "${YELLOW}发现端口 $port 被进程 $pid 占用，正在停止...${NC}"
            kill -9 $pid 2>/dev/null || true
            sleep 2
        else
            echo -e "${GREEN}端口 $port 未被占用${NC}"
        fi
    done
}

# 清理MetaServer进程
cleanup_processes() {
    echo -e "${YELLOW}清理MetaServer进程...${NC}"
    
    # 查找Java进程
    local java_pids=$(ps aux | grep "metaserver" | grep -v grep | awk '{print $2}' 2>/dev/null || true)
    
    if [ ! -z "$java_pids" ]; then
        echo -e "${YELLOW}发现MetaServer进程: $java_pids${NC}"
        for pid in $java_pids; do
            echo -e "${YELLOW}停止进程 $pid...${NC}"
            kill -9 $pid 2>/dev/null || true
        done
        sleep 3
    else
        echo -e "${GREEN}未发现MetaServer进程${NC}"
    fi
}

# 清理数据目录
cleanup_data_dirs() {
    echo -e "${YELLOW}清理数据目录...${NC}"
    
    if [ -d "$WORK_DIR" ]; then
        # 清理RocksDB锁文件
        find "$WORK_DIR" -name "LOCK" -type f -delete 2>/dev/null || true
        find "$WORK_DIR" -name "*.log" -type f -delete 2>/dev/null || true
        find "$WORK_DIR" -name "*.pid" -type f -delete 2>/dev/null || true
        
        # 清理临时RocksDB目录
        find "$WORK_DIR" -name "rocksdb_metadata" -type d -exec rm -rf {} + 2>/dev/null || true
        
        echo -e "${GREEN}数据目录清理完成${NC}"
    else
        echo -e "${YELLOW}工作目录不存在: $WORK_DIR${NC}"
    fi
}

# 清理ZK临时节点
cleanup_zk_nodes() {
    echo -e "${YELLOW}清理Zookeeper临时节点...${NC}"
    
    # 检查ZK连接
    if nc -z localhost 2181 2>/dev/null; then
        echo -e "${YELLOW}连接到Zookeeper清理临时节点...${NC}"
        
        # 使用zkCli清理临时节点（如果存在）
        echo "ls /minfs/metaservers" | zkCli.sh -server localhost:2181 2>/dev/null | grep -E "meta[0-9]+" | while read node; do
            if [ ! -z "$node" ]; then
                echo "delete /minfs/metaservers/$node" | zkCli.sh -server localhost:2181 2>/dev/null || true
                echo -e "${GREEN}清理ZK节点: $node${NC}"
            fi
        done
        
        echo -e "${GREEN}Zookeeper节点清理完成${NC}"
    else
        echo -e "${YELLOW}Zookeeper未运行，跳过节点清理${NC}"
    fi
}

# 检查系统状态
check_system_status() {
    echo -e "${YELLOW}检查系统状态...${NC}"
    
    local ports=(8000 8001 8002)
    local all_free=true
    
    for port in "${ports[@]}"; do
        if lsof -ti:$port >/dev/null 2>&1; then
            echo -e "${RED}端口 $port 仍被占用${NC}"
            all_free=false
        else
            echo -e "${GREEN}端口 $port 可用${NC}"
        fi
    done
    
    if [ "$all_free" = true ]; then
        echo -e "${GREEN}所有端口都已释放${NC}"
    else
        echo -e "${YELLOW}部分端口仍被占用，可能需要手动清理${NC}"
    fi
}

# 显示清理结果
show_cleanup_result() {
    echo -e "${GREEN}=== 清理完成 ===${NC}"
    echo ""
    echo "已清理的内容:"
    echo "✅ 端口占用 (8000, 8001, 8002)"
    echo "✅ MetaServer进程"
    echo "✅ 数据目录和锁文件"
    echo "✅ Zookeeper临时节点"
    echo ""
    echo "现在可以重新启动MetaServer集群:"
    echo "  ./bin/start-metaservers.sh"
}

# 主函数
main() {
    echo -e "${GREEN}=== MinFS MetaServer清理脚本 ===${NC}"
    
    # 确认操作
    echo -e "${YELLOW}此脚本将清理所有MetaServer相关的进程、端口和文件${NC}"
    read -p "是否继续? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${YELLOW}操作已取消${NC}"
        exit 0
    fi
    
    # 执行清理
    cleanup_ports
    cleanup_processes
    cleanup_data_dirs
    cleanup_zk_nodes
    
    # 等待清理完成
    echo -e "${YELLOW}等待清理完成...${NC}"
    sleep 5
    
    # 检查状态
    check_system_status
    
    # 显示结果
    show_cleanup_result
}

# 执行主函数
main "$@"
