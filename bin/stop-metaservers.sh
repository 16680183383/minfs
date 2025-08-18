#!/bin/bash

# MetaServer集群停止脚本

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 配置变量
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK_DIR="$PROJECT_ROOT/workpublish"

# 停止单个MetaServer
stop_metaserver() {
    local port=$1
    
    local pid_file="$WORK_DIR/metaServer/logs/metaserver-$port.pid"
    
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        
        if kill -0 $pid 2>/dev/null; then
            echo -e "${YELLOW}停止MetaServer (端口: $port, PID: $pid)...${NC}"
            kill $pid
            
            # 等待进程结束
            local count=0
            while kill -0 $pid 2>/dev/null && [ $count -lt 10 ]; do
                sleep 1
                count=$((count + 1))
            done
            
            # 强制杀死进程
            if kill -0 $pid 2>/dev/null; then
                echo -e "${YELLOW}强制停止MetaServer (端口: $port)...${NC}"
                kill -9 $pid
            fi
            
            echo -e "${GREEN}MetaServer (端口: $port) 已停止${NC}"
        else
            echo -e "${YELLOW}MetaServer (端口: $port) 进程不存在${NC}"
        fi
        
        # 删除PID文件
        rm -f "$pid_file"
    else
        echo -e "${YELLOW}MetaServer (端口: $port) PID文件不存在${NC}"
    fi
}

# 停止所有MetaServer
stop_all_metaservers() {
    echo -e "${YELLOW}停止MetaServer集群...${NC}"
    
    local ports=(8000 8001 8002)
    
    for port in "${ports[@]}"; do
        stop_metaserver $port
    done
    
    echo -e "${GREEN}所有MetaServer已停止${NC}"
}

# 检查服务状态
check_status() {
    echo -e "${YELLOW}检查MetaServer状态...${NC}"
    
    local ports=(8000 8001 8002)
    local all_stopped=true
    
    for port in "${ports[@]}"; do
        if curl -s "http://localhost:$port/health" > /dev/null 2>&1; then
            echo -e "${RED}MetaServer (端口: $port) 仍在运行${NC}"
            all_stopped=false
        else
            echo -e "${GREEN}MetaServer (端口: $port) 已停止${NC}"
        fi
    done
    
    if [ "$all_stopped" = true ]; then
        echo -e "${GREEN}所有MetaServer已成功停止${NC}"
    else
        echo -e "${YELLOW}部分MetaServer仍在运行${NC}"
    fi
}

# 清理临时文件
cleanup() {
    echo -e "${YELLOW}清理临时文件...${NC}"
    
    # 清理日志文件
    find "$WORK_DIR/metaServer/logs" -name "*.log" -type f -delete 2>/dev/null || true
    
    # 清理PID文件
    find "$WORK_DIR/metaServer/logs" -name "*.pid" -type f -delete 2>/dev/null || true
    
    echo -e "${GREEN}清理完成${NC}"
}

# 主函数
main() {
    echo -e "${GREEN}=== MinFS MetaServer集群停止脚本 ===${NC}"
    
    # 停止服务
    stop_all_metaservers
    
    # 等待服务完全停止
    echo -e "${YELLOW}等待服务停止完成...${NC}"
    sleep 5
    
    # 检查状态
    check_status
    
    # 清理
    cleanup
    
    echo -e "${GREEN}MetaServer集群停止完成！${NC}"
}

# 执行主函数
main "$@"
