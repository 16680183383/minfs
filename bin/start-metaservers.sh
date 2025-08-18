#!/bin/bash

# MetaServer集群启动脚本
# 启动三个MetaServer实例（一主二从）

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 配置变量
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK_DIR="$PROJECT_ROOT/workpublish"
JAVA_OPTS="-Xms512m -Xmx1g -XX:+UseG1GC"

# 检查端口占用
check_ports() {
    echo -e "${YELLOW}检查端口占用...${NC}"
    
    local ports=(8000 8001 8002)
    local ports_available=true
    
    for port in "${ports[@]}"; do
        if lsof -ti:$port >/dev/null 2>&1; then
            echo -e "${RED}错误: 端口 $port 已被占用${NC}"
            ports_available=false
        else
            echo -e "${GREEN}端口 $port 可用${NC}"
        fi
    done
    
    if [ "$ports_available" = false ]; then
        echo -e "${RED}端口检查失败，请先清理端口占用${NC}"
        echo -e "${YELLOW}可以使用清理脚本: ./bin/cleanup-metaservers.sh${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}所有端口检查通过${NC}"
}

# 检查Java环境
check_java() {
    if ! command -v java &> /dev/null; then
        echo -e "${RED}错误: 未找到Java环境${NC}"
        exit 1
    fi
    
    JAVA_VERSION=$(java -version 2>&1 | head -n 1 | cut -d'"' -f2)
    echo -e "${GREEN}Java版本: $JAVA_VERSION${NC}"
}

# 检查Zookeeper
check_zk() {
    echo -e "${YELLOW}检查Zookeeper连接...${NC}"
    if ! nc -z localhost 2181 2>/dev/null; then
        echo -e "${RED}错误: Zookeeper未运行，请先启动Zookeeper${NC}"
        echo -e "${YELLOW}启动命令: zkServer.sh start${NC}"
        exit 1
    fi
    echo -e "${GREEN}Zookeeper连接正常${NC}"
}

# 创建必要目录
create_directories() {
    echo -e "${YELLOW}创建必要目录...${NC}"
    
    # MetaServer数据目录
    mkdir -p "$WORK_DIR/metaServer/data/metaserver-8000/rocksdb"
    mkdir -p "$WORK_DIR/metaServer/data/metaserver-8000/backup"
    mkdir -p "$WORK_DIR/metaServer/data/metaserver-8000/logs"
    
    mkdir -p "$WORK_DIR/metaServer/data/metaserver-8001/rocksdb"
    mkdir -p "$WORK_DIR/metaServer/data/metaserver-8001/backup"
    mkdir -p "$WORK_DIR/metaServer/data/metaserver-8001/logs"
    
    mkdir -p "$WORK_DIR/metaServer/data/metaserver-8002/rocksdb"
    mkdir -p "$WORK_DIR/metaServer/data/metaserver-8002/backup"
    mkdir -p "$WORK_DIR/metaServer/data/metaserver-8002/logs"
    
    echo -e "${GREEN}目录创建完成${NC}"
}

# 启动单个MetaServer
start_metaserver() {
    local port=$1
    local role=$2
    
    echo -e "${YELLOW}启动MetaServer (端口: $port, 角色: $role)...${NC}"
    
    cd "$WORK_DIR/metaServer"
    
    # 设置配置文件
    export SPRING_PROFILES_ACTIVE="$port"
    
    # 启动MetaServer
    nohup java $JAVA_OPTS \
        -Dspring.config.location=classpath:/application-$port.yml \
        -Dmetaserver.host=localhost \
        -Dmetaserver.zk.connect.string=localhost:2181 \
        -Dmetaserver.zk.root.path=/minfs \
        -Dmetaserver.cluster.role=$role \
        -jar metaserver-1.0.jar \
        > logs/metaserver-$port.log 2>&1 &
    
    local pid=$!
    echo $pid > "logs/metaserver-$port.pid"
    
    # 等待启动
    sleep 5
    
    # 检查是否启动成功
    if kill -0 $pid 2>/dev/null; then
        echo -e "${GREEN}MetaServer (端口: $port) 启动成功，PID: $pid${NC}"
    else
        echo -e "${RED}MetaServer (端口: $port) 启动失败${NC}"
        exit 1
    fi
}

# 启动所有MetaServer
start_all_metaservers() {
    echo -e "${YELLOW}启动MetaServer集群...${NC}"
    
    # 启动主节点
    start_metaserver 8000 master
    
    # 启动从节点
    start_metaserver 8001 slave
    start_metaserver 8002 slave
    
    echo -e "${GREEN}所有MetaServer启动完成${NC}"
}

# 检查服务状态
check_status() {
    echo -e "${YELLOW}检查MetaServer状态...${NC}"
    
    local ports=(8000 8001 8002)
    local all_running=true
    
    for port in "${ports[@]}"; do
        if curl -s "http://localhost:$port/health" > /dev/null 2>&1; then
            echo -e "${GREEN}MetaServer (端口: $port) 运行正常${NC}"
        else
            echo -e "${RED}MetaServer (端口: $port) 未响应${NC}"
            all_running=false
        fi
    done
    
    if [ "$all_running" = true ]; then
        echo -e "${GREEN}所有MetaServer运行正常${NC}"
    else
        echo -e "${RED}部分MetaServer运行异常${NC}"
        exit 1
    fi
}

# 显示集群信息
show_cluster_info() {
    echo -e "${YELLOW}集群信息:${NC}"
    echo "MetaServer集群:"
    echo "  - 主节点: localhost:8000"
    echo "  - 从节点1: localhost:8001"
    echo "  - 从节点2: localhost:8002"
    echo ""
    echo "Zookeeper: localhost:2181"
    echo "集群根路径: /minfs"
    echo ""
    echo "检查集群状态: curl http://localhost:8000/zk/cluster"
    echo "检查ZK注册: curl http://localhost:8000/zk/check"
}

# 主函数
main() {
    echo -e "${GREEN}=== MinFS MetaServer集群启动脚本 ===${NC}"
    
    # 检查环境
    check_java
    check_zk
    check_ports
    
    # 创建目录
    create_directories
    
    # 启动服务
    start_all_metaservers
    
    # 等待服务完全启动
    echo -e "${YELLOW}等待服务启动完成...${NC}"
    sleep 10
    
    # 检查状态
    check_status
    
    # 显示信息
    show_cluster_info
    
    echo -e "${GREEN}MetaServer集群启动完成！${NC}"
}

# 清理函数
cleanup() {
    echo -e "${YELLOW}清理临时文件...${NC}"
    # 可以在这里添加清理逻辑
}

# 设置信号处理
trap cleanup EXIT

# 执行主函数
main "$@"
