#!/bin/bash

# 修复Zookeeper Leader选举问题
# 清理错误的leader节点，重新建立正确的选举机制

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 配置变量
ZK_HOST="localhost:2181"
ZK_ROOT="/minfs"

# 检查ZK连接
check_zk_connection() {
    echo -e "${YELLOW}检查Zookeeper连接...${NC}"
    
    if ! nc -z localhost 2181 2>/dev/null; then
        echo -e "${RED}错误: Zookeeper未运行${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}Zookeeper连接正常${NC}"
}

# 显示当前ZK状态
show_current_zk_state() {
    echo -e "${YELLOW}当前Zookeeper状态:${NC}"
    
    echo -e "\n${YELLOW}1. MetaServer节点:${NC}"
    echo "ls $ZK_ROOT/metaservers" | zkCli.sh -server $ZK_HOST 2>/dev/null | grep -E "meta[0-9]+" || echo "无MetaServer节点"
    
    echo -e "\n${YELLOW}2. Leader节点:${NC}"
    echo "ls $ZK_ROOT/leader" | zkCli.sh -server $ZK_HOST 2>/dev/null | grep -E "meta[0-9]+" || echo "无Leader节点"
    
    echo -e "\n${YELLOW}3. 根路径结构:${NC}"
    echo "ls $ZK_ROOT" | zkCli.sh -server $ZK_HOST 2>/dev/null || echo "根路径不存在"
}

# 清理错误的leader节点
cleanup_leader_nodes() {
    echo -e "${YELLOW}清理错误的Leader节点...${NC}"
    
    # 获取leader路径下的所有节点
    local leader_nodes=$(echo "ls $ZK_ROOT/leader" | zkCli.sh -server $ZK_HOST 2>/dev/null | grep -E "meta[0-9]+" || echo "")
    
    if [ ! -z "$leader_nodes" ]; then
        echo -e "${YELLOW}发现错误的Leader节点:${NC}"
        echo "$leader_nodes"
        
        # 删除所有leader节点
        for node in $leader_nodes; do
            echo -e "${YELLOW}删除Leader节点: $node${NC}"
            echo "delete $ZK_ROOT/leader/$node" | zkCli.sh -server $ZK_HOST 2>/dev/null || true
        done
        
        echo -e "${GREEN}Leader节点清理完成${NC}"
    else
        echo -e "${GREEN}未发现错误的Leader节点${NC}"
    fi
}

# 清理metaservers节点
cleanup_metaserver_nodes() {
    echo -e "${YELLOW}清理MetaServer节点...${NC}"
    
    # 获取metaservers路径下的所有节点
    local metaserver_nodes=$(echo "ls $ZK_ROOT/metaservers" | zkCli.sh -server $ZK_HOST 2>/dev/null | grep -E "meta[0-9]+" || echo "")
    
    if [ ! -z "$metaserver_nodes" ]; then
        echo -e "${YELLOW}发现MetaServer节点:${NC}"
        echo "$metaserver_nodes"
        
        # 删除所有metaserver节点
        for node in $metaserver_nodes; do
            echo -e "${YELLOW}删除MetaServer节点: $node${NC}"
            echo "delete $ZK_ROOT/metaservers/$node" | zkCli.sh -server $ZK_HOST 2>/dev/null || true
        done
        
        echo -e "${GREEN}MetaServer节点清理完成${NC}"
    else
        echo -e "${GREEN}未发现MetaServer节点${NC}"
    fi
}

# 清理根路径
cleanup_root_paths() {
    echo -e "${YELLOW}清理根路径...${NC}"
    
    # 删除leader路径
    echo -e "${YELLOW}删除leader路径${NC}"
    echo "rmr $ZK_ROOT/leader" | zkCli.sh -server $ZK_HOST 2>/dev/null || true
    
    # 删除metaservers路径
    echo -e "${YELLOW}删除metaservers路径${NC}"
    echo "rmr $ZK_ROOT/metaservers" | zkCli.sh -server $ZK_HOST 2>/dev/null || true
    
    echo -e "${GREEN}根路径清理完成${NC}"
}

# 验证清理结果
verify_cleanup() {
    echo -e "${YELLOW}验证清理结果...${NC}"
    
    # 检查metaservers路径
    local metaservers_exist=$(echo "ls $ZK_ROOT" | zkCli.sh -server $ZK_HOST 2>/dev/null | grep "metaservers" || echo "")
    if [ -z "$metaservers_exist" ]; then
        echo -e "${GREEN}metaservers路径已清理${NC}"
    else
        echo -e "${RED}metaservers路径仍存在${NC}"
    fi
    
    # 检查leader路径
    local leader_exist=$(echo "ls $ZK_ROOT" | zkCli.sh -server $ZK_HOST 2>/dev/null | grep "leader" || echo "")
    if [ -z "$leader_exist" ]; then
        echo -e "${GREEN}leader路径已清理${NC}"
    else
        echo -e "${RED}leader路径仍存在${NC}"
    fi
    
    echo -e "${GREEN}清理验证完成${NC}"
}

# 显示修复后的状态
show_fixed_state() {
    echo -e "${GREEN}=== 修复完成 ===${NC}"
    echo ""
    echo "已修复的问题:"
    echo "✅ 清理了错误的Leader节点"
    echo "✅ 清理了MetaServer节点"
    echo "✅ 清理了根路径结构"
    echo ""
    echo "现在可以重新启动MetaServer集群:"
    echo "  ./bin/start-metaservers.sh"
    echo ""
    echo "启动后，正确的ZK结构应该是:"
    echo "  /minfs/"
    echo "  ├── /metaservers/"
    echo "  │   ├── meta8000 (临时节点)"
    echo "  │   ├── meta8001 (临时节点)"
    echo "  │   └── meta8002 (临时节点)"
    echo "  └── /leader/"
    echo "      └── meta{port} (只有一个Leader节点)"
}

# 主函数
main() {
    echo -e "${GREEN}=== MinFS Zookeeper Leader选举修复脚本 ===${NC}"
    
    # 确认操作
    echo -e "${YELLOW}此脚本将清理Zookeeper中的错误节点，修复Leader选举问题${NC}"
    echo -e "${YELLOW}注意: 这将停止所有MetaServer服务${NC}"
    read -p "是否继续? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${YELLOW}操作已取消${NC}"
        exit 0
    fi
    
    # 检查环境
    check_zk_connection
    
    # 显示当前状态
    show_current_zk_state
    
    # 执行清理
    cleanup_leader_nodes
    cleanup_metaserver_nodes
    cleanup_root_paths
    
    # 验证清理结果
    verify_cleanup
    
    # 显示修复结果
    show_fixed_state
}

# 执行主函数
main "$@"
