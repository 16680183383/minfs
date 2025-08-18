#!/bin/bash

# 快速修复Zookeeper Leader选举问题

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== 快速修复ZK Leader选举问题 ===${NC}"

# 停止所有MetaServer
echo -e "${YELLOW}1. 停止所有MetaServer...${NC}"
pkill -f "metaserver" 2>/dev/null || true
sleep 3

# 清理ZK中的错误节点
echo -e "${YELLOW}2. 清理ZK中的错误节点...${NC}"

# 删除所有leader节点
echo "ls /minfs/leader" | zkCli.sh -server localhost:2181 2>/dev/null | grep -E "meta[0-9]+" | while read node; do
    if [ ! -z "$node" ]; then
        echo "delete /minfs/leader/$node" | zkCli.sh -server localhost:2181 2>/dev/null || true
        echo -e "${GREEN}删除Leader节点: $node${NC}"
    fi
done

# 删除所有metaserver节点
echo "ls /minfs/metaservers" | zkCli.sh -server localhost:2181 2>/dev/null | grep -E "meta[0-9]+" | while read node; do
    if [ ! -z "$node" ]; then
        echo "delete /minfs/metaservers/$node" | zkCli.sh -server localhost:2181 2>/dev/null || true
        echo -e "${GREEN}删除MetaServer节点: $node${NC}"
    fi
done

# 删除根路径
echo "rmr /minfs/leader" | zkCli.sh -server localhost:2181 2>/dev/null || true
echo "rmr /minfs/metaservers" | zkCli.sh -server localhost:2181 2>/dev/null || true

echo -e "${GREEN}ZK节点清理完成${NC}"

# 验证清理结果
echo -e "${YELLOW}3. 验证清理结果...${NC}"
echo "ls /minfs" | zkCli.sh -server localhost:2181 2>/dev/null || echo "根路径已清理"

echo -e "${GREEN}=== 修复完成 ===${NC}"
echo ""
echo "现在可以重新启动MetaServer集群:"
echo "  ./bin/start-metaservers.sh"
echo ""
echo "修复后的正确ZK结构:"
echo "  /minfs/"
echo "  ├── /metaservers/"
echo "  │   ├── meta8000 (临时节点)"
echo "  │   ├── meta8001 (临时节点)"
echo "  │   └── meta8002 (临时节点)"
echo "  └── /leader/"
echo "      └── leader (只有一个Leader节点)"
