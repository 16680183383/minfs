#!/bin/bash

# minFS Python Client 编译脚本
# 生成workpublish目录结构

echo "=== 开始编译 minFS Python Client ==="

# 设置变量
PROJECT_NAME="easyClient"
VERSION="1.0"
WORK_DIR="workpublish"
BIN_DIR="${WORK_DIR}/bin"
CLIENT_DIR="${WORK_DIR}/${PROJECT_NAME}"

# 清理旧的构建目录
echo "清理旧的构建目录..."
rm -rf ${WORK_DIR}

# 创建目录结构
echo "创建目录结构..."
mkdir -p ${BIN_DIR}
mkdir -p ${CLIENT_DIR}

# 复制核心Python代码
echo "复制核心代码..."
cp -r core ${CLIENT_DIR}/
cp -r domain ${CLIENT_DIR}/
cp -r util ${CLIENT_DIR}/
cp -r examples ${CLIENT_DIR}/
cp -r tests ${CLIENT_DIR}/

# 复制配置文件
echo "复制配置文件..."
cp requirements.txt ${CLIENT_DIR}/
cp setup.py ${CLIENT_DIR}/
cp README.md ${CLIENT_DIR}/
cp PROJECT_STRUCTURE.md ${CLIENT_DIR}/
cp __init__.py ${CLIENT_DIR}/

# 创建启动脚本
echo "创建启动脚本..."
cat > ${BIN_DIR}/start.sh << 'EOF'
#!/bin/bash

# minFS 一键启动脚本
echo "=== 启动 minFS 服务 ==="

# 检查Python环境
if ! command -v python3 &> /dev/null; then
    echo "错误: 未找到Python3，请先安装Python3"
    exit 1
fi

# 检查依赖
echo "检查Python依赖..."
cd easyClient
pip3 install -r requirements.txt

# 启动客户端测试
echo "启动客户端测试..."
python3 examples/basic_usage.py

echo "=== minFS 服务启动完成 ==="
EOF

# 设置执行权限
chmod +x ${BIN_DIR}/start.sh

# 创建Python包
echo "创建Python包..."
cd ${CLIENT_DIR}
python3 setup.py sdist bdist_wheel

# 创建Python包（tar.gz格式）
echo "创建Python包..."
cd ${CLIENT_DIR}
python3 setup.py sdist bdist_wheel

# 创建tar.gz包（标准Python包格式）
echo "创建tar.gz包..."
mkdir -p temp_tar
cp -r core domain util examples tests __init__.py setup.py requirements.txt README.md PROJECT_STRUCTURE.md temp_tar/
cd temp_tar

# 使用tar创建标准压缩包
tar -czf ../${PROJECT_NAME}-${VERSION}.tar.gz *
cd ..
rm -rf temp_tar

# 创建版本信息文件
echo "创建版本信息..."
cat > VERSION << EOF
项目名称: ${PROJECT_NAME}
版本: ${VERSION}
构建时间: $(date)
Python版本: $(python3 --version)
EOF

# 创建部署说明
echo "创建部署说明..."
cat > DEPLOY.md << 'EOF'
# minFS Python Client 部署说明

## 目录结构
- bin/: 包含启动脚本start.sh
- easyClient/: 包含客户端SDK代码和依赖

## 使用方法
1. 确保系统已安装Python3.8+
2. 运行启动脚本: ./bin/start.sh
3. 或者直接使用: cd easyClient && python3 examples/basic_usage.py

## 依赖要求
- Python 3.8+
- requests
- kazoo
- pytest (测试用)

## 注意事项
- 首次使用需要安装依赖: pip3 install -r easyClient/requirements.txt
- 确保ZooKeeper和minFS集群服务已启动
EOF

echo "=== 编译完成 ==="
echo "生成目录: ${WORK_DIR}"
echo "目录结构:"
tree ${WORK_DIR} || find ${WORK_DIR} -type f

echo ""
echo "✅ 编译成功！"
echo "📁 输出目录: ${WORK_DIR}"
echo "🚀 启动命令: cd ${WORK_DIR} && ./bin/start.sh"
