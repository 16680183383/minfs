@echo off
REM minFS Python Client 编译脚本 (Windows版本)
REM 生成workpublish目录结构

echo === 开始编译 minFS Python Client ===

REM 设置变量
set PROJECT_NAME=easyClient
set VERSION=1.0
set WORK_DIR=workpublish
set BIN_DIR=%WORK_DIR%\bin
set CLIENT_DIR=%WORK_DIR%\%PROJECT_NAME%

REM 清理旧的构建目录
echo 清理旧的构建目录...
if exist %WORK_DIR% rmdir /s /q %WORK_DIR%

REM 创建目录结构
echo 创建目录结构...
mkdir %BIN_DIR%
mkdir %CLIENT_DIR%

REM 复制核心Python代码
echo 复制核心代码...
xcopy core %CLIENT_DIR%\core /e /i /y
xcopy domain %CLIENT_DIR%\domain /e /i /y
xcopy util %CLIENT_DIR%\util /e /i /y
xcopy examples %CLIENT_DIR%\examples /e /i /y
xcopy tests %CLIENT_DIR%\tests /e /i /y

REM 复制配置文件
echo 复制配置文件...
copy requirements.txt %CLIENT_DIR%\
copy setup.py %CLIENT_DIR%\
copy README.md %CLIENT_DIR%\
copy PROJECT_STRUCTURE.md %CLIENT_DIR%\
copy __init__.py %CLIENT_DIR%\

REM 创建启动脚本
echo 创建启动脚本...
echo @echo off > %BIN_DIR%\start.bat
echo REM minFS 一键启动脚本 >> %BIN_DIR%\start.bat
echo echo === 启动 minFS 服务 === >> %BIN_DIR%\start.bat
echo. >> %BIN_DIR%\start.bat
echo REM 检查Python环境 >> %BIN_DIR%\start.bat
echo python --version ^>nul 2^>^&1 >> %BIN_DIR%\start.bat
echo if errorlevel 1 ( >> %BIN_DIR%\start.bat
echo     echo 错误: 未找到Python，请先安装Python >> %BIN_DIR%\start.bat
echo     pause >> %BIN_DIR%\start.bat
echo     exit /b 1 >> %BIN_DIR%\start.bat
echo ) >> %BIN_DIR%\start.bat
echo. >> %BIN_DIR%\start.bat
echo REM 检查依赖 >> %BIN_DIR%\start.bat
echo echo 检查Python依赖... >> %BIN_DIR%\start.bat
echo cd %PROJECT_NAME% >> %BIN_DIR%\start.bat
echo pip install -r requirements.txt >> %BIN_DIR%\start.bat
echo. >> %BIN_DIR%\start.bat
echo REM 启动客户端测试 >> %BIN_DIR%\start.bat
echo echo 启动客户端测试... >> %BIN_DIR%\start.bat
echo python examples\basic_usage.py >> %BIN_DIR%\start.bat
echo. >> %BIN_DIR%\start.bat
echo echo === minFS 服务启动完成 === >> %BIN_DIR%\start.bat
echo pause >> %BIN_DIR%\start.bat

REM 创建Python包
echo 创建Python包...
cd %CLIENT_DIR%
python setup.py sdist bdist_wheel

REM 创建Python包（tar.gz格式）
echo 创建Python包...
cd %CLIENT_DIR%
python setup.py sdist bdist_wheel

REM 创建tar.gz包（标准Python包格式）
echo 创建tar.gz包...
mkdir temp_tar
xcopy core temp_tar\core /e /i /y
xcopy domain temp_tar\domain /e /i /y
xcopy util temp_tar\util /e /i /y
xcopy examples temp_tar\examples /e /i /y
xcopy tests temp_tar\tests /e /i /y
copy __init__.py temp_tar\
copy setup.py temp_tar\
copy requirements.txt temp_tar\
copy README.md temp_tar\
copy PROJECT_STRUCTURE.md temp_tar\

REM 使用PowerShell创建tar.gz文件
cd temp_tar
powershell -command "Compress-Archive -Path * -DestinationPath ..\%PROJECT_NAME%-%VERSION%.tar.gz"
cd ..
rmdir /s /q temp_tar

REM 创建版本信息文件
echo 创建版本信息...
echo 项目名称: %PROJECT_NAME% > VERSION
echo 版本: %VERSION% >> VERSION
echo 构建时间: %date% %time% >> VERSION
python --version >> VERSION 2>&1

REM 创建部署说明
echo 创建部署说明...
echo # minFS Python Client 部署说明 > DEPLOY.md
echo. >> DEPLOY.md
echo ## 目录结构 >> DEPLOY.md
echo - bin/: 包含启动脚本start.bat >> DEPLOY.md
echo - %PROJECT_NAME%/: 包含客户端SDK代码和依赖 >> DEPLOY.md
echo. >> DEPLOY.md
echo ## 使用方法 >> DEPLOY.md
echo 1. 确保系统已安装Python3.8+ >> DEPLOY.md
echo 2. 运行启动脚本: .\bin\start.bat >> DEPLOY.md
echo 3. 或者直接使用: cd %PROJECT_NAME% ^&^& python examples\basic_usage.py >> DEPLOY.md
echo. >> DEPLOY.md
echo ## 依赖要求 >> DEPLOY.md
echo - Python 3.8+ >> DEPLOY.md
echo - requests >> DEPLOY.md
echo - kazoo >> DEPLOY.md
echo - pytest (测试用) >> DEPLOY.md
echo. >> DEPLOY.md
echo ## 注意事项 >> DEPLOY.md
echo - 首次使用需要安装依赖: pip install -r %PROJECT_NAME%\requirements.txt >> DEPLOY.md
echo - 确保ZooKeeper和minFS集群服务已启动 >> DEPLOY.md

echo === 编译完成 ===
echo 生成目录: %WORK_DIR%
echo 目录结构:
dir /s %WORK_DIR%

echo.
echo ✅ 编译成功！
echo 📁 输出目录: %WORK_DIR%
echo 🚀 启动命令: cd %WORK_DIR% ^&^& .\bin\start.bat
pause
