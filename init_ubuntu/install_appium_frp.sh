#!/bin/bash

# 颜色定义
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # 无颜色

# 显示带颜色的进度信息
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查命令是否成功执行
check_status() {
    if [ $? -eq 0 ]; then
        log_success "$1 完成"
    else
        log_error "$1 失败"
        exit 1
    fi
}

# 显示安装进度
show_progress() {
    local step=$1
    local total=$2
    local percent=$((step * 100 / total))
    local completed=$((percent / 2))
    local remaining=$((50 - completed))
    
    printf "\r进度: ["
    printf "%${completed}s" | tr ' ' '#'
    printf "%${remaining}s" | tr ' ' ' '
    printf "] %d%%" $percent
}

# 开始安装
log_info "开始安装必要组件，共14个步骤"

# 步骤1: 安装基础工具
show_progress 1 14
log_info "安装 wget 和 unzip..."
sudo apt update && sudo apt install wget unzip -y
check_status "基础工具安装"

# 步骤2: 下载FRP
show_progress 2 14
# log_info "下载 FRP..."
# wget https://github.com/fatedier/frp/releases/download/v0.51.3/frp_0.51.3_linux_amd64.tar.gz
# check_status "FRP下载"

# 步骤3: 解压FRP
show_progress 3 14
log_info "解压 FRP..."
tar -zxvf frp_0.51.3_linux_amd64.tar.gz
check_status "FRP解压"

# 步骤4: 设置Node.js源
show_progress 4 14
log_info "安装curl..."
sudo apt install curl
log_info "设置Node.js源..."
curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash -
check_status "Node.js源设置"

# 步骤5: 安装Node.js和npm
show_progress 5 14
log_info "安装Node.js..."
sudo apt-get install nodejs -y
check_status "Node.js安装"
log_info "安装npm..."
sudo apt-get install npm -y
check_status "npm安装"
sudo apt install -y python3-pip
check_status "pip3安装"

# 步骤6: 安装Appium
show_progress 6 14
log_info "安装Appium (全局)..."
sudo npm install -g appium
check_status "Appium安装"

# 步骤7: 安装Appium Python客户端
show_progress 7 14
log_info "安装Appium Python客户端..."
sudo pip install Appium-Python-Client -i https://pypi.tuna.tsinghua.edu.cn/simple --trusted-host pypi.tuna.tsinghua.edu.cn
check_status "Appium Python客户端安装"

# 步骤8: 安装Java和Android SDK
show_progress 8 14
log_info "安装Java和Android SDK..."
sudo apt install -y default-jdk android-sdk
check_status "Java和Android SDK安装"

# 步骤9: 设置Android环境变量
show_progress 9 14
log_info "设置Android环境变量..."
echo 'export ANDROID_HOME=/usr/lib/android-sdk' >> ~/.bashrc
check_status "Android HOME环境变量设置"

# 步骤10: 设置Android PATH环境变量
show_progress 10 14
log_info "设置Android PATH环境变量..."
echo 'export PATH=/root/.local/bin:/usr/lib/android-sdk/platform-tools:/usr/lib/android-sdk/cmdline-tools/latest/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin' >> ~/.bashrc
check_status "Android PATH环境变量设置"

# 步骤11: 应用环境变量
show_progress 11 14
log_info "应用环境变量..."
source ~/.bashrc
check_status "环境变量应用"

# 步骤12: 安装ADB
show_progress 12 14
log_info "安装ADB..."
sudo apt install -y adb
check_status "ADB安装"

# 步骤13: 安装Appium UIAutomator2驱动
show_progress 13 14
log_info "安装Appium UIAutomator2驱动..."
npm config set registry https://registry.npmmirror.com
npm install appium-uiautomator2-driver
check_status "Appium UIAutomator2驱动安装"

# 步骤14: 安装Appium UIAutomator2驱动到Appium
show_progress 14 14
log_info "安装UIAutomator2驱动到Appium..."
appium driver install uiautomator2
check_status "UIAutomator2驱动安装到Appium"

log_success "所有组件安装完成！"
echo ""
log_info "请运行以下命令使环境变量生效:"
echo "source ~/.bashrc"
echo ""
log_info "验证安装:"
echo "appium -v"
echo "adb version"
echo "java -version"
echo "appium driver list"