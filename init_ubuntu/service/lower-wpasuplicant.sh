#!/bin/bash

# 降级 wpa_supplicant 脚本
# 此脚本用于将 wpa_supplicant 降级到特定版本以解决热点连接问题
# 作者：LucyAI
# 日期：2025-06-16

set -e  # 遇到错误立即退出

# 显示彩色输出的函数
info() {
    echo -e "\033[0;32m[INFO]\033[0m $1"
}

error() {
    echo -e "\033[0;31m[ERROR]\033[0m $1" >&2
}

warning() {
    echo -e "\033[0;33m[WARNING]\033[0m $1"
}

# 检查是否以 root 权限运行
if [ "$(id -u)" -ne 0 ]; then
    error "此脚本需要 root 权限运行。请使用 sudo 运行此脚本。"
    exit 1
 fi

# 备份原始 sources.list 文件
info "备份原始 sources.list 文件..."
cp /etc/apt/sources.list /etc/apt/sources.list.backup.$(date +%Y%m%d%H%M%S)

# 更新 sources.list 文件
info "更新软件源配置..."
cat > /etc/apt/sources.list << EOF
deb http://old-releases.ubuntu.com/ubuntu/ impish main restricted universe multiverse
deb http://old-releases.ubuntu.com/ubuntu/ impish-updates main restricted universe multiverse
deb http://old-releases.ubuntu.com/ubuntu/ impish-security main restricted universe multiverse
EOF

# 更新软件包列表
info "更新软件包列表..."
apt update || { error "更新软件包列表失败"; exit 1; }

# 降级安装 wpa_supplicant
info "降级安装 wpa_supplicant 到版本 2:2.9.0-21build1..."
apt --allow-downgrades install wpasupplicant=2:2.9.0-21build1 -y || { error "降级 wpa_supplicant 失败"; exit 1; }

# 锁定 wpa_supplicant 版本
info "锁定 wpa_supplicant 版本，防止自动更新..."
apt-mark hold wpasupplicant

# 验证安装版本
info "验证 wpa_supplicant 版本..."
WPA_VERSION=$(wpa_supplicant -v | head -n 1)
echo "当前 wpa_supplicant 版本: $WPA_VERSION"

if echo "$WPA_VERSION" | grep -q "2.9.0"; then
    info "✅ wpa_supplicant 已成功降级到所需版本。"
else
    warning "⚠️ wpa_supplicant 版本可能不是预期的 2.9.0 版本，请检查。"
fi

info "脚本执行完成。"
