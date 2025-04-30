#!/bin/bash

# 自动WiFi连接脚本，适用于树莓派
# 扫描可用WiFi并尝试使用预设密码列表连接

# 检查是否以root权限运行
if [ "$(id -u)" -ne 0 ]; then
    echo "此脚本需要以root权限运行，请使用sudo"
    exit 1
fi

# 定义密码列表
PASSWORD_LIST=("88888888")

# 扫描可用的WiFi网络
echo "正在扫描可用的WiFi网络..."
APARRAY=($(iwlist wlan0 scan | grep -i essid | awk -F\" '{print $2}'))

if [ ${#APARRAY[@]} -eq 0 ]; then
    echo "未找到可用的WiFi网络，请检查WiFi适配器状态。"
    exit 1
fi

echo "找到 ${#APARRAY[@]} 个WiFi网络"

# 打印所有找到的WiFi网络名称
echo "可用的WiFi网络列表:"
for AP in "${APARRAY[@]}"; do
    if [ -n "$AP" ]; then
        echo "  - $AP"
    fi
done
echo ""

# 尝试连接每个找到的WiFi网络
CONNECTED=false

for AP in "${APARRAY[@]}"; do
    if [ -z "$AP" ]; then
        continue  # 跳过空SSID
    fi
    
    echo "尝试连接到: $AP"
    
    # 尝试每个密码
    for PASSWORD in "${PASSWORD_LIST[@]}"; do
        echo "  使用密码: $PASSWORD"
        
        # 尝试连接
        if nmcli device wifi connect "$AP" password "$PASSWORD" &>/dev/null; then
            echo "成功连接到 $AP 使用密码 $PASSWORD"
            CONNECTED=true
            break 2  # 成功连接后跳出两层循环
        else
            echo "  无法连接到 $AP 使用密码 $PASSWORD"
            
            # 删除失败连接的缓存
            # 查找该AP的连接配置文件并删除
            CON_NAME=$(nmcli -t -f NAME connection show | grep "$AP")
            if [ -n "$CON_NAME" ]; then
                echo "  删除连接缓存: $CON_NAME"
                nmcli connection delete "$CON_NAME" &>/dev/null
            fi
        fi
    done
done

# 检查连接结果
if [ "$CONNECTED" = true ]; then
    echo "WiFi连接成功！"
    # 显示网络信息
    ip addr show wlan0 | grep "inet "
    echo "网络连接信息:"
    nmcli connection show --active
    
    # 测试互联网连接
    if ping -c 1 8.8.8.8 &>/dev/null; then
        echo "互联网连接正常"
    else
        echo "警告: 已连接到WiFi但无法访问互联网"
    fi
else
    echo "无法使用提供的密码列表连接到任何WiFi网络"
    
    # 全面清理失败的连接缓存
    echo "清理所有失败的连接缓存..."
    # 获取当前没有激活的连接列表
    INACTIVE_CONNECTIONS=$(nmcli -t -f NAME connection show | grep -v "$(nmcli -t -f NAME connection show --active)")
    
    if [ -n "$INACTIVE_CONNECTIONS" ]; then
        echo "$INACTIVE_CONNECTIONS" | while read -r CON_NAME; do
            echo "  删除非活动连接: $CON_NAME"
            nmcli connection delete "$CON_NAME" &>/dev/null
        done
    fi
fi

# 最后显示当前网络状态
echo "当前保存的网络连接:"
nmcli connection show
