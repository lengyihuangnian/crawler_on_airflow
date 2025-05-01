#!/bin/bash

# 自动WiFi连接脚本，适用于树莓派
# 扫描可用WiFi并尝试使用预设密码列表连接
# 支持自动重启网卡并重试3次

# 检查是否以root权限运行
if [ "$(id -u)" -ne 0 ]; then
    echo "此脚本需要以root权限运行，请使用sudo"
    exit 1
fi

# 重启网卡函数
restart_wifi() {
    echo "==============================================="
    echo "重启WiFi网卡..."
    # 关闭网卡
    ip link set wlan0 down
    sleep 2
    # 开启网卡
    ip link set wlan0 up
    sleep 3
    echo "WiFi网卡已重启"
    echo "==============================================="
}

# 定义密码列表
PASSWORD_LIST=("88888888")

# 要过滤的密码
FILTER_PASSWORD="@@@@@@@"

# 初始化要过滤的WiFi网络列表
FILTERED_NETWORKS=()

# 最大重试次数
MAX_RETRY=3
retry_count=0
success=false

# 开始重试循环
while [ $retry_count -lt $MAX_RETRY ] && [ "$success" = false ]; do
    retry_count=$((retry_count + 1))
    echo "\n尝试连接 (第 $retry_count 次，共 $MAX_RETRY 次)..."
    
    # 扫描可用的WiFi网络
    echo "正在扫描可用的WiFi网络..."
    # 保存扫描结果到临时文件
    iwlist wlan0 scan > /tmp/wifi_scan.txt
    
    # 提取加密的WiFi网络名称
    APARRAY=($(grep -B2 "WPA\|WPA2\|WEP" /tmp/wifi_scan.txt | grep -i essid | awk -F\" '{print $2}'))
    
    # 如果没有加密网络，则默认显示所有网络
    if [ ${#APARRAY[@]} -eq 0 ]; then
        echo "未找到加密的WiFi网络，正在扫描所有网络..."
        APARRAY=($(grep -i essid /tmp/wifi_scan.txt | awk -F\" '{print $2}'))
    fi
    
    if [ ${#APARRAY[@]} -eq 0 ]; then
        echo "未找到可用的WiFi网络，重启WiFi适配器并重试。"
        restart_wifi
        continue
    fi

    echo "找到 ${#APARRAY[@]} 个WiFi网络 (已过滤开放WiFi)"

    # 测试并过滤可以使用@@@@@@@密码连接的WiFi
    echo "测试并过滤使用特定密码可连接的WiFi网络..."
    
    # 清空过滤列表
    FILTERED_NETWORKS=()
    
    # 遍历所有WiFi网络进行测试
    for AP in "${APARRAY[@]}"; do
        if [ -z "$AP" ]; then
            continue  # 跳过空SSID
        fi
        
        # 尝试使用要过滤的密码连接
        echo "  测试网络: $AP 使用过滤密码..."
        if nmcli device wifi connect "$AP" password "$FILTER_PASSWORD" &>/dev/null; then
            echo "  警告: $AP 可以使用过滤密码连接，已添加到过滤列表"
            FILTERED_NETWORKS+=("$AP")
            
            # 断开这个连接
            nmcli device disconnect wlan0 &>/dev/null
            
            # 删除这个连接的配置
            CON_NAME=$(nmcli -t -f NAME connection show | grep "$AP")
            if [ -n "$CON_NAME" ]; then
                nmcli connection delete "$CON_NAME" &>/dev/null
            fi
        fi
    done
    
    # 打印所有找到的WiFi网络名称，并标记被过滤的
    echo "可用的WiFi网络列表:"
    for AP in "${APARRAY[@]}"; do
        if [ -n "$AP" ]; then
            # 检查是否在过滤列表中
            FILTERED=""
            for FILTERED_AP in "${FILTERED_NETWORKS[@]}"; do
                if [ "$AP" = "$FILTERED_AP" ]; then
                    FILTERED=" [已过滤]"
                    break
                fi
            done
            echo "  - $AP$FILTERED"
        fi
    done
    echo ""

    # 尝试连接每个找到的WiFi网络
    CONNECTED=false

    for AP in "${APARRAY[@]}"; do
        if [ -z "$AP" ]; then
            continue  # 跳过空SSID
        fi
        
        # 检查是否在过滤列表中
        SKIP=false
        for FILTERED_AP in "${FILTERED_NETWORKS[@]}"; do
            if [ "$AP" = "$FILTERED_AP" ]; then
                echo "跳过过滤的网络: $AP"
                SKIP=true
                break
            fi
        done
        
        # 如果在过滤列表中，跳过这个网络
        if [ "$SKIP" = true ]; then
            continue
        fi
        
        echo "尝试连接到: $AP"
        
        # 尝试每个密码
        for PASSWORD in "${PASSWORD_LIST[@]}"; do
            echo "  使用密码: $PASSWORD"
            
            # 尝试连接
            if nmcli device wifi connect "$AP" password "$PASSWORD" &>/dev/null; then
                echo "成功连接到 $AP 使用密码 $PASSWORD"
                CONNECTED=true
                success=true  # 设置成功标记以退出重试循环
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
            
            # 使用PWR LED闪烁表示网络连接成功
            echo "闪烁LED指示网络连接成功..."
            sudo sh -c "echo none > /sys/class/leds/PWR/trigger"
            for i in {1..10}; do
                sudo sh -c "echo 1 > /sys/class/leds/PWR/brightness"
                sleep 0.3
                sudo sh -c "echo 0 > /sys/class/leds/PWR/brightness"
                sleep 0.3
            done
            # 恢复LED默认行为
            sudo sh -c "echo mmc0 > /sys/class/leds/ACT/trigger"
            echo "LED指示完成"
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
    

    # 如果重试次数小于最大重试次数且未成功，重启网卡并继续
    if [ $retry_count -lt $MAX_RETRY ] && [ "$success" = false ]; then
        echo "准备重试连接..."
        restart_wifi
    else
        echo "已达到最大重试次数，仍未成功连接。"
    fi
done

# 如果三次尝试后仍未成功连接，长亮红灯20秒表示失败
if [ "$success" = false ]; then
    echo "连接失败，长亮红灯20秒提示..."
    # 设置PWR LED为手动控制模式
    sudo sh -c "echo none > /sys/class/leds/PWR/trigger"
    # 长亮红灯
    sudo sh -c "echo 1 > /sys/class/leds/PWR/brightness"
    # 等待20秒
    sleep 20
    # 关闭红灯
    sudo sh -c "echo 0 > /sys/class/leds/PWR/brightness"
    # 恢复LED默认行为
    sudo sh -c "echo input > /sys/class/leds/PWR/trigger"
    echo "红灯提示完成"
fi

# 最后显示当前网络状态
echo "当前保存的网络连接:"
nmcli connection show
