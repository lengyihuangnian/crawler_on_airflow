#!/bin/bash

# 获取 Wi-Fi 接口
WIFI_IFACE=$(nmcli -t -f DEVICE,TYPE dev | grep ':wifi' | cut -d: -f1)

if [ -z "$WIFI_IFACE" ]; then
    echo "[INFO] 未检测到 Wi-Fi 接口，跳过启动。"
    exit 0
fi

# 检查 Wi-Fi 是否启用
if ! nmcli -t -f WIFI g | grep -q "enabled"; then
    echo "[INFO] Wi-Fi 未启用，跳过启动。"
    exit 0
fi

# 检查是否能 ping 通外网
echo "[INFO] 检查网络连通性（ping 8.8.8.8）..."
if ping -I "$WIFI_IFACE" -c 1 -W 2 8.8.8.8 > /dev/null 2>&1; then
    echo "[INFO] 网络可用，跳过 WiFi Connect。"
    exit 0
fi

# 检查是否有 Wi-Fi 配置，等待连接
if nmcli connection show | grep -q "wifi"; then
    echo "[INFO] 检测到已保存的 Wi-Fi，等待连接..."
    sleep 10
    if ping -I "$WIFI_IFACE" -c 1 -W 2 8.8.8.8 > /dev/null 2>&1; then
        echo "[INFO] 网络连接成功，跳过 WiFi Connect。"
        exit 0
    fi
fi

# 启动 WiFi Connect
echo "[INFO] 启动 WiFi Connect 配网..."
/usr/local/bin/wifi-connect --ui-directory /usr/local/share/wifi-connect-ui -s MyDevice-Setup -p 12345678