#!/bin/bash
set -e

echo "[1/6] 安装依赖..."
sudo apt-get update
sudo apt-get install -y network-manager wireless-tools dnsmasq curl tar

echo "[2/6] 安装 wifi-connect 可执行文件到 /usr/local/bin..."
curl -x http://lucyai:lucyai@proxy.lucyai.ai:8080 -L https://github.com/balena-os/wifi-connect/releases/download/v4.11.83/wifi-connect-x86_64-unknown-linux-gnu.tar.gz \
  -o /tmp/wifi-connect.tar.gz
sudo tar -xzf /tmp/wifi-connect.tar.gz -C /usr/local/bin
sudo chmod +x /usr/local/bin/wifi-connect

echo "[3/6] 安装 UI 到 /usr/local/share/wifi-connect-ui..."
sudo mkdir -p /usr/local/share/wifi-connect-ui
curl -x http://lucyai:lucyai@proxy.lucyai.ai:8080 -L https://github.com/balena-os/wifi-connect/releases/download/v4.11.83/wifi-connect-ui.tar.gz \
  -o /tmp/wifi-connect-ui.tar.gz
sudo tar -xzf /tmp/wifi-connect-ui.tar.gz -C /usr/local/share/wifi-connect-ui

echo "[4/6] 创建 start-if-needed.sh..."
sudo tee /usr/local/bin/start-if-needed.sh > /dev/null <<'EOF'
#!/bin/bash

# 检查 Wi-Fi 是否启用
if ! nmcli -t -f WIFI g | grep -q "enabled"; then
    echo "[INFO] Wi-Fi 未启用，跳过启动。"
    exit 0
fi

# 是否已连接网络
if nmcli -t -f DEVICE,STATE dev | grep -q ":connected"; then
    echo "[INFO] 已连接网络，跳过 WiFi Connect。"
    exit 0
fi

# 是否有保存的 Wi-Fi 配置并尝试自动连接
if nmcli connection show | grep -q "wifi"; then
    echo "[INFO] 检测到已保存 Wi-Fi 配置，等待自动连接..."
    sleep 15
    if nmcli -t -f DEVICE,STATE dev | grep -q ":connected"; then
        echo "[INFO] 自动连接成功，跳过 WiFi Connect。"
        exit 0
    fi
fi

# 启动 WiFi Connect 配网
echo "[INFO] 启动 WiFi Connect 配网..."
/usr/local/bin/wifi-connect --ui-directory /usr/local/share/wifi-connect-ui -s MyDevice-Setup -p 12345678
EOF

sudo chmod +x /usr/local/bin/start-if-needed.sh

echo "[5/6] 创建 systemd 服务..."
sudo tee /etc/systemd/system/wifi-connect.service > /dev/null <<EOF
[Unit]
Description=WiFi Connect (only when needed)
After=network.target

[Service]
ExecStart=/usr/local/bin/start-if-needed.sh
Restart=on-failure
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

echo "[6/6] 启动服务并设置开机自启..."
sudo systemctl daemon-reload
sudo systemctl enable --now wifi-connect.service

echo "[✅ 完成] WiFi Connect 已部署并配置为按需启动。"
echo "[🔎 查看日志] journalctl -u wifi-connect.service -f"