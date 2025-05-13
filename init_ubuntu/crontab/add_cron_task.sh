#!/bin/bash

# 自动WiFi连接服务配置文件
# 创建目的：用于在系统启动时自动连接可用的WiFi网络
# 使用方法：
# 0. 创建脚本：sudo nano /etc/systemd/system/auto-wifi.service
# 1. 启用服务：sudo systemctl enable auto-wifi.service
# 2. 启动服务：sudo systemctl start auto-wifi.service
# 3. 查看状态：sudo systemctl status auto-wifi.service
# 4. 查看日志：journalctl -u auto-wifi.service
# 5. 修改路径： ExecStart=/bin/bash /path/to/auto_connect.sh为真实路径
# 6. 修改wifi： ssh上服务器之后，nmcli device wifi connect <SSID> password <密码>；然后再ssh一次 nmcli connection modify <连接名称> connection.autoconnect yes


# 添加定时任务到crontab
SCRIPT_PATH=/root/auto_connect.sh
LOG_FILE="/var/log/auto_wifi.log"

# 确保脚本有执行权限
chmod +x "$SCRIPT_PATH"

# 创建cron任务
(crontab -l 2>/dev/null; echo "* * * * * $SCRIPT_PATH >> $LOG_FILE 2>&1") | crontab -

echo "已添加自动WiFi连接任务到cron，每分钟执行一次"
echo "日志文件: $LOG_FILE"
