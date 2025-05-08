#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
更新设备列表和Appium端口 DAG

这个 DAG 用于定期从远程主机获取可用的 Android 设备列表和Appium可用端口，并将其保存到 Airflow 变量中，
以便其他 DAG 可以使用这些设备和端口进行分布式任务处理。

主要功能：
1. 通过 SSH 连接到远程主机
2. 执行 adb devices 命令获取设备列表
3. 检查6001-6033端口中哪些可用于Appium
4. 解析设备列表和端口，并保存到 Airflow 变量中
"""

# 标准库导入
from datetime import datetime, timedelta
import json
import socket

# 第三方库导入
import paramiko

# Airflow相关导入
from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.python import PythonOperator


def check_port_availability(ssh_client, port):
    """检查远程主机上的端口是否正在运行Appium服务
    
    Args:
        ssh_client: SSH客户端连接
        port: 要检查的端口号
        
    Returns:
        bool: 如果端口被Appium服务占用返回True，否则返回False
    """
    # 首先检查端口是否被占用
    command = f"netstat -tuln | grep :{port}"
    stdin, stdout, stderr = ssh_client.exec_command(command)
    output = stdout.read().decode('utf-8')
    
    if not output.strip():
        return False
        
    # 检查是否有Appium进程在使用该端口
    command = f"ps aux | grep appium | grep {port}"
    stdin, stdout, stderr = ssh_client.exec_command(command)
    output = stdout.read().decode('utf-8')
    
    # 如果找到Appium进程且包含指定端口，则返回True
    if output.strip() and str(port) in output:
        return True
        
    return False


def get_remote_devices():
    """通过SSH获取远程主机上的设备列表和可用的Appium端口"""
    device_info_list = Variable.get("XHS_DEVICE_INFO_LIST", default_var=[], deserialize_json=True)
    print(f"XHS_DEVICE_INFO_LIST: {len(device_info_list)}")

    for device_info in device_info_list:
        print(f"checking host: {device_info}")
        ssh_client = None
        try:
            device_ip = device_info['device_ip']
            username = device_info['username']
            password = device_info['password']
            port = device_info['port']
            
            # 创建SSH客户端
            ssh_client = paramiko.SSHClient()

            # 自动添加主机密钥
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            # 连接到远程服务器
            ssh_client.connect(hostname=device_ip, username=username, password=password, port=port)
                        
            # 构建adb pull命令（指定设备）
            adb_command = f"adb devices"
            
            # 执行命令
            stdin, stdout, stderr = ssh_client.exec_command(adb_command)
            
            # 获取命令输出
            output = stdout.read().decode('utf-8')
            error = stderr.read().decode('utf-8')
            
            # 解析设备列表
            devices = []
            for line in output.split('\n'):
                if line.strip() and 'device' in line and "devices" not in line:
                    device_id = line.split()[0]
                    devices.append(device_id.strip())
            print(f"devices: {devices}")
            
            # 检查Appium可用端口（6001-6033）
            available_appium_ports = []
            for appium_port in range(6001, 6034):
                if check_port_availability(ssh_client, appium_port):
                    available_appium_ports.append(appium_port)
            print(f"available_appium_ports: {available_appium_ports}")

            # 更新设备信息
            device_info['available_appium_ports'] = available_appium_ports
            device_info['appium_port_num'] = len(available_appium_ports)    
            device_info['phone_device_list'] = devices
            device_info['phone_device_num'] = len(devices)
            device_info['update_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        except Exception as e:
            print(f"An error occurred with host {device_ip}: {e}")
            # 继续检查下一个主机，而不是直接返回
            continue
        finally:
            # 确保无论如何都会关闭SSH连接
            if ssh_client:
                ssh_client.close()
                print("SSH connection closed")
    
    # 更新Airflow变量
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    Variable.set("XHS_DEVICE_INFO_LIST", device_info_list, serialize_json=True, description=f"更新时间: {timestamp}")
    print(f"device_info_list:\n{json.dumps(device_info_list, ensure_ascii=False, indent=2)}")
    
    return device_info_list  # 返回所有成功检查的主机信息

# DAG 定义
dag = DAG(
    dag_id='update_device_list',
    default_args={'owner': 'yueyang', 'start_date': datetime(2025, 4, 30)},
    description='定期更新设备列表和Appium可用端口',
    schedule_interval='*/5 * * * *',  # 每10分钟执行一次
    max_active_runs=1,
    tags=['设备管理'],
    catchup=False,
)

# 更新设备列表和可用端口的任务
update_devices_task = PythonOperator(
    task_id='update_devices',
    python_callable=get_remote_devices,
    dag=dag,
)

update_devices_task
