#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
更新设备列表 DAG

这个 DAG 用于定期从远程主机获取可用的 Android 设备列表，并将其保存到 Airflow 变量中，
以便其他 DAG 可以使用这些设备进行分布式任务处理。

主要功能：
1. 通过 SSH 连接到远程主机
2. 执行 adb devices 命令获取设备列表
3. 解析设备列表并保存到 Airflow 变量中
"""

# 标准库导入
from datetime import datetime, timedelta

# 第三方库导入
import paramiko

# Airflow相关导入
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable


def get_remote_devices():
    """通过SSH获取远程主机上的设备列表"""
    xhs_host_list = Variable.get("XHS_HOST_LIST", default_var=[], deserialize_json=True)
    print(f"xhs_host_list: {xhs_host_list}")

    for host_info in xhs_host_list:
        print(f"checking host: {host_info}")
        device_ip = host_info['device_ip']
        username = host_info['username']
        password = host_info['password']
        port = host_info['port']
        
        # 创建SSH客户端
        ssh_client = paramiko.SSHClient()
        try:
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
                if line.strip() and 'device' in line:
                    device_id = line.split()[0]
                    devices.append(device_id.strip())
            print(f"devices: {devices}")
                
        except Exception as e:
            print(f"An error occurred: {e}")
            return None
        finally:
            # 确保无论如何都会关闭SSH连接
            if ssh_client:
                ssh_client.close()
                print("SSH connection closed")
            

# DAG 定义
dag = DAG(
    dag_id='update_device_list',
    default_args={'owner': 'airflow', 'depends_on_past': False},
    description='定期更新设备列表',
    schedule_interval='*/10 * * * *',  # 每10分钟执行一次
    tags=['设备管理'],
    catchup=False,
)

# 更新设备列表的任务
update_devices_task = PythonOperator(
    task_id='update_devices',
    python_callable=get_remote_devices,
    dag=dag,
)

update_devices_task
