from datetime import datetime, timedelta
import subprocess
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable

def get_remote_devices():
    """通过SSH获取远程主机上的设备列表"""
    try:
        remote_host = Variable.get("REMOTE_TEST_HOST") #user@192.168.1.103
        ssh_key_path = Variable.get("SSH_KEY_PATH", default_var="/root/.ssh/id_rsa") 
        
        # 执行SSH命令获取设备列表
        cmd = f"ssh -i {ssh_key_path} {remote_host} 'adb devices'"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"SSH命令执行失败: {result.stderr}")
            return []
        
        # 解析adb devices输出
        devices = []
        for line in result.stdout.split('\n')[1:]:  # 跳过第一行标题
            if line.strip() and 'device' in line:
                device_id = line.split('\t')[0]
                devices.append(device_id)
        
        # 将设备列表保存到Airflow变量
        Variable.set("XHS_DEVICES", json.dumps(devices))
        print(f"更新设备列表成功: {devices}")
        return devices
    except Exception as e:
        print(f"获取设备列表失败: {str(e)}")
        return []

# DAG 定义
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='update_device_list',
    default_args=default_args,
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