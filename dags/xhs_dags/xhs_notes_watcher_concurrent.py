#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable
from airflow.hooks.base import BaseHook
from airflow.decorators import task, task_group

from utils.xhs_appium import XHSOperator
from utils.device_manager import DeviceManager, TaskDistributor, TaskProcessorManager, collect_notes_processor



def save_notes_to_db(notes: list) -> None:
    """
    保存笔记到数据库(如果表不存在，则初始新建该表)
    """
    # 使用get_hook函数获取数据库连接
    db_hook = BaseHook.get_connection("xhs_db").get_hook()
    db_conn = db_hook.get_conn()
    cursor = db_conn.cursor()

    try:
        # 检查表是否存在，如果不存在则创建
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS xhs_notes (
            id INT AUTO_INCREMENT PRIMARY KEY,
            keyword TEXT,
            title TEXT NOT NULL,
            author TEXT,
            content TEXT,
            likes INT DEFAULT 0,
            collects INT DEFAULT 0,
            comments INT DEFAULT 0,
            note_url TEXT,
            collect_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP  
        )
        """)
        db_conn.commit()
        
        # 准备插入数据的SQL语句
        insert_sql = """
        INSERT INTO xhs_notes 
        (keyword, title, author, content, likes, collects, comments, note_url, collect_time) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # 批量插入笔记数据
        insert_data = []
        for note in notes:
            insert_data.append((
                note.get('keyword', ''),
                note.get('title', ''),
                note.get('author', ''),
                note.get('content', ''),
                note.get('likes', 0),
                note.get('collects', 0),
                note.get('comments', 0),
                note.get('note_url', ''),
                note.get('collect_time', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            ))
        
        cursor.executemany(insert_sql, insert_data)
        db_conn.commit()
        
        print(f"成功保存 {len(notes)} 条笔记到数据库")
        
    except Exception as e:
        db_conn.rollback()
        print(f"保存笔记到数据库失败: {str(e)}")
        raise
    finally:
        cursor.close()
        db_conn.close()

def get_adb_devices_from_remote(remote_host, **context):
    """调用dag，从远程主机获取设备池"""
    # test-使用预定义的设备信息
    devices = [
        {"device_id": "97266a1f0107", "port": 6001},
        {"device_id": "c2c56d1b0107", "port": 6002}
    ]
    print(f"Using devices: {[d['device_id'] for d in devices]}")
    return devices

def get_devices_pool_from_remote(port=6001, system_port=8200, **context): 
    """远程控制设备启动参数管理池。含启动参数和对应的端口号"""
    appium_server_url = Variable.get("APPIUM_SERVER_CONCURRENT_URL", "http://localhost")
    remote_host = Variable.get("REMOTE_TEST_HOST", "localhost")
    #获取远程主机连接的设备
    devices_pool = get_adb_devices_from_remote(remote_host)
    
    # 构建设备池，使用已配置的Appium服务端口
    devs_pool = []
    for idx, device in enumerate(devices_pool):
        dev_port = device["port"]  # 使用设备预定义的端口
        dev_system_port = system_port + idx * 4  # 为每个设备分配唯一的系统端口
        new_dict = {
            "device_id": device["device_id"],
            "port": dev_port,
            "system_port": dev_system_port,
            "appium_server_url": f"{appium_server_url}:{dev_port - 1278}" 
        }
        devs_pool.append(new_dict)
        print(f"设备 {device['device_id']} 配置: {new_dict}")
    return devs_pool

def collect_xhs_notes_for_device(device_info, task, **context) -> None:
    """
    为单个设备收集小红书笔记    
    Args:
        **context: Airflow上下文参数字典
    
    Returns:
        None
    """
    try:
        # 创建XHSOperator实例
        xhs = XHSOperator(
            appium_server_url=device_info['appium_server_url'],
            force_app_launch=True,
            device_id=device_info['device_id'],
            system_port=device_info['system_port']
        )
        
        try:
            all_results = []
            # 执行笔记收集
            result = collect_notes_processor(
                {"task": task},
                device_info,
                xhs   
            )
            all_results.append(result)
                
            return {
                "status": "success",
                "device_id": device_info['device_id'],
                "results": all_results
            }
        finally:
            xhs.close()
    
    except Exception as e:
        return {
            "status": "error",
            "device_id": device_info['device_id'],
            "error": str(e),            
        }   

   

# DAG 定义
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    dag_id='xhs_notes_collector_concurrent',
    default_args=default_args,
    description='定时并发收集小红书笔记',
    schedule_interval=None,
    tags=['小红书'],
    catchup=False,
    max_active_runs=1,
    concurrency=2,
    max_active_tasks=2,
)


@task_group(group_id="device_tasks", dag=dag)
def create_device_tasks():
    """动态创建设备任务组"""
    devices_pool = get_devices_pool_from_remote()
    
    # 初始化已收集笔记URL集合
    collected_notes_urls = set()
    
    for device in devices_pool:
        device_id = device['device_id']
        
        @task(task_id=f'collect_notes_device_{device_id}')
        def collect_notes(device_info=device, **context):
            # 获取关键词和最大笔记数
            keyword = (context['dag_run'].conf.get('keyword', '黑糖波波') 
                      if context['dag_run'].conf 
                      else '黑糖波波')
            
            max_notes = (context['dag_run'].conf.get('max_notes', 3)
                        if context['dag_run'].conf
                        else 3)
            
            # 获取设备索引
            device_index = next(i for i, dev in enumerate(devices_pool) if dev['device_id'] == device_info['device_id'])
            
            # 获取之前所有设备收集的笔记URL
            previous_notes_urls = context['task_instance'].xcom_pull(task_ids=None, key='collected_notes_urls') or set()
            current_collected_notes_urls = set(previous_notes_urls)
            
            # 创建任务对象
            task = {
                "task_id": 1,
                "type": "collect_notes",
                "keyword": keyword,
                "notes_per_device": max_notes,
                "target_url_count": max_notes,
                "device_idx": device_index
            }
            
            result = collect_xhs_notes_for_device(
                device_info=device_info,
                task=task,
                collected_notes_urls=current_collected_notes_urls
            )
            
            if result and 'notes_url' in result:
                current_collected_notes_urls.update(result['notes_url'])
            
            context['task_instance'].xcom_push(key='collected_notes_urls', value=list(current_collected_notes_urls))
            
            return result
        
        collect_notes()

# 创建设备任务组
device_tasks = create_device_tasks()
