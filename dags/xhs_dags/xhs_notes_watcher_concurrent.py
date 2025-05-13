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



def get_devices_pool_from_remote(port=6010, system_port=8200, **context): 
    """远程控制设备启动参数管理池。含启动参数和对应的端口号"""
    # 获取设备列表
    device_info_list = Variable.get("XHS_DEVICE_INFO_LIST", default_var=[], deserialize_json=True)
    
    # 获取指定username的设备信息
    target_username = "lucy"  # 设置目标username
    device_info = next((device for device in device_info_list if device.get('username') == target_username), None)
    
    if not device_info:
        raise Exception(f"未找到用户 {target_username} 的设备信息")
    
    # 获取设备IP和端口信息
    device_ip = device_info.get('device_ip', '42.193.193.179')
    available_ports = device_info.get('available_appium_ports', [6010])
    device_ids = device_info.get('phone_device_list', ['c2c56d1b0107'])
    
    # 构建设备池
    devices_pool = []
    for idx, (device_id, port) in enumerate(zip(device_ids, available_ports)):
        dev_system_port = system_port + idx * 4  # 为每个设备分配唯一的系统端口
        device_config = {
            "device_id": device_id,
            "port": port,
            "system_port": dev_system_port,
            "appium_server_url": f"http://{device_ip}:{port}"
        }
        devices_pool.append(device_config)
        print(f"设备 {device_id} 配置: {device_config}")
    
    return devices_pool

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
                task,  # 直接传递 task 对象
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
    'owner': 'yueyang',
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
    concurrency=10,
    max_active_tasks=10,
)


@task_group(group_id="device_tasks", dag=dag)
def create_device_tasks():
    """动态创建设备任务组"""
    devices_pool = get_devices_pool_from_remote()
    
    # 初始化已收集笔记URL集合
    collected_notes_urls = set()
    
    tasks = []
    for device in devices_pool:
        device_id = device['device_id']
        
        @task(task_id=f'collect_notes_device_{device_id}', dag=dag)
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
            
            # 计算当前设备需要收集的笔记数量
            notes_per_device = max_notes // len(devices_pool)
            if device_index < max_notes % len(devices_pool):
                notes_per_device += 1
            
            # 获取之前所有设备收集的笔记URL
            previous_notes_urls = context['task_instance'].xcom_pull(task_ids=None, key='collected_notes_urls') or set()
            current_collected_notes_urls = set(previous_notes_urls)
            
            # 创建任务对象
            task = {
                "task_id": 1,
                "type": "collect_notes",
                "keyword": keyword,
                "notes_per_device": notes_per_device,
                "target_url_count": max_notes,
                "device_idx": device_index
            }
            
            result = collect_xhs_notes_for_device(
                device_info=device_info,
                task=task,
                collected_notes_urls=current_collected_notes_urls
            )
            
            if result and result.get('status') == 'success':
                # 处理每个收集结果
                for collect_result in result.get('results', []):
                    if collect_result.get('status') == 'success':
                        # 更新已收集的URL
                        if 'collected_urls' in collect_result:
                            current_collected_notes_urls.update(collect_result['collected_urls'])
                        
                        # 保存收集到的笔记到数据库
                        if 'notes' in collect_result and collect_result['notes']:
                            try:
                                save_notes_to_db(collect_result['notes'])
                                print(f"设备 {device_info['device_id']} 成功保存 {len(collect_result['notes'])} 条笔记到数据库")
                            except Exception as e:
                                print(f"设备 {device_info['device_id']} 保存笔记到数据库失败: {str(e)}")
            
            context['task_instance'].xcom_push(key='collected_notes_urls', value=list(current_collected_notes_urls))
            
            return result
        
        # 将任务添加到任务列表
        tasks.append(collect_notes())
    
    return tasks

# 创建设备任务组
device_tasks = create_device_tasks()

# 设置任务依赖关系
device_tasks