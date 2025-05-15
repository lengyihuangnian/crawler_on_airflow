#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable
from airflow.hooks.base import BaseHook

from utils.xhs_appium import XHSOperator


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


def collect_xhs_notes(**context) -> None:
    """
    收集小红书笔记    
    Args:
        **context: Airflow上下文参数字典
    
    Returns:
        None
    """
    # 获取任务实例对象，用于XCom传递数据
    ti = context['ti']
    # 获取关键词，默认为"AI客服"
    keyword = (context['dag_run'].conf.get('keyword', '广州探店') 
              if context['dag_run'].conf 
              else '广州探店')
    
    # 获取最大收集笔记数，默认为5
    max_notes = int(context['dag_run'].conf.get('max_notes', 5)
                if context['dag_run'].conf
                else 5)
    
    # 获取设备列表
    device_info_list = Variable.get("XHS_DEVICE_INFO_LIST", default_var=[], deserialize_json=True)
    # 获取指定username的设备信息
    target_username = "lucy"  # 设置目标username
    device_info = next((device for device in device_info_list if device.get('username') == target_username), None)
    
    print(f"获取指定username的设备信息: \n{device_info}")
    # 如果找不到指定username的设备，使用默认值
    device_ip = device_info.get('device_ip', '42.193.193.179') if device_info else '42.193.193.179'
    device_port = device_info.get('available_appium_ports', [6030])[0] if device_info else 6030
    device_id = device_info.get('phone_device_list', ['c2c56d1b0107'])[0] if device_info else 'c2c56d1b0107'
    appium_server_url = f"http://{device_ip}:{device_port}"

    #test
    # appium_server_url = 'http://42.193.193.179:6010'
    # device_id = 'c2c5819d0107'

    print(f"开始收集关键词 '{keyword}' 的小红书笔记... ，数量为'{max_notes}")
    
    try:
        # 初始化小红书操作器
        xhs = XHSOperator(appium_server_url=appium_server_url, force_app_launch=True, device_id=device_id)
        
        # 修改收集逻辑，每收集三条数据就保存一次
        all_notes = []  # 存储所有收集的笔记
        batch_size = 3  # 每批次保存的笔记数量
        current_batch = []  # 当前批次的笔记
        
        # 收集笔记
        for i in range(1, max_notes + 1):
            # 每次只收集一条笔记
            single_note_batch = xhs.collect_notes_by_keyword(
                keyword=keyword,
                max_notes=1,
                filters={
                    "note_type": "图文"
                }
            )
            
            # 如果成功收集到笔记
            if single_note_batch:
                note = single_note_batch[0]
                current_batch.append(note)
                all_notes.append(note)
                print(f"已收集第 {len(all_notes)}/{max_notes} 条笔记: {note.get('title', '无标题')}")
                
                # 当收集到3条笔记或达到最大笔记数时保存到数据库
                if len(current_batch) >= batch_size or len(all_notes) >= max_notes:
                    print(f"保存批次数据到数据库，当前批次包含 {len(current_batch)} 条笔记")
                    save_notes_to_db(current_batch)
                    current_batch = []  # 清空当前批次
                
                # 如果已达到最大笔记数，结束收集
                if len(all_notes) >= max_notes:
                    break
            else:
                print(f"未能收集到第 {len(all_notes) + 1} 条笔记，尝试继续收集")
        
        # 如果还有未保存的笔记，保存剩余的笔记
        if current_batch:
            print(f"保存剩余 {len(current_batch)} 条笔记到数据库")
            save_notes_to_db(current_batch)
        
        if not all_notes:
            print(f"未找到关于 '{keyword}' 的笔记")
            return
            
        # 打印收集结果
        print("\n收集完成!")
        print(f"共收集到 {len(all_notes)} 条笔记")
        
        # 提取笔记URL列表并存入XCom
        note_urls = [note.get('note_url', '') for note in all_notes]
        ti.xcom_push(key='note_urls', value=note_urls)
        ti.xcom_push(key='keyword', value=keyword)
        
        return note_urls
            
    except Exception as e:
        error_msg = f"收集小红书笔记失败: {str(e)}"
        print(error_msg)
        raise
    finally:
        # 确保关闭小红书操作器
        if 'xhs' in locals():
            xhs.close()


# DAG 定义
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    dag_id='xhs_notes_collector',
    default_args=default_args,
    description='定时收集小红书笔记',
    schedule_interval=None,
    tags=['小红书'],
    catchup=False,
)

collect_notes_task = PythonOperator(
    task_id='collect_xhs_notes',
    python_callable=collect_xhs_notes,
    provide_context=True,
    dag=dag,
)

collect_notes_task
