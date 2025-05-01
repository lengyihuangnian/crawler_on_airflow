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
    max_notes = int(context['dag_run'].conf.get('max_notes', 2)
                if context['dag_run'].conf
                else 2)
    
    # 获取Appium服务器URL
    appium_server_url = Variable.get("APPIUM_LOCAL_SERVER_URL", "http://localhost:4723")
    
    print(f"开始收集关键词 '{keyword}' 的小红书笔记...")
    
    try:
        # 初始化小红书操作器
        xhs = XHSOperator(appium_server_url=appium_server_url, force_app_launch=True, device_id='63ebd8370906')
        
        # 收集笔记
        notes = xhs.collect_notes_by_keyword(
            keyword=keyword,
            max_notes=max_notes,
            filters={
                "note_type": "图文"
            }
        )
        
        if not notes:
            print(f"未找到关于 '{keyword}' 的笔记")
            return
            
        # 打印收集结果
        print("\n收集完成!")
        print(f"共收集到 {len(notes)} 条笔记:")
        for note in notes:
            print(note)

        # 保存笔记到数据库
        save_notes_to_db(notes)
        
        # 提取笔记URL列表并存入XCom
        note_urls = [note.get('note_url', '') for note in notes]
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
