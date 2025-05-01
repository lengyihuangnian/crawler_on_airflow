#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.utils.state import DagRunState
from airflow.hooks.base import BaseHook


# DAG定义


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='xhs_workflow_manager',
    default_args=default_args,
    description='管理小红书笔记采集和评论收集工作流',
    # schedule_interval='0 10 * * *',  # 每天上午10点执行
    catchup=False,
    tags=['小红书', 'workflow'],
)

# 从笔记收集DAG获取笔记URL的函数
def get_collected_note_urls(ti):
    """从笔记收集DAG的XCOM中获取收集到的笔记URL列表
    Args:
        ti: 任务实例对象
    Returns:
        收集到的笔记URL列表
    """
    # 直接从xhs_notes_collector DAG的XCom中获取笔记URL
    note_urls = ti.xcom_pull(key='note_urls', task_ids='trigger_notes_watcher')
    
    if note_urls:
        print(f"从XCom中获取到{len(note_urls)}条笔记URL")
        return note_urls
    
    print("未从XCom中找到笔记URL，尝试从数据库获取")
    
    # 作为备选方案，从数据库中获取最近收集的笔记URL
    db_hook = BaseHook.get_connection("xhs_db").get_hook()
    db_conn = db_hook.get_conn()
    cursor = db_conn.cursor()
    
    try:
        # 获取最近添加的笔记URL
        keyword = ti.xcom_pull(key='keyword', task_ids='trigger_notes_watcher')
        if keyword:
            cursor.execute(
                "SELECT note_url FROM xhs_notes WHERE keyword = %s ORDER BY created_at DESC LIMIT %s", 
                (keyword, int(workflow_params['max_notes']))
            )
        else:
            cursor.execute(
                "SELECT note_url FROM xhs_notes ORDER BY created_at DESC LIMIT %s", 
                (int(workflow_params['max_notes']),)
            )
        
        note_urls = [row[0] for row in cursor.fetchall()]
        print(f"从数据库获取到{len(note_urls)}条笔记URL")
        return note_urls
    except Exception as e:
        print(f"获取笔记URL时出错: {str(e)}")
        return []
    finally:
        cursor.close()
        db_conn.close()

# 定义传递给各个DAG的参数
workflow_params = {
    'keyword': '{{ dag_run.conf.get("keyword", "广州探店") }}',
    'max_notes': '{{ dag_run.conf.get("max_notes", 1) }}',
    'max_comments': '{{ dag_run.conf.get("max_comments", 5) }}',
}

# 1. 触发笔记采集DAG
trigger_notes_watcher = TriggerDagRunOperator(
    task_id='trigger_notes_watcher',
    trigger_dag_id='xhs_notes_collector',
    conf={
        'keyword': workflow_params['keyword'],
        'max_notes': workflow_params['max_notes'],
    },
    wait_for_completion=True,  # 等待DAG完成
    poke_interval=30,  # 每30秒检查一次状态
    dag=dag,
)

# 2. 触发评论收集DAG
trigger_comments_collector = TriggerDagRunOperator(
    task_id='trigger_comments_collector',
    trigger_dag_id='xhs_comments_collector',
    conf={
        'keyword': "{{ task_instance.xcom_pull(key='keyword', task_ids='trigger_notes_watcher') }}",
        'max_comments': workflow_params['max_comments'],
        'note_urls': "{{ task_instance.xcom_pull(task_ids='get_note_urls') }}",
    },
    wait_for_completion=True,
    poke_interval=30,
    dag=dag,
)

# 获取笔记URL的任务
get_note_urls = PythonOperator(
    task_id='get_note_urls',
    python_callable=get_collected_note_urls,
    provide_context=True,
    dag=dag,
)

# 设置任务依赖关系
trigger_notes_watcher >> get_note_urls >> trigger_comments_collector
