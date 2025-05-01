#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago


def get_notes_urls_and_trigger_comments(**context):
    """
    从XCom获取笔记URL列表和关键词，然后触发评论收集DAG
    """
    # 获取任务实例
    ti = context['ti']
    
    # 从XCom获取笔记URL列表和关键词
    note_urls = ti.xcom_pull(task_ids='wait_for_notes_collection', key='note_urls')
    keyword = ti.xcom_pull(task_ids='wait_for_notes_collection', key='keyword')
    
    if not note_urls:
        print("未找到笔记URL列表，无法触发评论收集")
        return
    
    print(f"获取到{len(note_urls)}个笔记URL，关键词：{keyword}")
    
    # 准备传递给评论收集DAG的配置
    conf = {
        'note_urls': note_urls,
        'keyword': keyword
    }
    
    # 返回配置，将由TriggerDagRunOperator使用
    return conf


# DAG 定义
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='auto_notes_comments',
    default_args=default_args,
    description='自动触发小红书笔记收集和评论收集',
    schedule_interval=None,
    tags=['小红书', '自动化'],
    catchup=False,
)

# 触发笔记收集DAG
trigger_notes_collection = TriggerDagRunOperator(
    task_id='trigger_notes_collection',
    trigger_dag_id='xhs_notes_collector',
    conf={
        'keyword': '{{ dag_run.conf["keyword"] if dag_run.conf and "keyword" in dag_run.conf else "猫咖" }}',
        'max_notes': '{{ dag_run.conf["max_notes"] if dag_run.conf and "max_notes" in dag_run.conf else 1 }}'
    },
    wait_for_completion=True,
    dag=dag,
)

# 等待笔记收集DAG完成并获取XCom数据
wait_for_notes_collection = ExternalTaskSensor(
    task_id='wait_for_notes_collection',
    external_dag_id='xhs_notes_collector',
    external_task_id='collect_xhs_notes',
    allowed_states=['success'],
    execution_delta=None,  # 同一执行日期
    execution_date_fn=lambda dt: dt,  # 使用相同的执行日期
    timeout=600,  # 10分钟超时
    mode='reschedule',  # 重新调度模式
    poke_interval=30,  # 每30秒检查一次
    dag=dag,
)

# 获取笔记URL列表和关键词
get_data = PythonOperator(
    task_id='get_notes_urls_and_keyword',
    python_callable=get_notes_urls_and_trigger_comments,
    provide_context=True,
    dag=dag,
)

# 触发评论收集DAG
trigger_comments_collection = TriggerDagRunOperator(
    task_id='trigger_comments_collection',
    trigger_dag_id='xhs_comments_collector',
    conf=lambda context: context['ti'].xcom_pull(task_ids='get_notes_urls_and_keyword'),
    wait_for_completion=True,
    dag=dag,
)

# 设置任务依赖关系
trigger_notes_collection >> wait_for_notes_collection >> get_data >> trigger_comments_collection