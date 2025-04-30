#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.state import DagRunState


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
    schedule_interval='0 10 * * *',  # 每天上午10点执行
    catchup=False,
    tags=['小红书', 'workflow'],
)

# 定义传递给各个DAG的参数
workflow_params = {
    'keyword': '{{ dag_run.conf.get("keyword", "广州探店") }}',
    'max_notes': '{{ dag_run.conf.get("max_notes", 3) }}',
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
        'keyword': workflow_params['keyword'],
        'max_comments': workflow_params['max_comments'],
    },
    wait_for_completion=True,
    poke_interval=30,
    dag=dag,
)

# 设置任务依赖关系
trigger_notes_watcher >> trigger_comments_collector
