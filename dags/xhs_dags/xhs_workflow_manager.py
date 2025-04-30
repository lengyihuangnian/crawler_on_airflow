#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.hooks.base import BaseHook
from airflow.models import DagRun
from airflow.utils.state import DagRunState


def get_latest_comments_ids(**context):
    """
    从数据库获取最近收集的评论ID，用于传递给评论回复DAG
    Returns:
        最新评论的ID列表
    """
    # 使用get_hook函数获取数据库连接
    db_hook = BaseHook.get_connection("xhs_db").get_hook()
    db_conn = db_hook.get_conn()
    cursor = db_conn.cursor()

    try:
        # 获取最新添加的评论ID（例如，最近10条）
        cursor.execute("""
        SELECT id FROM xhs_comments 
        ORDER BY created_at DESC 
        LIMIT 10
        """)
        comment_ids = [row[0] for row in cursor.fetchall()]
        
        if not comment_ids:
            print("警告：未找到最近的评论记录")
            return []
            
        print(f"成功获取 {len(comment_ids)} 条最新评论ID")
        
        # 将评论ID列表传递给下一个任务
        context['ti'].xcom_push(key='latest_comment_ids', value=comment_ids)
        return comment_ids
        
    except Exception as e:
        print(f"获取最新评论ID失败: {str(e)}")
        raise
    finally:
        cursor.close()
        db_conn.close()


def check_dag_status(dag_id, run_id, **context):
    """
    检查特定DAG run的状态
    Args:
        dag_id: 要检查的DAG ID
        run_id: 要检查的DAG运行ID
    Returns:
        True如果成功，False如果失败
    """
    from airflow.models import DagRun
    from airflow.utils.state import DagRunState
    
    dag_run = DagRun.find(dag_id=dag_id, run_id=run_id)
    
    if not dag_run or len(dag_run) == 0:
        print(f"找不到DAG运行记录: {dag_id} / {run_id}")
        return False
        
    state = dag_run[0].state
    print(f"DAG {dag_id} / {run_id} 的状态为: {state}")
    
    return state == DagRunState.SUCCESS


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
    description='管理小红书笔记采集-评论收集-评论回复工作流',
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

# 获取最新评论的ID
get_comment_ids = PythonOperator(
    task_id='get_comment_ids',
    python_callable=get_latest_comments_ids,
    provide_context=True,
    dag=dag,
)

# 3. 触发评论回复DAG
trigger_comments_replier = TriggerDagRunOperator(
    task_id='trigger_comments_replier',
    trigger_dag_id='xhs_comments_template_replier',
    conf={
        'comment_ids': '{{ ti.xcom_pull(task_ids="get_comment_ids") }}',
        'max_comments': workflow_params['max_comments'],
    },
    wait_for_completion=True,
    poke_interval=30,
    dag=dag,
)

# 设置任务依赖关系
trigger_notes_watcher >> trigger_comments_collector >> get_comment_ids >> trigger_comments_replier
