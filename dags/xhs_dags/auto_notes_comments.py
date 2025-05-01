#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
import time

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.models.dagrun import DagRun
from airflow.utils.dates import days_ago


def wait_for_dag_completion(**context):
    """
    等待笔记收集DAG完成并获取XCom数据
    """
    # 获取任务实例
    ti = context['ti']
    dag_run = context['dag_run']
    
    # 构建被触发DAG的run_id
    # 格式通常为：triggered__<触发时间>__<触发DAG的run_id>
    current_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    triggered_dag_run_id = f"triggered__{current_time}__{dag_run.run_id}"
    
    print(f"等待DAG运行完成，预计DAG运行ID: {triggered_dag_run_id}")
    
    # 循环检查DAG运行状态
    max_wait_time = 300  # 最大等待时间（秒）
    start_time = time.time()
    
    while (time.time() - start_time) < max_wait_time:
        # 获取最近的DAG运行
        recent_dag_runs = DagRun.find(dag_id="xhs_notes_collector", 
                                    state=None,  # 获取所有状态
                                    order_by=DagRun.execution_date.desc(),
                                    limit=5)  # 获取最近的几个运行
        
        print(f"最近的DAG运行: {[run.run_id for run in recent_dag_runs]}")
        
        # 检查每个运行
        for dag_run in recent_dag_runs:
            # 检查是否是由当前DAG触发的
            if dag_run.external_trigger and dag_run.run_id.startswith('manual_'):
                print(f"找到可能的匹配DAG运行: {dag_run.run_id}, 状态: {dag_run.state}")
                
                if dag_run.state == 'success':
                    # 从外部DAG获取XCom数据
                    note_urls = ti.xcom_pull(dag_id='xhs_notes_collector', task_ids='collect_xhs_notes', key='note_urls')
                    keyword = ti.xcom_pull(dag_id='xhs_notes_collector', task_ids='collect_xhs_notes', key='keyword')
                    
                    if note_urls:
                        print(f"成功获取到笔记URL: {len(note_urls)}个")
                        # 将数据存入当前任务的XCom
                        ti.xcom_push(key='note_urls', value=note_urls)
                        ti.xcom_push(key='keyword', value=keyword)
                        return True
                elif dag_run.state == 'failed':
                    print(f"外部DAG运行失败: {dag_run.run_id}")
                    return False
        
        print(f"[HANDLE] 等待DAG运行完成，已等待: {int(time.time() - start_time)}秒")
        time.sleep(5)
    
    print("等待超时，未找到成功完成的DAG运行")
    return False


def get_notes_urls_and_trigger_comments(**context):
    """
    从XCom获取笔记URL列表和关键词，然后触发评论收集DAG
    """
    # 获取任务实例
    ti = context['ti']
    
    # 从XCom获取笔记URL列表和关键词
    note_urls = ti.xcom_pull(task_ids='wait_for_dag_completion', key='note_urls')
    keyword = ti.xcom_pull(task_ids='wait_for_dag_completion', key='keyword')
    
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
    wait_for_completion=False,  # 不在这里等待完成，我们将使用自定义的等待方法
    dag=dag,
)

# 等待笔记收集DAG完成并获取XCom数据
wait_for_dag_completion = PythonOperator(
    task_id='wait_for_dag_completion',
    python_callable=wait_for_dag_completion,
    provide_context=True,
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
trigger_notes_collection >> wait_for_dag_completion >> get_data >> trigger_comments_collection