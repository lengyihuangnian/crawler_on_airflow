#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.state import DagRunState
from airflow.operators.python import PythonOperator

# 定义默认参数
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 定义任务函数
def task_1(**kwargs):
    # 生成一些数据
    data = {"message": "这是来自任务1的数据", "value": 100}
    # 使用xcom_push存储数据
    kwargs['ti'].xcom_push(key='task_1_data', value=data)
    return "任务1完成"

def task_2(**kwargs):
    # 从task_1获取数据
    ti = kwargs['ti']
    task_1_data = ti.xcom_pull(task_ids='task_1', key='task_1_data')
    print(f"从任务1获取的数据: {task_1_data}")
    
    # 处理数据并存储新的结果
    task_1_data['value'] += 50
    task_1_data['processed_by'] = 'task_2'
    ti.xcom_push(key='task_2_data', value=task_1_data)
    return "任务2完成"

def task_3(**kwargs):
    # 从task_2获取数据
    ti = kwargs['ti']
    task_2_data = ti.xcom_pull(task_ids='task_2', key='task_2_data')
    print(f"从任务2获取的数据: {task_2_data}")
    
    # 处理数据并存储新的结果
    task_2_data['value'] *= 2
    task_2_data['processed_by'] = 'task_3'
    ti.xcom_push(key='task_3_data', value=task_2_data)
    return "任务3完成"

def task_4(**kwargs):
    # 同时从多个任务获取数据
    ti = kwargs['ti']
    task_1_data = ti.xcom_pull(task_ids='task_1', key='task_1_data')
    task_3_data = ti.xcom_pull(task_ids='task_3', key='task_3_data')
    
    # 合并数据
    combined_data = {
        "original_data": task_1_data,
        "processed_data": task_3_data,
        "processed_by": "task_4"
    }
    
    ti.xcom_push(key='combined_data', value=combined_data)
    return "任务4完成"

def task_5(**kwargs):
    # 获取合并后的数据
    ti = kwargs['ti']
    combined_data = ti.xcom_pull(task_ids='task_4', key='combined_data')
    
    # 生成最终报告
    final_report = f"""
    XCom 数据传递示例报告:
    
    原始数据: {combined_data['original_data']}
    处理后数据: {combined_data['processed_data']}
    最终处理者: task_5
    
    数据值变化:
    初始值: {combined_data['original_data']['value']}
    最终值: {combined_data['processed_data']['value']}
    """
    
    print(final_report)
    return "所有任务完成！"

# 创建DAG
with DAG(
    'xcom_example_dag',
    default_args=default_args,
    description='展示XCom用法的示例DAG',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['example', 'xcom'],
) as dag:

    # 定义任务
    t1 = PythonOperator(
        task_id='task_1',
        python_callable=task_1,
    )
    
    t2 = PythonOperator(
        task_id='task_2',
        python_callable=task_2,
    )
    
    t3 = PythonOperator(
        task_id='task_3',
        python_callable=task_3,
    )
    
    t4 = PythonOperator(
        task_id='task_4',
        python_callable=task_4,
    )
    
    t5 = PythonOperator(
        task_id='task_5',
        python_callable=task_5,
    )
    
    # 设置任务依赖关系
    t1 >> t2 >> t3 >> t4 >> t5


