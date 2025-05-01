#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
import time

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.models.dagrun import DagRun
from airflow.utils.dates import days_ago


def trigger_and_wait_for_notes_collection(**context):
    """
    触发笔记收集DAG并等待其完成，使用特定的run_id进行跟踪
    """
    # 获取任务实例和参数
    ti = context['ti']
    dag_run = context['dag_run']
    
    # 从DAG运行配置中获取关键词和最大笔记数量
    keyword = dag_run.conf.get('keyword', '猫咖') if dag_run.conf else '猫咖'
    max_notes = dag_run.conf.get('max_notes', 1) if dag_run.conf else 1
    
    # 使用时间戳生成唯一的run_id
    timestamp = int(time.time())
    dag_run_id = f'xhs_notes_{timestamp}'
    
    print(f"[HANDLE] 触发小红书笔记收集DAG，关键词: {keyword}, 最大笔记数: {max_notes}")
    
    # 导入trigger_dag函数
    from airflow.api.common.trigger_dag import trigger_dag
    
    # 触发DAG
    trigger_dag(
        dag_id='xhs_notes_collector',
        conf={
            'keyword': keyword,
            'max_notes': max_notes
        },
        run_id=dag_run_id,
    )
    
    # 循环等待DAG运行完成
    max_wait_time = 300  # 最大等待时间（秒）
    start_time = time.time()
    
    while (time.time() - start_time) < max_wait_time:
        # 使用确定的run_id查找DAG运行
        dag_run_list = DagRun.find(dag_id="xhs_notes_collector", run_id=dag_run_id)
        
        if not dag_run_list:
            print(f"[HANDLE] 等待DAG运行开始，已等待: {int(time.time() - start_time)}秒")
            time.sleep(5)
            continue
        
        dag_run_status = dag_run_list[0].state
        print(f"[HANDLE] 等待DAG运行完成，当前状态: {dag_run_status}，已等待: {int(time.time() - start_time)}秒")
        
        if dag_run_status == 'success':
            # 直接从XCom表中查询特定run_id的数据
            from airflow.models import XCom
            from airflow.utils.session import create_session
            
            print(f"尝试直接从XCom表获取run_id: {dag_run_id}的数据")
            
            # 使用create_session创建会话查询note_urls
            with create_session() as session:
                xcom_results = session.query(XCom).filter(
                    XCom.dag_id == 'xhs_notes_collector',
                    XCom.task_id == 'collect_xhs_notes',
                    XCom.key == 'note_urls',
                    XCom.run_id == dag_run_id
                ).first()
            
            # 使用create_session创建会话查询keyword
            with create_session() as session:
                keyword_xcom = session.query(XCom).filter(
                    XCom.dag_id == 'xhs_notes_collector',
                    XCom.task_id == 'collect_xhs_notes',
                    XCom.key == 'keyword',
                    XCom.run_id == dag_run_id
                ).first()
            
            note_urls = None
            keyword = None
            
            if xcom_results:
                note_urls = xcom_results.value
                print(f"通过直接查询XCom表获取到笔记URL: {len(note_urls)}个")
                # 将数据存入当前任务的XCom
                ti.xcom_push(key='note_urls', value=note_urls)
                
                if keyword_xcom:
                    keyword = keyword_xcom.value
                    ti.xcom_push(key='keyword', value=keyword)
                else:
                    # 如果找不到关键词，使用传入的关键词
                    ti.xcom_push(key='keyword', value=keyword)
                
                return True
                
            else:
                print("无法通过任何方式获取笔记URL")
                return False
        elif dag_run_status == 'failed':
            print(f"外部DAG运行失败: {dag_run_id}")
            return False
        
        time.sleep(5)
    
    print("等待超时，DAG运行未完成")
    return False


def get_notes_urls_and_trigger_comments(**context):
    """
    从XCom获取笔记URL列表和关键词，然后触发评论收集DAG
    """
    # 获取任务实例
    ti = context['ti']
    
    # 从XCom获取笔记URL列表和关键词
    note_urls = ti.xcom_pull(task_ids='trigger_and_wait_for_notes', key='note_urls')
    keyword = ti.xcom_pull(task_ids='trigger_and_wait_for_notes', key='keyword')
    
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

# 触发笔记收集DAG并等待其完成
trigger_and_wait_task = PythonOperator(
    task_id='trigger_and_wait_for_notes',
    python_callable=trigger_and_wait_for_notes_collection,
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
trigger_and_wait_task >> get_data >> trigger_comments_collection