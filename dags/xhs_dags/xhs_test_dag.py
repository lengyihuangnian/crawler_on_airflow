#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time
from datetime import datetime, timedelta
import re 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowSkipException
from appium.webdriver.common.appiumby import AppiumBy
import requests
from utils.xhs_appium import XHSOperator
def get_time_range():
    from datetime import datetime, timedelta
    
    current_time = datetime.utcnow()
    twelve_hours_ago = current_time - timedelta(hours=12)
    
    current_time_iso = current_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    twelve_hours_ago_iso = twelve_hours_ago.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    
    return {
        'current_time': current_time_iso,
        'twelve_hours_ago': twelve_hours_ago_iso
    }

def get_dag_info():
    """获取DAG运行信息"""
    time_range = get_time_range()
    headers = {
        "accept": "application/json"
    }
    url = "https://marketing.lucyai.sale/airflow/api/v1/dags/comments_collector/dagRuns"
    params = {
        "limit": "100",
        "start_date_gte": time_range['twelve_hours_ago'],
        "start_date_lte": time_range['current_time']        
    }
    response = requests.get(url, headers=headers, params=params)

    print(response.text)
    print(response)

with DAG(
    dag_id='test_dag',
    default_args={'owner': 'yuchangongzhu', 'depends_on_past': False, 'start_date': datetime(2024, 1, 1)},
    description='测试',
    schedule_interval=None,
    tags=['测试'],
    catchup=False,
    max_active_runs=5,
) as dag:

    for index in range(10):
        PythonOperator(
            task_id=f'test_dag{index}',
            python_callable=get_dag_info,
            op_kwargs={
                'device_index': index,
            },
            provide_context=True,
        )
