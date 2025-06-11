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
import base64
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
def btoa(string_to_encode):
    # 将字符串转换为字节
    bytes_to_encode = string_to_encode.encode('utf-8')
    # 进行Base64编码
    encoded_bytes = base64.b64encode(bytes_to_encode)
    # 转换回字符串
    return encoded_bytes.decode('utf-8')


def get_dag_info(**context):
    """获取DAG运行信息"""
    email = context['dag_run'].conf.get('email')
    
    time_range = get_time_range()
    headers = {
        'Authorization': f'Basic {btoa(f"claude89757:claude@airflow")}',
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "Connection": "keep-alive",
        "Referer": "https://marketing.lucyai.sale/airflow/api/v1/ui/",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36 Edg/137.0.0.0",
        "accept": "application/json",
        "sec-ch-ua": "\"Microsoft Edge\";v=\"137\", \"Chromium\";v=\"137\", \"Not/A)Brand\";v=\"24\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\""
    }
    url = f"https://marketing.lucyai.sale/airflow/api/v1/dags/notes_collector/dagRuns"
    params = {
        "limit": "100",
        "start_date_gte": time_range['twelve_hours_ago'],
        "start_date_lte": time_range['current_time']        
    }
    response = requests.get(url, headers=headers, params=params).json()

    for i in response['dag_runs']:
        print(i['conf']['email'], i['dag_run_id'], i['state'])
        

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
