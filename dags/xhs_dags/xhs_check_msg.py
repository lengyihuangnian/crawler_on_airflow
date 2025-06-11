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

from utils.xhs_appium import XHSOperator


def xhs_msg_check(device_index,**context):
    email = context['dag_run'].conf.get('email')
    
    # 获取设备列表
    device_info_list = Variable.get("XHS_DEVICE_INFO_LIST", default_var=[], deserialize_json=True)
    device_info = next((device for device in device_info_list if device.get('email') == email), None)
    if device_info:
        print(f"device_info: {device_info}")
    else:
        raise ValueError("email参数不能为空")
     # 获取设备信息
    try:
        device_ip = device_info.get('device_ip')
        appium_port = device_info.get('available_appium_ports')[device_index]
        device_id = device_info.get('phone_device_list')[device_index]
    except Exception as e:
        print(f"获取设备信息失败: {e}")
        print(f"跳过当前任务，因为获取设备信息失败")
        raise AirflowSkipException("设备信息获取失败")
    appium_server_url = f"http://{device_ip}:{appium_port}"
    
    print(f"选择设备 {device_id}, appium_server_url: {appium_server_url}")
    print(f"开始回复私信'")
    xhs = XHSOperator(appium_server_url=appium_server_url, force_app_launch=True, device_id=device_id)
    xhs.check_unreplied_messages(device_id)

with DAG(
    dag_id='xhs_msg_check',
    default_args={'owner': 'yuchangongzhu', 'depends_on_past': False, 'start_date': datetime(2024, 1, 1)},
    description='小红书私信检查任务',
    schedule_interval=None,
    tags=['小红书'],
    catchup=False,
    max_active_runs=5,
) as dag:

    for index in range(10):
        PythonOperator(
            task_id=f'xhs_msg_check{index}',
            python_callable=xhs_msg_check,
            op_kwargs={
                'device_index': index,
            },
            provide_context=True
        )
