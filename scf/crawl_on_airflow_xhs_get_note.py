#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import requests
import os
from datetime import datetime
import base64
import hmac
import hashlib
import urllib.parse
import time

# 配置信息
AIRFLOW_HOST = os.environ.get('AIRFLOW_HOST')
AIRFLOW_USERNAME = os.environ.get('AIRFLOW_USERNAME')
AIRFLOW_PASSWORD = os.environ.get('AIRFLOW_PASSWORD')
DAG_ID = 'xhs_notes_collector'
TASK_ID = 'collect_xhs_notes'

# 腾讯云API配置
SECRET_ID = os.environ.get('TENCENT_SECRET_ID', '')
SECRET_KEY = os.environ.get('TENCENT_SECRET_KEY', '')

def trigger_dag_run(keyword=None, max_notes=None):
    """
    触发DAG运行
    
    Args:
        keyword: 搜索关键词
        max_notes: 最大笔记数量
    
    Returns:
        DAG运行响应数据
    """
    # 使用基本认证
    headers = {
        "Content-Type": "application/json"
    }
    auth = (AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
    
    # 准备DAG运行配置
    conf = {}
    if keyword:
        conf['keyword'] = keyword
    if max_notes:
        conf['max_notes'] = max_notes
    
    dag_run_url = f"{AIRFLOW_HOST}/api/v1/dags/{DAG_ID}/dagRuns"
    dag_run_data = {
        "conf": conf,
        "note": f"Triggered by Tencent Cloud Function at {datetime.now().isoformat()}"
    }
    
    try:
        # 使用基本认证发送请求
        response = requests.post(dag_run_url, headers=headers, json=dag_run_data, auth=auth)
        response.raise_for_status()
        # 返回完整的响应数据，而不仅仅是dag_run_id
        return response.json()
    except Exception as e:
        print(f"触发DAG运行失败: {str(e)}")
        raise


def check_dag_run_status(dag_run_id):
    """
    检查DAG运行状态
    
    Args:
        dag_run_id: DAG运行ID
    
    Returns:
        DAG运行状态
    """
    # 使用基本认证
    headers = {
        "Content-Type": "application/json"
    }
    auth = (AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
    
    status_url = f"{AIRFLOW_HOST}/api/v1/dags/{DAG_ID}/dagRuns/{dag_run_id}"
    
    try:
        response = requests.get(status_url, headers=headers, auth=auth)
        response.raise_for_status()
        return response.json()['state']
    except Exception as e:
        print(f"检查DAG运行状态失败: {str(e)}")
        raise


def get_task_logs(dag_run_id):
    """
    获取任务日志
    
    Args:
        dag_run_id: DAG运行ID
    
    Returns:
        任务日志内容
    """
    # 使用基本认证
    headers = {
        "Content-Type": "application/json"
    }
    auth = (AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
    
    logs_url = f"{AIRFLOW_HOST}/api/v1/dags/{DAG_ID}/dagRuns/{dag_run_id}/taskInstances/{TASK_ID}/logs"
    
    try:
        response = requests.get(logs_url, headers=headers, auth=auth)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"获取任务日志失败: {str(e)}")
        raise


def sign_tencent_request(params, method='GET'):
    """
    腾讯云API请求签名
    
    Args:
        params: 请求参数
        method: 请求方法
    
    Returns:
        签名后的请求参数
    """
    timestamp = int(time.time())
    date = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d')
    
    # 1. 拼接规范请求串
    canonical_uri = '/'
    canonical_querystring = ''
    
    # 按字典序排序参数
    sorted_params = sorted(params.items(), key=lambda x: x[0])
    canonical_headers = 'content-type:application/json\nhost:scf.tencentcloudapi.com\n'
    signed_headers = 'content-type;host'
    
    # 拼接规范请求串
    payload = ''
    if method == 'POST':
        payload = json.dumps(params)
    else:
        canonical_querystring = '&'.join(['%s=%s' % (k, urllib.parse.quote(str(v), safe='')) for k, v in sorted_params])
    
    hashed_request_payload = hashlib.sha256(payload.encode('utf-8')).hexdigest()
    canonical_request = f"{method}\n{canonical_uri}\n{canonical_querystring}\n{canonical_headers}\n{signed_headers}\n{hashed_request_payload}"
    
    # 2. 拼接待签名字符串
    algorithm = 'TC3-HMAC-SHA256'
    credential_scope = f"{date}/scf/tc3_request"
    hashed_canonical_request = hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()
    string_to_sign = f"{algorithm}\n{timestamp}\n{credential_scope}\n{hashed_canonical_request}"
    
    # 3. 计算签名
    def sign(key, msg):
        return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()
    
    secret_date = sign(('TC3' + SECRET_KEY).encode('utf-8'), date)
    secret_service = sign(secret_date, 'scf')
    secret_signing = sign(secret_service, 'tc3_request')
    signature = hmac.new(secret_signing, string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()
    
    # 4. 拼接 Authorization
    authorization = (f"{algorithm} Credential={SECRET_ID}/{credential_scope}, "
                    f"SignedHeaders={signed_headers}, Signature={signature}")
    
    return {
        'Authorization': authorization,
        'Content-Type': 'application/json',
        'Host': 'scf.tencentcloudapi.com',
        'X-TC-Action': params.get('Action', ''),
        'X-TC-Version': params.get('Version', ''),
        'X-TC-Timestamp': str(timestamp),
        'X-TC-Region': params.get('Region', '')
    }


def main_handler(event, context):
    """
    腾讯云函数入口
    
    Args:
        event: 事件数据
        context: 函数上下文
    
    Returns:
        函数执行结果
    """
    print("开始执行小红书笔记收集云函数...")
    
    try:
        # 解析事件参数
        query_params = {}
        if 'queryString' in event:
            query_params = event['queryString']
        elif 'body' in event:
            try:
                # 尝试解析body为JSON
                if isinstance(event['body'], str):
                    query_params = json.loads(event['body'])
                else:
                    query_params = event['body']
            except:
                pass
        
        # 获取关键词和最大笔记数
        keyword = query_params.get('keyword')
        max_notes = query_params.get('max_notes')
        
        if not keyword:
            keyword = '旅游'  # 默认关键词
        
        if max_notes:
            try:
                max_notes = int(max_notes)
            except:
                max_notes = 1  # 默认最大笔记数
        
        # 触发DAG运行
        print(f"触发DAG运行，关键词: {keyword}, 最大笔记数: {max_notes}")
        dag_run_response = trigger_dag_run(keyword=keyword, max_notes=max_notes)
        print(f"DAG运行响应: {dag_run_response}")
        
        # 直接返回API响应结果
        return {
            'statusCode': 200,
            'body': json.dumps(dag_run_response)
        }
        
    except Exception as e:
        error_msg = f"执行小红书笔记收集云函数失败: {str(e)}"
        print(error_msg)
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': error_msg
            })
        }


# 本地测试用
if __name__ == "__main__":
    # 模拟云函数事件
    test_event = {
        'queryString': {
            'keyword': '旅游',
            'max_notes': '3'
        }
    }
    
    # 模拟云函数上下文
    test_context = {}
    
    # 执行云函数
    result = main_handler(test_event, test_context)
    print(result)