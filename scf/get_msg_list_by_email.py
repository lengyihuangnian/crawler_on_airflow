# -*- coding: utf8 -*-
"""
获取小红书私信列表数据

Author: by cursor
Date: 2025-01-20
"""

import json
import os
import pymysql
import logging
from datetime import datetime

# 配置日志
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_db_connection():
    """
    获取数据库连接
    """
    try:
        # 从环境变量获取数据库连接信息
        db_name = os.environ.get('DB_NAME')
        db_ip = os.environ.get('DB_IP')
        db_port = int(os.environ.get('DB_PORT', 3306))
        db_user = os.environ.get('DB_USER')
        db_password = os.environ.get('DB_PASSWORD')
        
        # 创建数据库连接
        connection = pymysql.connect(
            host=db_ip,
            port=db_port,
            user=db_user,
            password=db_password,
            database=db_name,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        
        return connection
    except Exception as e:
        logger.error(f"数据库连接失败: {str(e)}")
        raise e


def get_msg_list_by_email(email):
    """
    根据email获取xhs_msg_list表中的所有数据
    
    Args:
        email: 用户邮箱
        
    Returns:
        list: 私信列表数据
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 查询指定email的所有私信数据
        query = "SELECT * FROM xhs_msg_list WHERE userInfo  = %s ORDER BY id DESC"
        cursor.execute(query, (email,))
            
        result = cursor.fetchall()
        
        # 关闭连接
        cursor.close()
        conn.close()
        
        # 处理datetime字段，转换为字符串格式
        if result:
            for row in result:
                for key, value in row.items():
                    if isinstance(value, datetime):
                        row[key] = value.strftime('%Y-%m-%d %H:%M:%S')
            return result
        return []
    except Exception as e:
        logger.error(f"获取私信列表失败: {str(e)}")
        return []


def main_handler(event, context):
    """
    云函数入口函数，获取指定email的小红书私信列表
    支持URL参数: email - 用户邮箱（必需）
    示例: ${baseUrl}?email=${encodeURIComponent(email)}
    
    Args:
        event: 触发事件
        context: 函数上下文
        
    Returns:
        JSON格式的私信列表数据
    """
    logger.info(f"收到请求: {json.dumps(event, ensure_ascii=False)}")
    
    try:
        # 从请求中获取email参数
        email = None
        if 'queryString' in event and 'email' in event['queryString']:
            email = event['queryString']['email']
        elif 'queryStringParameters' in event and event['queryStringParameters'] and 'email' in event['queryStringParameters']:
            email = event['queryStringParameters']['email']
        
        # email参数是必需的
        if not email:
            return {
                'code': 1,
                'message': 'email参数不能为空',
                'data': []
            }
        
        logger.info(f"获取email为 {email} 的私信列表")
        
        # 获取私信列表数据
        msg_list = get_msg_list_by_email(email)
        
        # 构建响应
        response = {
            'code': 0,
            'message': 'success',
            'data': msg_list,
            'count': len(msg_list)
        }
        
        return response
    except Exception as e:
        logger.error(f"处理请求失败: {str(e)}")
        return {
            'code': 1,
            'message': f'获取私信列表失败: {str(e)}',
            'data': []
        }


if __name__ == "__main__":
    # 本地测试用
    test_event = {
        'queryStringParameters': {
            'email': 'test@example.com'
        }
    }
    result = main_handler(test_event, {})
    print(json.dumps(result, ensure_ascii=False, indent=2))