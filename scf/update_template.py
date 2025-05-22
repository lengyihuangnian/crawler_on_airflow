#!/usr/bin/env python3
# -*- coding: utf8 -*-
"""
管理回复模板数据（增、删、改）

Author: by cursor
Date: 2025-05-22
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


def execute_update(query, params):
    """
    执行更新操作
    
    Args:
        query: SQL查询语句
        params: 查询参数
        
    Returns:
        int: 受影响的行数
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 执行更新操作
        affected_rows = cursor.execute(query, params)
        conn.commit()
        
        # 关闭连接
        cursor.close()
        conn.close()
        
        return affected_rows
    except Exception as e:
        logger.error(f"执行更新失败: {str(e)}")
        return 0


def insert_many(query, data):
    """
    批量插入数据
    
    Args:
        query: SQL查询语句
        data: 数据列表
        
    Returns:
        int: 受影响的行数
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 执行批量插入
        affected_rows = cursor.executemany(query, data)
        conn.commit()
        
        # 关闭连接
        cursor.close()
        conn.close()
        
        return affected_rows
    except Exception as e:
        logger.error(f"批量插入失败: {str(e)}")
        return 0


def add_reply_template(content, user_id="zacks"):
    """
    添加回复模板
    
    Args:
        content: 模板内容
        user_id: 用户ID，默认为zacks
        
    Returns:
        int: 受影响的行数
    """
    query = "INSERT INTO reply_template (user_id, content) VALUES (%s, %s)"
    params = (user_id, content)
    return execute_update(query, params)


def add_reply_templates(templates, user_id="zacks"):
    """
    批量添加回复模板
    
    Args:
        templates: 模板内容列表
        user_id: 用户ID，默认为zacks
        
    Returns:
        int: 受影响的行数
    """
    if not templates:
        return 0
    query = "INSERT INTO reply_template (user_id, content) VALUES (%s, %s)"
    data = [(user_id, template) for template in templates]
    return insert_many(query, data)


def delete_reply_template(template_id, user_id="zacks"):
    """
    删除指定ID的回复模板
    
    Args:
        template_id: 模板ID
        user_id: 用户ID，默认为zacks
        
    Returns:
        int: 受影响的行数
    """
    query = "DELETE FROM reply_template WHERE id = %s AND user_id = %s"
    params = (template_id, user_id)
    return execute_update(query, params)


def delete_all_reply_templates(user_id="zacks"):
    """
    删除用户的所有回复模板
    
    Args:
        user_id: 用户ID，默认为zacks
        
    Returns:
        int: 受影响的行数
    """
    query = "DELETE FROM reply_template WHERE user_id = %s"
    params = (user_id,)
    return execute_update(query, params)


def update_reply_template(template_id, content, user_id="zacks"):
    """
    更新指定ID的回复模板内容
    
    Args:
        template_id: 模板ID
        content: 新的模板内容
        user_id: 用户ID，默认为zacks
        
    Returns:
        int: 受影响的行数
    """
    query = "UPDATE reply_template SET content = %s WHERE id = %s AND user_id = %s"
    params = (content, template_id, user_id)
    return execute_update(query, params)


def main_handler(event, context):
    """
    云函数入口函数，管理回复模板数据
    
    Args:
        event: 触发事件，包含操作参数
        context: 函数上下文
        
    Returns:
        JSON格式的操作结果
    """
    logger.info(f"收到请求: {json.dumps(event, ensure_ascii=False)}")
    
    # 解析参数 - 只使用POST请求的body
    params = {}
    if 'body' in event:
        try:
            # 尝试解析body为JSON
            if isinstance(event['body'], str):
                params = json.loads(event['body'])
            else:
                params = event['body']
        except Exception as e:
            logger.error(f"解析请求体失败: {str(e)}")
            return {
                "code": 1,
                "message": f"解析请求体失败: {str(e)}",
                "data": None
            }
    
    # 获取操作类型
    action = params.get('action', '')
    user_id = params.get('user_id', 'zacks')
    
    try:
        # 根据操作类型执行相应操作
        if action == 'add':
            # 添加单个模板
            content = params.get('content', '')
            if not content:
                return {
                    "code": 1,
                    "message": "模板内容不能为空",
                    "data": None
                }
            
            affected_rows = add_reply_template(content, user_id)
            return {
                "code": 0 if affected_rows > 0 else 1,
                "message": "success" if affected_rows > 0 else "添加失败",
                "data": {
                    "affected_rows": affected_rows
                }
            }
            
        elif action == 'add_batch':
            # 批量添加模板
            templates = params.get('templates', [])
            if not templates:
                return {
                    "code": 1,
                    "message": "模板列表不能为空",
                    "data": None
                }
            
            affected_rows = add_reply_templates(templates, user_id)
            return {
                "code": 0 if affected_rows > 0 else 1,
                "message": "success" if affected_rows > 0 else "批量添加失败",
                "data": {
                    "affected_rows": affected_rows
                }
            }
            
        elif action == 'delete':
            # 删除指定ID的模板
            template_id = params.get('template_id')
            if not template_id:
                return {
                    "code": 1,
                    "message": "模板ID不能为空",
                    "data": None
                }
            
            affected_rows = delete_reply_template(template_id, user_id)
            return {
                "code": 0 if affected_rows > 0 else 1,
                "message": "success" if affected_rows > 0 else "删除失败",
                "data": {
                    "affected_rows": affected_rows
                }
            }
            
        elif action == 'delete_all':
            # 删除用户的所有模板
            affected_rows = delete_all_reply_templates(user_id)
            return {
                "code": 0,
                "message": "success",
                "data": {
                    "affected_rows": affected_rows
                }
            }
            
        elif action == 'update':
            # 更新模板内容
            template_id = params.get('template_id')
            content = params.get('content', '')
            
            if not template_id:
                return {
                    "code": 1,
                    "message": "模板ID不能为空",
                    "data": None
                }
                
            if not content:
                return {
                    "code": 1,
                    "message": "模板内容不能为空",
                    "data": None
                }
            
            affected_rows = update_reply_template(template_id, content, user_id)
            return {
                "code": 0 if affected_rows > 0 else 1,
                "message": "success" if affected_rows > 0 else "更新失败",
                "data": {
                    "affected_rows": affected_rows
                }
            }
        else:
            # 未知操作
            return {
                "code": 1,
                "message": f"未知操作类型: {action}",
                "data": None
            }
            
    except Exception as e:
        logger.error(f"操作失败: {str(e)}")
        return {
            "code": 1,
            "message": f"操作失败: {str(e)}",
            "data": None
        }


if __name__ == "__main__":
    # 本地测试用
    test_event = {
        'body': json.dumps({
            'action': 'add',
            'user_id': 'zacks',
            'content': '这是一个测试模板'
        })
    }
    result = main_handler(test_event, {})
    print(json.dumps(result, ensure_ascii=False, indent=2))