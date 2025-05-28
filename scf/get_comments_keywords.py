#!/usr/bin/env python3
# -*- coding: utf8 -*-
"""
获取小红书评论中的所有关键字

Author: by cursor
Date: 2025-05-27
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


def get_comments_keywords():
    """
    获取评论中的所有关键字
    
    Returns:
        list: 所有不重复的评论关键字列表
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 查询所有不重复的评论关键字
        query = "SELECT DISTINCT keyword FROM xhs_comments"
        cursor.execute(query)
        result = cursor.fetchall()
        
        # 关闭连接
        cursor.close()
        conn.close()
        
        # 返回关键字列表
        if result:
            return [row['keyword'] for row in result]
        return []
    except Exception as e:
        logger.error(f"获取评论关键字失败: {str(e)}")
        return []


def get_comments_count_by_keyword():
    """
    获取每个关键字对应的评论数量
    
    Returns:
        list: 包含关键字和对应评论数量的字典列表
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 查询每个关键字对应的评论数量
        query = """
        SELECT keyword, COUNT(*) as count 
        FROM xhs_comments 
        GROUP BY keyword 
        ORDER BY count DESC
        """
        cursor.execute(query)
        result = cursor.fetchall()
        
        # 关闭连接
        cursor.close()
        conn.close()
        
        # 返回结果
        return result
    except Exception as e:
        logger.error(f"获取评论关键字统计失败: {str(e)}")
        return []


def main_handler(event, context):
    """
    云函数入口函数，获取所有小红书评论关键字
    
    Args:
        event: 触发事件
        context: 函数上下文
        
    Returns:
        JSON格式的关键字列表或统计信息
    """
    logger.info(f"收到请求: {json.dumps(event, ensure_ascii=False)}")
    
    try:
        # 检查是否需要统计数量
        with_count = event.get('with_count', False)
        
        if with_count:
            # 获取带有数量统计的关键字列表
            keywords_data = get_comments_count_by_keyword()
        else:
            # 获取所有关键字
            keywords = get_comments_keywords()
            keywords_data = keywords
        
        # 构建响应
        response = {
            'code': 0,
            'message': 'success',
            'data': keywords_data
        }
        
        return response
    except Exception as e:
        logger.error(f"处理请求失败: {str(e)}")
        return {
            'code': 1,
            'message': f'获取评论关键字失败: {str(e)}',
            'data': []
        }


if __name__ == "__main__":
    # 本地测试用
    result = main_handler({'with_count': True}, {})
    print(json.dumps(result, ensure_ascii=False, indent=2))
