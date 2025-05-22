from datetime import datetime
import time
import random

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable
from airflow.hooks.base import BaseHook

from utils.xhs_appium import XHSOperator

def get_reply_templates_from_db(**context):
    """从数据库获取回复模板
    Returns:
        回复模板内容列表
    """
    # 使用get_hook函数获取数据库连接
    db_hook = BaseHook.get_connection("xhs_db").get_hook()
    db_conn = db_hook.get_conn()
    cursor = db_conn.cursor()

    # 查询回复模板
    cursor.execute("SELECT content FROM reply_template")
    templates = [row[0] for row in cursor.fetchall()]

    cursor.close()
    db_conn.close()

    if not templates:
        print("警告：未找到回复模板，请确保reply_template表中有数据")
        # 返回一个默认模板，避免程序崩溃
        return ["谢谢您的评论，我们会继续努力！"]
    
    print(f"成功获取 {len(templates)} 条回复模板")
    return templates

def get_reply_contents_from_db(comment_ids: list, max_comments: int = 10):
    """从xhs_comments表获取需要回复的评论
    Args:
        comment_ids: 评论ID列表，对应xhs_comments表中的id字段
        max_comments: 本次要回复的评论数量
    Returns:
        包含note_url的字典列表，其中comment_id对应xhs_comments表的id
    """
    # 使用get_hook函数获取数据库连接
    db_hook = BaseHook.get_connection("xhs_db").get_hook()
    db_conn = db_hook.get_conn()
    cursor = db_conn.cursor()
    
    # 确保comment_manual_reply表存在（用于后续插入回复记录）
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS comment_manual_reply (
        id INT AUTO_INCREMENT PRIMARY KEY,
        comment_id INT NOT NULL,
        note_url VARCHAR(512),
        author VARCHAR(255),
        content TEXT,
        reply TEXT NOT NULL,
        replied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY unique_comment (comment_id)
    )
    """)
    db_conn.commit()

    # 从xhs_comments表中获取评论记录
    # 如果ids列表不为空，添加ID限制条件
    if comment_ids:
        placeholders = ','.join(['%s'] * len(comment_ids))
        cursor.execute(
            f"SELECT id, note_url, author, content FROM xhs_comments WHERE id IN ({placeholders})",
            comment_ids
        )
    else:
        cursor.execute(
            "SELECT id, note_url, author, content FROM xhs_comments LIMIT %s",
            (max_comments,)
        )

    results = [{'comment_id': row[0], 'note_url': row[1], 'author': row[2], 'content': row[3]} for row in cursor.fetchall()]

    cursor.close()
    db_conn.close()

    return results

def insert_manual_reply(comment_id: int, note_url: str, author: str, content: str, reply: str):
    """将回复记录插入到comment_manual_reply表
    Args:
        comment_id: 评论ID
        note_url: 笔记URL
        author: 评论作者
        content: 评论内容
        reply: 回复内容
    """
    try:
        # 使用get_hook函数获取数据库连接
        db_hook = BaseHook.get_connection("xhs_db").get_hook()
        db_conn = db_hook.get_conn()
        cursor = db_conn.cursor()
        
        # 确保comment_manual_reply表存在
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS comment_manual_reply (
            id INT AUTO_INCREMENT PRIMARY KEY,
            comment_id INT NOT NULL,
            note_url VARCHAR(512),
            author VARCHAR(255),
            content TEXT,
            reply TEXT NOT NULL,
            replied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_comment (comment_id)
        )
        """)
        db_conn.commit()
        
        # 插入回复记录
        cursor.execute(
            """
            INSERT INTO comment_manual_reply 
            (comment_id, note_url, author, content, reply) 
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            note_url = VALUES(note_url),
            author = VALUES(author),
            content = VALUES(content),
            reply = VALUES(reply),
            replied_at = CURRENT_TIMESTAMP
            """,
            (comment_id, note_url, author, content, reply)
        )
        
        # 提交事务
        db_conn.commit()
        print(f"成功插入评论 {comment_id} 的回复记录到comment_manual_reply表")
        
    except Exception as e:
        print(f"插入回复记录失败: {str(e)}")
        if 'db_conn' in locals():
            db_conn.rollback()
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'db_conn' in locals():
            db_conn.close()

# 移除了对comment_reply表的更新操作

def reply_with_template(**context):       
    """使用模板自动回复评论
    Args:
        comment_ids: 评论ID列表
        max_comments: 本次要回复的评论数量
        **context: Airflow上下文参数字典
    """
    # 从DAG运行配置中获取参数，如果没有则使用默认值
    comment_ids = context['dag_run'].conf.get('comment_ids') 
    max_comments = context['dag_run'].conf.get('max_comments') 
    
    # 获取回复模板
    reply_templates = get_reply_templates_from_db()
    
    # 获取评论内容
    initial_contents = get_reply_contents_from_db(comment_ids=comment_ids, max_comments=max_comments)
    
    # 获取Appium服务器URL
    appium_server_url = Variable.get("APPIUM_LOCAL_SERVER_URL", "http://localhost:4723")

    print(f"开始使用模板回复评论... 本次要回复的评论数量: {len(initial_contents)}")
    
    try:
        # 初始化小红书操作器
        xhs = XHSOperator(appium_server_url=appium_server_url, force_app_launch=True, device_id='63ebd8370906')
        
        # 遍历每条评论进行回复
        for content in initial_contents:
            try:
                note_url = content['note_url']
                author = content['author']
                comment_content = content['content']
                comment_id = content['comment_id']
                
                # 随机选择一条回复模板
                reply_content = random.choice(reply_templates)
                
                print(f"正在回复评论 - 作者: {author}, 内容: {comment_content}")
                print(f"选择的回复模板: {reply_content}")
                
                # 调用评论回复功能
                success = xhs.comments_reply(
                    note_url=note_url,
                    author=author,
                    comment_content=comment_content,
                    reply_content=reply_content
                )
                
                if success:
                    print(f"成功回复评论: {comment_content}")
                    # 插入到manual_reply表
                    insert_manual_reply(
                        comment_id=comment_id,
                        note_url=note_url,
                        author=author,
                        content=comment_content,
                        reply=reply_content
                    )
                else:
                    print(f"回复评论失败: {comment_content}")
                
                # 添加延时，避免操作过快
                time.sleep(2)
                
            except Exception as e:
                print(f"处理评论时出错: {str(e)}")
                continue
        
    except Exception as e:
        print(f"运行出错: {str(e)}")
        raise e
    finally:
        # 确保关闭小红书操作器
        if 'xhs' in locals():
            xhs.close()

# DAG 定义
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    dag_id='xhs_comments_template_replier',
    default_args=default_args,
    description='使用模板自动回复评论',
    schedule_interval=None,
    tags=['小红书'],
    catchup=False,
)

reply_comments_task = PythonOperator(
    task_id='reply_with_template',
    python_callable=reply_with_template,
    provide_context=True,
    dag=dag,
)

reply_comments_task