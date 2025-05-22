from datetime import datetime
import time
import random

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowSkipException

from utils.xhs_appium import XHSOperator

def get_reply_templates_from_db():
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

def reply_with_template(device_index: int = 0, **context):       
    """使用模板自动回复评论
    Args:
        device_index: 设备索引
        **context: Airflow上下文参数字典
    """
    print(f"dag_run_conf: {context['dag_run'].conf}")
    
    # 从DAG运行配置中获取参数，如果没有则使用默认值
    comment_ids = context['dag_run'].conf.get('comment_ids') 
    max_comments = context['dag_run'].conf.get('max_comments') 
    email = context['dag_run'].conf.get('email')
    
    if not email:
        raise ValueError("email参数不能为空")
    
    # 获取回复模板
    reply_templates = get_reply_templates_from_db()
    
    # 获取评论内容
    initial_contents = get_reply_contents_from_db(comment_ids=comment_ids, max_comments=max_comments)
    
    # 分配评论 - 根据设备索引分割评论列表
    # 确保对应设备索引的任务只处理相应分配的评论
    total_tasks = 10  # 与下面创建的任务数量保持一致
    comments_to_process = []
    
    for i, comment in enumerate(initial_contents):
        if i % total_tasks == device_index:
            comments_to_process.append(comment)
    
    if not comments_to_process:
        print(f"设备索引 {device_index}: 没有需要处理的评论")
        return
    
    print(f"设备索引 {device_index}: 需要处理 {len(comments_to_process)}/{len(initial_contents)} 条评论")
    
    # 获取设备列表
    device_info_list = Variable.get("XHS_DEVICE_INFO_LIST", default_var=[], deserialize_json=True)
    
    # 根据email查找设备信息
    device_info = next((device for device in device_info_list if device.get('email') == email), None)
    if device_info:
        print(f"device_info: {device_info}")
    else:
        raise ValueError("email参数不能为空或设备信息不存在")
    
    # 获取设备信息
    try:
        device_ip = device_info.get('device_ip')
        device_port = device_info.get('available_appium_ports')[device_index]
        device_id = device_info.get('phone_device_list')[device_index]
        appium_server_url = f"http://{device_ip}:{device_port}"
    except Exception as e:
        print(f"获取设备信息失败: {e}")
        print(f"跳过当前任务，因为获取设备信息失败")
        raise AirflowSkipException("设备信息获取失败")
    
    print(f"使用Appium服务器: {appium_server_url}")
    print(f"使用设备ID: {device_id}")
    print(f"准备处理 {len(comments_to_process)} 条评论")
    
    # 初始化小红书操作器
    xhs = None
    successful_replies = 0
    failed_replies = 0
    
    try:
        # 初始化小红书操作器
        xhs = XHSOperator(appium_server_url=appium_server_url, force_app_launch=True, device_id=device_id)
        
        # 处理每条分配的评论
        for i, comment in enumerate(comments_to_process):
            try:
                note_url = comment['note_url']
                author = comment['author']
                comment_content = comment['content']
                comment_id = comment['comment_id']
                
                print(f"设备 {device_id} 正在处理第 {i+1}/{len(comments_to_process)} 条评论 - 作者: {author}")
                
                # 随机选择一条回复模板
                reply_content = random.choice(reply_templates)
                print(f"选择的回复模板: {reply_content}")
                
                # 调用评论回复功能
                success = xhs.comments_reply(
                    note_url=note_url,
                    author=author,
                    comment_content=comment_content,
                    reply_content=reply_content
                )
                
                if success:
                    print(f"设备 {device_id} 成功回复评论: {comment_content}")
                    # 插入到manual_reply表
                    try:
                        insert_manual_reply(
                            comment_id=comment_id,
                            note_url=note_url,
                            author=author,
                            content=comment_content,
                            reply=reply_content
                        )
                    except Exception as db_err:
                        print(f"插入回复记录到数据库失败: {str(db_err)}")
                    
                    successful_replies += 1
                else:
                    print(f"设备 {device_id} 回复评论失败: {comment_content}")
                    failed_replies += 1
                
                # 添加延时，避免操作过快
                time.sleep(2)  # 增加延时，确保有足够时间处理下一条评论
                
            except Exception as e:
                print(f"设备 {device_id} 处理评论时出错: {str(e)}")
                failed_replies += 1
                # 出错后等待时间稍长一些，避免连续失败
                time.sleep(3)
                continue
        
        print(f"设备 {device_id} 完成评论回复任务，成功回复: {successful_replies}/{len(comments_to_process)}，失败: {failed_replies}")
        return successful_replies
        
    except Exception as e:
        print(f"运行任务出错: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return successful_replies  # 即使整体任务出错，也返回已成功处理的评论数
    finally:
        # 确保关闭小红书操作器
        if xhs is not None:
            print(f"关闭设备 {device_id} 的控制器")
            try:
                xhs.close()
            except Exception as close_err:
                print(f"关闭设备控制器出错: {str(close_err)}")

# DAG 定义
with DAG(
    dag_id='xhs_comments_template_replier',
    default_args={
        'owner': 'yuchangongzhu',
        'depends_on_past': False,
        'start_date': datetime(2024, 1, 1)
    },
    description='使用模板自动回复评论',
    schedule_interval=None,
    tags=['小红书'],
    catchup=False,
    max_active_runs=5,
) as dag:

    # 创建多个任务，每个任务使用不同的设备索引
    for index in range(10):
        PythonOperator(
            task_id=f'reply_with_template_{index}',
            python_callable=reply_with_template,
            op_kwargs={
                'device_index': index,
            },
            provide_context=True,
        )