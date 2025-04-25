from datetime import datetime
import time

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable
from airflow.hooks.base import BaseHook

from utils.xhs_appium import XHSOperator

def get_reply_contents_from_db(comment_ids: list, max_comments: int = 10, **context):
    """从数据库获取高意向评论
    Args:
        ids: 评论ID列表
        max_comments: 本次要回复的评论数量
    Returns:
        包含note_url的字典列表
    """
    # 使用get_hook函数获取数据库连接
    db_hook = BaseHook.get_connection("xhs_db").get_hook()
    db_conn = db_hook.get_conn()
    cursor = db_conn.cursor()

    # 如果ids列表不为空，添加ID限制条件
    if comment_ids:
        placeholders = ','.join(['%s'] * len(comment_ids))
        cursor.execute(
            f"SELECT comment_id, note_url, author, content, reply FROM comment_reply WHERE comment_id IN ({placeholders}) AND is_sent = 0 LIMIT %s",
            (*comment_ids, max_comments)
        )
    else:
        cursor.execute(
            "SELECT comment_id, note_url, author, content, reply FROM comment_reply WHERE is_sent = 0 LIMIT %s",
            (max_comments,)
        )

    results = [{'comment_id': row[0], 'note_url': row[1], 'author': row[2], 'content': row[3], 'reply': row[4]} for row in cursor.fetchall()]

    cursor.close()
    db_conn.close()

    return results

def reply_high_intent_comments(**context):       
    """自动回复高意向评论
    Args:
        ids: 评论ID列表
        max_comments: 本次要回复的评论数量
        **context: Airflow上下文参数字典
        
    """
    # 从DAG运行配置中获取参数，如果没有则使用默认值
    comment_ids = (context['dag_run'].conf.get('comment_ids', [1158,1096]) 
        if context['dag_run'].conf 
        else [])
    
    max_comments = (context['dag_run'].conf.get('max_comments', 2) 
        if context['dag_run'].conf 
        else 2)
    
    # 获取评论内容
    reply_contents = get_reply_contents_from_db(comment_ids=comment_ids, max_comments=max_comments)
    
    # 获取Appium服务器URL
    appium_server_url = Variable.get("APPIUM_SERVER_CONCURRENT_URL", "http://localhost:4723")

    print(f"开始自动回复高意向评论... 本次要回复的评论数量: {max_comments}")
    
    try:
        # 初始化小红书操作器
        xhs = XHSOperator(appium_server_url=appium_server_url, force_app_launch=True, device_id='01176bc40007')
        
        # 遍历每条评论进行回复
        for content in reply_contents:
            try:
                note_url = content['note_url']
                author = content['author']
                comment_content = content['content']
                reply_content = content['reply']
                
                print(f"正在回复评论 - 作者: {author}, 内容: {comment_content}")
                
                # 调用评论回复功能
                success = xhs.comments_reply(
                    note_url=note_url,
                    author=author,
                    comment_content=comment_content,
                    reply_content=reply_content
                )
                
                if success:
                    print(f"成功回复评论: {comment_content}")
                    # 更新数据库中的回复状态
                    update_reply_status(content['comment_id'])
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

def update_reply_status(comment_id: int):
    """更新评论的回复状态
    Args:
        comment_id: 评论ID
    """
    try:
        # 使用get_hook函数获取数据库连接
        db_hook = BaseHook.get_connection("xhs_db").get_hook()
        db_conn = db_hook.get_conn()
        cursor = db_conn.cursor()
        
        # 更新评论的回复状态
        cursor.execute(
            "UPDATE comment_reply SET is_sent = 1 WHERE comment_id = %s",
            (comment_id,)
        )
        
        # 提交事务
        db_conn.commit()
        print(f"成功更新评论 {comment_id} 的回复状态")
        
    except Exception as e:
        print(f"更新评论回复状态失败: {str(e)}")
        if 'db_conn' in locals():
            db_conn.rollback()
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'db_conn' in locals():
            db_conn.close()

    
    
    # DAG 定义
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    dag_id='xhs_comments_replier',
    default_args=default_args,
    description='自动回复高意向评论',
    schedule_interval=None,
    tags=['小红书'],
    catchup=False,
)

reply_comments_task = PythonOperator(
    task_id='reply_xhs_comments',
    python_callable=reply_high_intent_comments,
    provide_context=True,
    dag=dag,
)

reply_comments_task
