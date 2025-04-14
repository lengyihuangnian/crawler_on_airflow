from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable
from airflow.hooks.base import BaseHook

from utils.xhs_appium import XHSOperator


def get_note_url(n: int = 10, **context):
    """从数据库获取笔记URL
    Args:
        n: 要获取的URL数量
    """
    # 使用get_hook函数获取数据库连接
    db_hook = BaseHook.get_connection("xhs_db").get_hook()
    db_conn = db_hook.get_conn()
    cursor = db_conn.cursor()
    
    # 查询前n条笔记的URL
    cursor.execute("SELECT note_url FROM xhs_notes LIMIT %s", (n,))
    note_urls = [row[0] for row in cursor.fetchall()]
    
    cursor.close()
    db_conn.close()
    
    return note_urls

def save_comments_to_db(comments: list, note_url: str):
    """保存评论到数据库
    Args:
        comments: 评论列表
        note_url: 笔记URL
    """
    db_hook = BaseHook.get_connection("xhs_db").get_hook()
    db_conn = db_hook.get_conn()
    cursor = db_conn.cursor()
    
    for comment in comments:
        cursor.execute(
            "INSERT INTO xhs_comments (note_url, author, content, likes, collect_time) VALUES (%s, %s, %s, %s, %s)",
            (note_url, comment['author'], comment['content'], comment['likes'], comment['collect_time'])
        )
    
    db_conn.commit()
    cursor.close()
    db_conn.close()

def collect_xhs_comments(n: int = 10, **context):
    """收集小红书评论
    Args:
        n: 要收集的笔记数量
    """
    # 获取笔记URL
    note_urls = get_note_url(n)
    

    # 获取Appium服务器URL
    appium_server_url = Variable.get("APPIUM_SERVER_URL", "http://localhost:4723")

    print("开始收集笔记评论...")
    
    try:
         # 初始化小红书操作器
        xhs = XHSOperator(appium_server_url=appium_server_url, force_app_launch=True)
        
        all_comments = []
        for note_url in note_urls:
            try:
                # 收集评论
                comments = xhs.collect_comments_by_url(note_url)
                # 保存评论到数据库
                save_comments_to_db(comments, note_url)
                all_comments.extend(comments)
            except Exception as e:
                print(f"处理笔记 {note_url} 时出错: {str(e)}")
                continue
        
        return all_comments
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
    dag_id='xhs_comments_collector',
    default_args=default_args,
    description='收集小红书笔记评论',
    schedule_interval=None,
    tags=['小红书'],
    catchup=False,
)

collect_comments_task = PythonOperator(
    task_id='collect_xhs_comments',
    python_callable=collect_xhs_comments,
    provide_context=True,
    dag=dag,
)

collect_comments_task