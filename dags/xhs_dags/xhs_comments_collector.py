from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable
from airflow.hooks.base import BaseHook

from utils.xhs_appium import XHSOperator


def get_note_url(max_comments: int = 10, keyword: str = None, **context):
    """从数据库获取笔记URL和关键词
    Args:
        max_comments: 要获取的URL数量
        keyword: 筛选的关键词
    Returns:
        包含note_url和keyword的字典列表
    """
    # 使用get_hook函数获取数据库连接
    db_hook = BaseHook.get_connection("xhs_db").get_hook()
    db_conn = db_hook.get_conn()
    cursor = db_conn.cursor()
    
    # 根据是否有关键词来构建不同的SQL查询
    if keyword:
        cursor.execute("SELECT note_url, keyword FROM xhs_notes WHERE keyword = %s LIMIT %s", (keyword, max_comments))
    else:
        cursor.execute("SELECT note_url, keyword FROM xhs_notes LIMIT %s", (max_comments,))
    results = [{'note_url': row[0], 'keyword': row[1]} for row in cursor.fetchall()]
    
    cursor.close()
    db_conn.close()
    
    return results

def save_comments_to_db(comments: list, note_url: str, keyword: str = None):
    """保存评论到数据库
    Args:
        comments: 评论列表
        note_url: 笔记URL
        keyword: 笔记关键词
    """
    db_hook = BaseHook.get_connection("xhs_db").get_hook()
    db_conn = db_hook.get_conn()
    cursor = db_conn.cursor()

    try:
        # 检查表是否存在，如果不存在则创建
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS xhs_comments (
            id INT AUTO_INCREMENT PRIMARY KEY,
            author TEXT,
            content TEXT,
            likes INT DEFAULT 0,
            note_url TEXT,
            keyword TEXT,
            collect_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP  
        )
        """)
        db_conn.commit()
        
        # 准备插入数据的SQL语句
        insert_sql = """
        INSERT INTO xhs_comments 
        (note_url, author, content, likes, keyword, collect_time) 
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        # 批量插入评论数据
        insert_data = []
        for comment in comments:
            insert_data.append((
                note_url,
                comment.get('author', ''),
                comment.get('content', ''),
                comment.get('likes', 0),
                keyword,
                comment.get('collect_time', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            ))
        
        cursor.executemany(insert_sql, insert_data)
        db_conn.commit()
        
        print(f"成功保存 {len(comments)} 条评论到数据库")
    except Exception as e:
        db_conn.rollback()
        print(f"保存评论到数据库失败: {str(e)}")
        raise
    finally:
        cursor.close()
        db_conn.close()

def get_notes_by_url_list(note_urls: list, keyword: str = None):
    """根据传入的笔记URL列表收集评论
    Args:
        note_urls: 笔记URL列表
        keyword: 关键词（可选）
    Returns:
        所有评论的列表
    """
    # 获取设备列表
    device_info_list = Variable.get("XHS_DEVICE_INFO_LIST", default_var=[], deserialize_json=True)
    # 获取第一个设备的IP、端口和设备ID
    device_ip = device_info_list[0].get('device_ip', '42.193.193.179') if device_info_list else '42.193.193.179'
    device_port = device_info_list[0].get('available_appium_ports', [6010])[0] if device_info_list else 6010
    device_id = device_info_list[0].get('phone_device_list', ['c2c56d1b0107'])[0] if device_info_list else 'c2c56d1b0107'
    appium_server_url = f"http://{device_ip}:{device_port}"

    print(f"开始收集{len(note_urls)}条笔记的评论...")
    print(f"使用Appium服务器: {appium_server_url}")
    print(f"使用设备ID: {device_id}")
    
    try:
        # 初始化小红书操作器
        xhs = XHSOperator(appium_server_url=appium_server_url, force_app_launch=True, device_id=device_id)
        
        all_comments = []
        for note_url in note_urls:
            try:
                # 收集评论
                full_url = xhs.get_redirect_url(note_url)
                comments = xhs.collect_comments_by_url(full_url)
                # 保存评论到数据库
                save_comments_to_db(comments, note_url, keyword)
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

def collect_xhs_comments(**context):
    """收集小红书评论
    Args:
        **context: Airflow上下文参数字典
    """
    # 从DAG运行配置中获取参数，如果没有则使用默认值
    max_comments = int(context['dag_run'].conf.get('max_comments', 10) 
        if context['dag_run'].conf 
        else 1)
    
    keyword = (context['dag_run'].conf.get('keyword', None) 
              if context['dag_run'].conf 
              else '番茄')
    
    # 检查是否有传入的笔记URL列表
    note_urls = context['dag_run'].conf.get('note_urls', None) if context['dag_run'].conf else None
    
    if note_urls:
        # 如果有传入的URL列表，直接使用
        print(f"使用传入的{len(note_urls)}个笔记URL收集评论")
        return get_notes_by_url_list(note_urls, keyword)
    else:
        # 否则从数据库获取笔记URL和关键词
        notes_data = get_note_url(max_comments, keyword)
        # 提取URL列表
        note_urls = [note['note_url'] for note in notes_data]
        return get_notes_by_url_list(note_urls, keyword)

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