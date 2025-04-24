from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable
from airflow.hooks.base import BaseHook

from utils.device_manager import DeviceManager, TaskProcessorManager, collect_comments_processor,TaskDistributor
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

    try:
        # 检查表是否存在，如果不存在则创建
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS xhs_comments (
            id INT AUTO_INCREMENT PRIMARY KEY,
            author TEXT,
            content TEXT,
            likes INT DEFAULT 0,
            note_url TEXT,
            collect_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP  
        )
        """)
        db_conn.commit()
        
        # 准备插入数据的SQL语句
        insert_sql = """
        INSERT INTO xhs_comments 
        (note_url, author, content, likes, collect_time) 
        VALUES (%s, %s, %s, %s, %s)
        """
        
        # 批量插入评论数据
        insert_data = []
        for comment in comments:
            insert_data.append((
                note_url,
                comment.get('author', ''),
                comment.get('content', ''),
                comment.get('likes', 0),
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

def collect_xhs_comments(n: int = 10, **context):
    """收集小红书评论
    Args:
        n: 要收集的笔记数量
    """
    # 获取笔记URL
    note_urls = get_note_url(n)

        # 获取Appium服务器URL
    appium_server_url = Variable.get("APPIUM_SERVER_CONCURRENT_URL", "http://localhost:4723")

    # 获取设备池-test
    devices_pool = [
        {
            "device_id": "01176bc40007",
            "port": 4723,
            "system_port": 8200,
            "appium_server_url": appium_server_url
        },
        {
            "device_id": "c2c56d1b0107",
            "port": 4727,
            "system_port": 8204,
            "appium_server_url": appium_server_url
        }
    ]
    if not devices_pool:
        print("No devices available")
        exit(1)
    print(f"Available devices: {[dev['device_id'] for dev in devices_pool]}")
    
    # 初始化设备管理器
    device_manager = DeviceManager(devices_pool)
    
    # 初始化任务分配器
    task_distributor = TaskDistributor(device_manager)
    

    print("开始收集笔记评论...")
    
    task_processor_manager = TaskProcessorManager()
    task_processor_manager.register_processor('collect_comments', collect_comments_processor)
    
    # 获取笔记URL
    note_urls = get_note_url(n)
    
     # 创建评论收集任务列表
    comment_tasks = []
    for i, url in enumerate(note_urls, 1):
        task = {
            "task_id": i,
            "type": "collect_comments",
            "note_url": url
        }
        comment_tasks.append(task)
        
    print(f"\n准备处理 {len(comment_tasks)} 条笔记的评论...")

    # 添加任务到分发器
    # 添加评论收集任务到分发器
    for task in comment_tasks:
        task_distributor.add_task(task)
    
    # 运行任务
    print("\n开始并发收集评论...")
    results = task_distributor.run_tasks(task_processor=task_processor_manager.process_task)
    
    
    # 打印评论收集结果
    print("\n评论收集结果:")
    success_count = 0
    total_comments = 0
    failed_urls = []  # Initialize the list here
    
    for result in results:
        #打印调试
        print(f"\n设备: {result['device']}")
        print(f"状态: {result['status']}")
        if result['status'] == 'success':
            print(f"笔记URL: {result['note_url']}")
            print(f"评论数量: {result['comments_count']}")
            if result['comments']:
                success_count += 1
                total_comments += result['comments_count']
                print("\n收集到的评论示例:")
                # 只显示前3条评论作为示例
                for comment in result['comments'][:3]:
                    print(f"作者: {comment['author']}")
                    print(f"内容: {comment['content']}")
                    print(f"点赞: {comment['likes']}")
                    print(f"时间: {comment['collect_time']}")
                    print("-" * 50)
            #保存评论到数据库
            save_comments_to_db(result['comments'], result['note_url'])
        else:
            # 修改这里：从result中安全地获取note_url
            failed_url = result.get('note_url', '未知URL')  # 使用get方法，提供默认值
            print(f"错误: {result.get('error', '未知错误')}")
            failed_urls.append(failed_url)
            
    # 打印统计信息
    print("\n任务统计:")
    print(f"总任务数: {len(comment_tasks)}")
    print(f"成功任务数: {success_count}")
    print(f"失败任务数: {len(failed_urls)}")
    print(f"总收集评论数: {total_comments}")
    
    if failed_urls:
        print("\n失败的URL:")
        for url in failed_urls:
            print(f"- {url}")

# DAG 定义
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    dag_id='xhs_comments_collector_concurrent',
    default_args=default_args,
    description='并发收集小红书笔记评论',
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