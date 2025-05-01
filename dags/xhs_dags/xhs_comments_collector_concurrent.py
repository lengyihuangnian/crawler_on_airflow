from datetime import datetime
import subprocess
import time
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable
from airflow.hooks.base import BaseHook
from airflow.models.xcom import XCom

from utils.device_manager import collect_comments_processor

from utils.xhs_appium import XHSOperator

def get_note_url(n: int = 10, **context):
    """从数据库获取笔记URL"""
    db_hook = BaseHook.get_connection("xhs_db").get_hook()
    db_conn = db_hook.get_conn()
    cursor = db_conn.cursor()
    
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


# def start_remote_appium_servers(devices, base_port=6001):
#     """
#     在远程主机上启动与设备数量对应的Appium服务，每个设备一个端口。
#     :param devices: 设备ID列表
#     :param base_port: 起始端口号
#     :return: 端口号列表
#     """
#     remote_host = Variable.get("REMOTE_TEST_HOST", "localhost")
#     ports = []
#     for idx, device_id in enumerate(devices):
#         port = base_port + idx
#         ports.append(port)
#         # 使用SSH在远程主机上启动Appium服务
#         cmd = f"ssh {remote_host} 'appium -p {port} --session-override'"
#         subprocess.Popen(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
#         print(f"已在远程主机 {remote_host} 上为设备 {device_id} 启动 Appium 服务，端口 {port}")
#         time.sleep(1)  # 给Appium服务一点启动时间
#     return ports

def get_adb_devices_from_remote(remote_host, **context):
    """调用dag，从远程主机获取设备池"""
    # test-使用预定义的设备信息
    devices = [
        {"device_id": "97266a1f0107", "port": 6001},
        {"device_id": "c2c56d1b0107", "port": 6002}
    ]
    print(f"Using devices: {[d['device_id'] for d in devices]}")
    return devices

def get_devices_pool_from_remote(port=6001, system_port=8200, **context): 
    """远程控制设备启动参数管理池。含启动参数和对应的端口号"""
    appium_server_url = Variable.get("APPIUM_SERVER_CONCURRENT_URL", "http://localhost:4723")
    remote_host = Variable.get("REMOTE_TEST_HOST", "localhost")
    #获取远程主机连接的设备
    devices_pool = get_adb_devices_from_remote(remote_host)
    
    # 构建设备池，使用已配置的Appium服务端口
    devs_pool = []
    for idx, device in enumerate(devices_pool):
        dev_port = device["port"]  # 使用设备预定义的端口
        dev_system_port = system_port + idx * 4  # 为每个设备分配唯一的系统端口
        new_dict = {
            "device_id": device["device_id"],
            "port": dev_port,
            "system_port": dev_system_port,
            "appium_server_url": f"{appium_server_url}:{dev_port - 1278}" 
        }
        devs_pool.append(new_dict)
        print(f"设备 {device['device_id']} 配置: {new_dict}")
    return devs_pool

def collect_comments_for_device(device_info, note_urls, collected_comments=None, **context):
    """为单个设备收集评论"""
    try:
        # 创建XHSOperator实例
        xhs = XHSOperator(
            appium_server_url=device_info['appium_server_url'],
            force_app_launch=True,
            device_id=device_info['device_id'],
            system_port=device_info['system_port']
        )
        
        try:
            all_results = []
            for note_url in note_urls:
                # 执行评论收集
                result = collect_comments_processor(
                    {"note_url": note_url},
                    device_info,
                    xhs,
                    collected_comments
                )
                all_results.append(result)
            
            return {
                "status": "success",
                "device_id": device_info['device_id'],
                "results": all_results
            }
        finally:
            xhs.close()
    except Exception as e:
        return {
            "status": "error",
            "device_id": device_info['device_id'],
            "error": str(e),
            "collected_comments": collected_comments
        }

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
    max_active_runs=1,
    concurrency=2,
    max_active_tasks=2,
)

# 获取笔记URL的任务
get_note_urls_task = PythonOperator(
    task_id='get_note_urls',
    python_callable=get_note_url,
    provide_context=True,
    dag=dag,
)

# 定义任务组
@task_group(group_id="device_tasks", dag=dag)
def create_device_tasks():
    """动态创建设备任务组"""
    devices_pool = get_devices_pool_from_remote()
    
    # 初始化已收集评论集合
    collected_comments = set()
    
    for device in devices_pool:
        device_id = device['device_id']
        
        @task(task_id=f'collect_comments_device_{device_id}')
        def collect_comments(device_info=device, **context):
            # 获取所有笔记URL
            all_note_urls = context['task_instance'].xcom_pull(task_ids='get_note_urls')
            
            # 根据设备ID分配URL，确保每个URL只被分配给一个设备
            device_index = next(i for i, dev in enumerate(devices_pool) if dev['device_id'] == device_info['device_id'])
            assigned_urls = [url for i, url in enumerate(all_note_urls) if i % len(devices_pool) == device_index]
            
            print(f"设备 {device_info['device_id']} 分配到的URL数量: {len(assigned_urls)}")
            
            # 获取之前所有设备收集的评论
            previous_comments = context['task_instance'].xcom_pull(task_ids=None, key='collected_comments') or set()
            # 合并到当前设备的已收集评论集合
            current_collected_comments = set(previous_comments)
            
            # 收集评论
            result = collect_comments_for_device(
                device_info=device_info,
                note_urls=assigned_urls,
                collected_comments=current_collected_comments
            )
            
            # 将新收集的评论添加到集合中
            if result and 'comments' in result:
                current_collected_comments.update(comment['comment_id'] for comment in result['comments'])
            
            # 将更新后的评论集合推送到XCom
            context['task_instance'].xcom_push(key='collected_comments', value=list(current_collected_comments))
            
            return result
        
        collect_comments()

# 创建设备任务组
device_tasks = create_device_tasks()

# 设置任务依赖关系
get_note_urls_task >> device_tasks