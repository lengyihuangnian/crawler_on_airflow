from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Optional, Callable
import threading
from queue import Queue
import time
from xhs_appium import XHSOperator, get_adb_devices
from dotenv import load_dotenv
import os
import json
from appium.webdriver.common.appiumby import AppiumBy
import subprocess

def start_appium_servers(devices, base_port=4723):
    """
    启动与设备数量对应的Appium服务，每个设备一个端口。
    :param devices: 设备ID列表
    :param base_port: 起始端口号
    :return: 端口号列表
    """
    ports = []
    for idx, device_id in enumerate(devices):
        port = base_port + idx * 4
        ports.append(port)
        # 检查端口是否已被占用（可选）
        # 启动Appium服务
        cmd = [
            "appium",
            "-p", str(port),
            "--session-override"
        ]
        # 只在主进程启动，不阻塞
        subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print(f"已为设备 {device_id} 启动 Appium 服务，端口 {port}")
        time.sleep(2)  # 给Appium服务一点启动时间
    return ports

def get_device_pool(port=4723, system_port=8200):
    """
    设备启动参数管理池。含启动参数和对应的端口号
    :param port: appium服务的端口号。每一个设备对应一个。
    :param system_port: appium服务指定的本地端口，用来转发数据给安卓设备。每一个设备对应一个。
    :return: 所有已连接设备的启动参数和appium端口号。
    """
    # 获取当前连接的所有设备
    devices = get_adb_devices()
    if not devices:
        return []
    # 启动Appium服务
    start_appium_servers(devices, base_port=port)
    devs_pool = []
    for idx, device_id in enumerate(devices):
        dev_port = port + idx * 4
        dev_system_port = system_port + idx * 4
        new_dict = {
            "device_id": device_id,
            "port": dev_port,
            "system_port": dev_system_port,
            "appium_server_url": f"http://localhost:{dev_port}"
        }
        devs_pool.append(new_dict)
    return devs_pool

class DeviceManager:
    """
    设备管理器，负责管理所有可用的设备
    Args:
            devices_pool: 设备池，包含设备信息的字典列表
            每个设备字典应包含以下信息：
                {
                'device_id': str,  # 设备ID
                'port': int,  # Appium服务器端口
                'system_port': int,  # 系统端口
                'appium_server_url': str  # Appium服务器URL
            }
    """
    def __init__(self, devices_pool: List[Dict]):
        self.devices_pool = devices_pool
        self.device_status = {}
        self.lock = threading.Lock()
        
        # 初始化设备状态
        for device in devices_pool:
            self.device_status[device['device_id']] = {
                'status': 'idle',
                'last_used': 0,  # 初始化为0
                'current_task': None
            }
    
    def update_devices(self, devices: List[str]):
        """更新设备列表"""
        with self.lock:
            # 更新设备池中的设备状态
            for device in self.devices_pool:
                if device['device_id'] in devices:
                    if device['device_id'] not in self.device_status:
                        self.device_status[device['device_id']] = {
                            'status': 'idle',
                            'last_used': 0,
                            'current_task': None
                        }
                else:
                    if device['device_id'] in self.device_status:
                        del self.device_status[device['device_id']]
    
    def get_available_device(self) -> Optional[Dict]:
        """获取一个空闲的设备及其配置信息"""
        with self.lock:
            current_time = time.time()
            for device in self.devices_pool:
                device_id = device['device_id']
                status_info = self.device_status[device_id]
                
                # 检查设备状态
                if status_info['status'] == 'idle':
                    self.device_status[device_id] = {
                        'status': 'busy',
                        'last_used': current_time,
                        'current_task': None
                    }
                    return device
                
                # 检查设备是否超时（5分钟）
                if (status_info['status'] == 'busy' and 
                    current_time - status_info['last_used'] > 300):
                    print(f"设备 {device_id} 任务超时，重置状态")
                    self.device_status[device_id] = {
                        'status': 'idle',
                        'last_used': 0,
                        'current_task': None
                    }
                    return device
                    
        return None
    
    def release_device(self, device_id: str):
        """释放设备"""
        with self.lock:
            if device_id in self.device_status:
                self.device_status[device_id] = {
                    'status': 'idle',
                    'last_used': 0,
                    'current_task': None
                }

class TaskDistributor:
    """任务分配器，负责分配任务给设备"""
    def __init__(self, device_manager: DeviceManager):
        self.device_manager = device_manager
        self.task_queue = Queue()
        self.results = []
        self.lock = threading.Lock()
        self.active_workers = 0
        self.max_workers = len(device_manager.devices_pool)
        self.task_status = {}  # 记录任务状态
        self.collected_urls = set()  # 所有设备共享的URL集合
        self.url_lock = threading.Lock()  # URL集合的锁
    
    def add_task(self, task_data: Dict):
        """添加任务到队列"""
        task_id = task_data['task_id']
        self.task_status[task_id] = {
            'status': 'pending',
            'device_id': None,
            'start_time': None
        }
        self.task_queue.put(task_data)
    
    def get_task(self) -> Optional[Dict]:
        """获取一个任务"""
        try:
            return self.task_queue.get_nowait()
        except:
            return None
    
    def add_result(self, result):
        """添加结果"""
        with self.lock:
            self.results.append(result)
    
    def add_url(self, url: str) -> bool:
        """
        添加URL到集合，如果URL已存在则返回False
        Args:
            url: 要添加的URL
        Returns:
            bool: 是否成功添加（True表示新URL，False表示重复URL）
        """
        with self.url_lock:
            if url in self.collected_urls:
                return False
            self.collected_urls.add(url)
            return True
    
    def get_collected_urls(self) -> set:
        """获取已收集的URL集合"""
        with self.url_lock:
            return self.collected_urls.copy()
    
    def get_collected_urls_count(self) -> int:
        """获取已收集的URL数量"""
        with self.url_lock:
            return len(self.collected_urls)
    
    def worker(self, task_processor: Callable):
        """工作线程，处理任务队列中的任务"""
        with self.lock:
            self.active_workers += 1
            
        try:
            while True:
                # 获取任务
                task = self.get_task()
                if not task:
                    break
                    
                task_id = task['task_id']
                
                # 获取可用设备
                device_info = self.device_manager.get_available_device()
                if not device_info:
                    print("没有可用设备，等待...")
                    time.sleep(1)
                    continue
                
                # 更新任务状态
                with self.lock:
                    self.task_status[task_id] = {
                        'status': 'running',
                        'device_id': device_info['device_id'],
                        'start_time': time.time()
                    }
                
                try:
                    print(f"使用设备 {device_info['device_id']} 处理任务 {task_id}")
                    
                    # 创建XHSOperator实例
                    xhs = XHSOperator(
                        appium_server_url=device_info['appium_server_url'],
                        force_app_launch=True,
                        device_id=device_info['device_id'],
                        system_port=device_info['system_port']
                    )
                    
                    try:
                        # 执行任务，传入URL集合
                        result = task_processor(task, device_info, xhs, self)
                        self.add_result(result)
                        
                        # 更新任务状态为完成
                        with self.lock:
                            self.task_status[task_id]['status'] = 'completed'
                    finally:
                        xhs.close()
                except Exception as e:
                    print(f"任务 {task_id} 执行失败: {str(e)}")
                    self.add_result({
                        "device": device_info['device_id'],
                        "status": "error",
                        "error": str(e)
                    })
                    # 更新任务状态为失败
                    with self.lock:
                        self.task_status[task_id]['status'] = 'failed'
                finally:
                    # 释放设备
                    self.device_manager.release_device(device_info['device_id'])
        finally:
            with self.lock:
                self.active_workers -= 1
    
    def run_tasks(self, task_processor: Callable, max_workers: int = None):
        """运行所有任务"""
        if max_workers is None:
            max_workers = self.max_workers
            
        print(f"启动 {max_workers} 个工作线程...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for _ in range(max_workers):
                future = executor.submit(self.worker, task_processor)
                futures.append(future)
            
            # 等待所有任务完成
            for future in futures:
                future.result()
        
        return self.results

class TaskProcessorManager:
    """任务处理器管理器，负责管理不同类型的任务处理器"""
    def __init__(self):
        self.processors = {}
    
    def register_processor(self, task_type: str, processor: Callable):
        """注册任务处理器"""
        self.processors[task_type] = processor
    
    def get_processor(self, task_type: str) -> Callable:
        """获取任务处理器"""
        if task_type not in self.processors:
            raise ValueError(f"Unknown task type: {task_type}")
        return self.processors[task_type]
    
    def process_task(self, task: Dict, device_info: Dict, xhs: XHSOperator, task_distributor: TaskDistributor) -> Dict:
        """处理任务"""
        processor = self.get_processor(task['type'])
        return processor(task, device_info, xhs, task_distributor)

# 定义任务处理器
def collect_notes_processor(task: Dict, device_info: Dict, xhs: XHSOperator, task_distributor: TaskDistributor) -> Dict:
    """收集笔记任务处理器"""
    print(f"正在设备 {device_info['device_id']}上收集笔记")
    max_retries = 3
    retry_count = 0
    
    # 获取目标URL数量
    target_url_count = task.get('target_url_count', 20)
    
    while retry_count < max_retries:
        try:
            # 检查是否已达到目标URL数量
            if task_distributor.get_collected_urls_count() >= target_url_count:
                print(f"已达到目标URL数量 {target_url_count}，停止收集")
                break
            
            # 使用XHSOperator的collect_notes_by_keyword方法收集笔记
            notes = xhs.collect_notes_by_keyword(
                keyword=task['keyword'],
                max_notes=task['notes_per_device'],
                filters={
                    "note_type": "图文"  # 只收集图文笔记
                }
            )
            
            if not notes:
                print(f"未找到关于 '{task['keyword']}' 的笔记")
                return {
                    "device": device_info['device_id'],
                    "status": "success",
                    "notes_count": 0,
                    "output_file": None
                }
            
            # 处理收集到的笔记
            valid_notes = []
            for note in notes:
                if note.get('note_url'):
                    # 尝试添加URL，如果成功（不重复）则保留笔记
                    if task_distributor.add_url(note['note_url']):
                        valid_notes.append(note)
                        print(f"设备 {device_info['device_id']} 收集到新笔记: {note['note_url']}")
                    else:
                        print(f"设备 {device_info['device_id']} 跳过重复笔记: {note['note_url']}")
            
            return {
                "device": device_info['device_id'],
                "status": "success",
                "notes_count": len(valid_notes),
                "notes": valid_notes,
                "collected_urls": list(task_distributor.get_collected_urls())
            }
        except Exception as e:
            retry_count += 1
            print(f"设备 {device_info['device_id']} 第 {retry_count} 次尝试失败: {str(e)}")
            if retry_count < max_retries:
                print(f"等待 5 秒后重试...")
                time.sleep(5)
                continue
            else:
                return {
                    "device": device_info['device_id'],
                    "status": "error",
                    "error": str(e)
                }
    
    # 返回最终结果
    return {
        "device": device_info['device_id'],
        "status": "success",
        "notes_count": task_distributor.get_collected_urls_count(),
        "collected_urls": list(task_distributor.get_collected_urls())
    }

if __name__ == "__main__":
    # 加载.env文件
    current_dir = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(os.path.join(current_dir, '.env'))
    
    # 获取设备池（会自动启动Appium服务）
    devices_pool = get_device_pool()
    if not devices_pool:
        print("No devices available")
        exit(1)
    print(f"Available devices: {[dev['device_id'] for dev in devices_pool]}")
    
    # 初始化设备管理器
    device_manager = DeviceManager(devices_pool)
    
    # 初始化任务分配器
    task_distributor = TaskDistributor(device_manager)
    
    # 初始化任务处理器管理器
    task_processor_manager = TaskProcessorManager()
    task_processor_manager.register_processor('collect_notes', collect_notes_processor)
    
    # 添加测试任务
    test_tasks = [
        {
            "task_id": 1,
            "type": "collect_notes",
            "keyword": "古诗词",
            "notes_per_device": 2,
            "target_url_count": 2 
        },
        {
            "task_id": 2,
            "type": "collect_notes",
            "keyword": "汉语言",
            "notes_per_device": 2,
            "target_url_count": 2
        }
    ]
    
    for task in test_tasks:
        task_distributor.add_task(task)
    
    # 运行任务
    print("\nStarting tasks...")
    results = task_distributor.run_tasks(task_processor=task_processor_manager.process_task)
    
    # 打印结果
    print("\nTask Results:")
    for result in results:
        print(f"\nDevice: {result['device']}")
        print(f"Status: {result['status']}")
        if result['status'] == 'success':
            print(f"Notes collected: {result['notes_count']}")
            if 'notes' in result:
                print("\nCollected Notes:")
                for note in result['notes']:
                    print(f"- Title: {note.get('title', 'N/A')}")
                    print(f"  Author: {note.get('author', 'N/A')}")
                    print(f"  URL: {note.get('note_url', 'N/A')}")
                    print(f"  Likes: {note.get('likes', 'N/A')}")
                    print(f"  Collects: {note.get('collects', 'N/A')}")
                    print(f"  Comments: {note.get('comments', 'N/A')}")
                    print("  " + "-" * 50)
        else:
            print(f"Error: {result['error']}")
    
    # 打印所有收集到的URL
    print("\nAll collected URLs:")
    for url in task_distributor.get_collected_urls():
        print(f"- {url}")
    