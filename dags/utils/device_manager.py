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

def get_device_pool(port=4723, system_port=8200):
    """
    设备启动参数管理池。含启动参数和对应的端口号
    :param port: appium服务的端口号。每一个设备对应一个。
    :param system_port: appium服务指定的本地端口，用来转发数据给安卓设备。每一个设备对应一个。
    :return: 所有已连接设备的启动参数和appium端口号。
    """
    # 获取当前连接的所有设备
    devices = get_adb_devices()
    devs_pool = []
    
    # 为每个设备配置端口信息
    if devices:
        for device_id in devices:
            new_dict = {
                "device_id": device_id,
                "port": port,
                "system_port": system_port,
                "appium_server_url": f"http://localhost:{port}"
            }
            devs_pool.append(new_dict)
            port += 4
            system_port += 4  # 每个设备使用不同的system_port
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
        self.devices_pool = devices_pool  # 设备池
        self.device_status = {}  # 设备状态
        self.lock = threading.Lock()
        
        # 初始化设备状态
        for device in devices_pool:
            self.device_status[device['device_id']] = 'idle'
    
    def update_devices(self, devices: List[str]):
        """更新设备列表"""
        with self.lock:
            # 更新设备池中的设备状态
            for device in self.devices_pool:
                if device['device_id'] in devices:
                    if device['device_id'] not in self.device_status:
                        self.device_status[device['device_id']] = 'idle'
                else:
                    if device['device_id'] in self.device_status:
                        del self.device_status[device['device_id']]
    
    def get_available_device(self) -> Optional[Dict]:
        """获取一个空闲的设备及其配置信息"""
        with self.lock:
            for device in self.devices_pool:
                if self.device_status[device['device_id']] == 'idle':
                    self.device_status[device['device_id']] = 'busy'
                    return device
        return None
    
    def release_device(self, device_id: str):
        """释放设备"""
        with self.lock:
            if device_id in self.device_status:
                self.device_status[device_id] = 'idle'

class TaskDistributor:
    """任务分配器，负责分配任务给设备"""
    def __init__(self, device_manager: DeviceManager):
        self.device_manager = device_manager
        self.task_queue = Queue() # 任务队列
        self.results = [] # 结果列表
        self.lock = threading.Lock() # 锁
        self.device_task_count = {}  # 记录每个设备处理的任务数量
    
    def add_task(self, task_data: Dict):
        """添加任务到队列"""
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
    
    def worker(self, task_processor: Callable):
        """工作线程，处理任务队列中的任务"""
        while True:
            # 获取任务
            task = self.get_task()
            if not task:
                break
                
            # 获取可用设备及其配置
            device_info = None
            while not device_info:
                device_info = self.device_manager.get_available_device()
                if not device_info:
                    print("等待可用设备...")
                    time.sleep(1)  # 等待1秒后重试
                    continue
                
            try:
                print(f"使用设备 {device_info['device_id']} (端口: {device_info['port']}) 处理任务 {task['task_id']}")
                
                # 使用设备配置创建XHSOperator实例，确保使用正确的端口
                xhs = XHSOperator(
                    appium_server_url=f"http://localhost:{device_info['port']}",
                    force_app_launch=True,  # 强制重启应用以确保干净状态
                    device_id=device_info['device_id'],
                    system_port=device_info['system_port']  # 传入系统端口
                )
                try:
                    # 执行任务处理函数
                    result = task_processor(task, device_info, xhs)
                    self.add_result(result)
                    print(f"任务 {task['task_id']} 在设备 {device_info['device_id']} 上执行完成")
                finally:
                    xhs.close()
            except Exception as e:
                print(f"处理任务 {task['task_id']} 时出错: {str(e)}")
                # 添加错误信息到结果中
                self.add_result({
                    "device": device_info['device_id'],
                    "status": "error",
                    "error": str(e)
                })
            finally:
                # 释放设备
                self.device_manager.release_device(device_info['device_id'])
                print(f"设备 {device_info['device_id']} 已释放")
    
    def run_tasks(self, task_processor: Callable, max_workers: int = None):
        """运行所有任务"""
        # 获取当前可用的设备数量
        available_devices = len(self.device_manager.devices_pool)
        if not available_devices:
            print("No devices available")
            return []
            
        # 如果没有指定最大工作线程数，则使用可用设备数
        if max_workers is None:
            max_workers = available_devices
            
        # 创建线程池
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交任务
            futures = [executor.submit(self.worker, task_processor) for _ in range(max_workers)]
            
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
    
    def process_task(self, task: Dict, device_info: Dict, xhs: XHSOperator) -> Dict:
        """处理任务"""
        processor = self.get_processor(task['type'])
        return processor(task, device_info, xhs)

# 定义任务处理器
def collect_notes_processor(task: Dict, device_info: Dict, xhs: XHSOperator) -> Dict:
    """收集笔记任务处理器"""
    print(f"Processing collect_notes task on device {device_info['device_id']}")
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # 确保设备在首页
            if not xhs.is_at_xhs_home_page():
                print(f"设备 {device_info['device_id']} 不在首页，尝试返回首页...")
                xhs.return_to_home_page()
                time.sleep(2)  # 等待页面加载
            
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
            
           
            return {
                "device": device_info['device_id'],
                "status": "success",
                "notes_count": len(notes),
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

if __name__ == "__main__":
    # 加载.env文件
    current_dir = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(os.path.join(current_dir, '.env'))
    
    # 获取设备池
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
            "keyword": "文字游戏",
            "start_index": 0,
            "notes_per_device": 5
        },
         {
            "task_id": 2,
            "type": "collect_notes",
            "keyword": "文字游戏",
            "start_index": 5,
            "notes_per_device": 5
        },
         {
            "task_id": 3,
            "type": "collect_notes",
            "keyword": "文字游戏",
            "start_index": 10,
            "notes_per_device": 5
        },
         {
            "task_id": 4,
            "type": "collect_notes",
            "keyword": "文字游戏",
            "start_index": 15,
            "notes_per_device": 5
        },
         {
            "task_id": 5,
            "type": "collect_notes",
            "keyword": "文字游戏",
            "start_index": 20,
            "notes_per_device": 5
        },
        
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
            print(f"Output file: {result['output_file']}")
        else:
            print(f"Error: {result['error']}")
    