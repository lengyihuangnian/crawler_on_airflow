#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time
from datetime import datetime, timedelta
import re 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowSkipException
from appium.webdriver.common.appiumby import AppiumBy
import base64
import requests
from utils.xhs_appium import XHSOperator



def save_notes_to_db(notes: list) -> None:
    """
    保存笔记到数据库(如果表不存在，则初始新建该表)
    """
    # 使用get_hook函数获取数据库连接
    db_hook = BaseHook.get_connection("xhs_db").get_hook()
    db_conn = db_hook.get_conn()
    cursor = db_conn.cursor()

    try:
        # 检查表是否存在，如果不存在则创建
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS xhs_notes (
            id INT AUTO_INCREMENT PRIMARY KEY,
            keyword TEXT,
            title TEXT NOT NULL,
            author TEXT,
            userInfo TEXT,
            content TEXT,
            likes INT DEFAULT 0,
            collects INT DEFAULT 0,
            comments INT DEFAULT 0,
            note_url VARCHAR(512) DEFAULT NULL,
            collect_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            note_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            note_location TEXT 
        )
        """)
        db_conn.commit()
        
        # 准备插入数据的SQL语句 - 使用INSERT IGNORE避免重复插入
        insert_sql = """
        INSERT IGNORE INTO xhs_notes
        (keyword, title, author, userInfo, content, likes, collects, comments, note_url, collect_time, note_time, note_location)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # 批量插入笔记数据
        insert_data = []
        for note in notes:
            insert_data.append((
                note.get('keyword', ''),
                note.get('title', ''),
                note.get('author', ''),
                note.get('userInfo', ''),
                note.get('content', ''),
                note.get('likes', 0),
                note.get('collects', 0),
                note.get('comments', 0),
                note.get('note_url', ''),
                note.get('collect_time', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                note.get('note_time', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                note.get('note_location', ''),
            ))

        # 执行插入操作
        cursor.executemany(insert_sql, insert_data)

        # 获取实际插入的记录数
        cursor.execute("SELECT ROW_COUNT()")
        affected_rows = cursor.fetchone()[0]
        
        db_conn.commit()
        
        print(f"成功保存 {affected_rows} 条新笔记到数据库，跳过 {len(notes) - affected_rows} 条重复笔记")
        
    except Exception as e:
        db_conn.rollback()
        print(f"保存笔记到数据库失败: {str(e)}")
        raise
    finally:
        cursor.close()
        db_conn.close()

def get_time_range():
    from datetime import datetime, timedelta
    
    current_time = datetime.utcnow() - timedelta(seconds=10)
    twelve_hours_ago = current_time - timedelta(hours=12)
    
    current_time_iso = current_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    twelve_hours_ago_iso = twelve_hours_ago.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    
    return {
        'current_time': current_time_iso,
        'twelve_hours_ago': twelve_hours_ago_iso
    }
def btoa(string_to_encode):
    # 将字符串转换为字节
    bytes_to_encode = string_to_encode.encode('utf-8')
    # 进行Base64编码
    encoded_bytes = base64.b64encode(bytes_to_encode)
    # 转换回字符串
    return encoded_bytes.decode('utf-8')


#清除task状态
def clear_task_status(dag_id,dag_run_id):
    headers = {
    'Authorization': f'Basic {btoa(f"claude89757:claude@airflow")}',
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
    "Connection": "keep-alive",
    "Content-Type": "application/json",
    "Origin": "https://marketing.lucyai.sale",
    "Referer": "https://marketing.lucyai.sale/airflow/api/v1/ui/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36 Edg/137.0.0.0",
    "accept": "application/json",
    "sec-ch-ua": "\"Microsoft Edge\";v=\"137\", \"Chromium\";v=\"137\", \"Not/A)Brand\";v=\"24\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\""
    }

    url = f"https://marketing.lucyai.sale/airflow/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/clear"
    data = {"dry_run": False}
    response = requests.post(url, headers=headers,  json=data)

    print(response.text)
    print(response)
#清除run'dag状态
def clear_dag_run_status(dag_id,dag_run_id):
    headers = {
        'Authorization': f'Basic {btoa(f"claude89757:claude@airflow")}',
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "Connection": "keep-alive",
        "Content-Type": "application/json",
        "Origin": "https://marketing.lucyai.sale",
        "Referer": "https://marketing.lucyai.sale/airflow/api/v1/ui/",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36 Edg/137.0.0.0",
        "accept": "application/json",
        "sec-ch-ua": "\"Microsoft Edge\";v=\"137\", \"Chromium\";v=\"137\", \"Not/A)Brand\";v=\"24\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\""
    }

    url = f"https://marketing.lucyai.sale/airflow/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}"
    data = {"state": "failed"}
    response = requests.patch(url, headers=headers,  json=data)

    print(response.text)
    print(response)

def deal_with_conflict(email):
    """获取DAG运行信息"""
    time_range = get_time_range()
    headers = {
        'Authorization': f'Basic {btoa(f"claude89757:claude@airflow")}',
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "Connection": "keep-alive",
        "Referer": "https://marketing.lucyai.sale/airflow/api/v1/ui/",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36 Edg/137.0.0.0",
        "accept": "application/json",
        "sec-ch-ua": "\"Microsoft Edge\";v=\"137\", \"Chromium\";v=\"137\", \"Not/A)Brand\";v=\"24\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\""
    }
    url = f"https://marketing.lucyai.sale/airflow/api/v1/dags/notes_collector/dagRuns"
    params = {
        "limit": "100",
        "start_date_gte": time_range['twelve_hours_ago'],
        "start_date_lte": time_range['current_time']        
    }
    response = requests.get(url, headers=headers, params=params).json()

    for i in response['dag_runs']:
        print(i['conf']['email'], i['dag_run_id'], i['state'])
        if i['state'] == 'running' and i['conf']['email'] == email:
            # 清除任务状态，解决appium冲突
            # clear_task_status(i['dag_id'], i['dag_run_id'])
            clear_dag_run_status(i['dag_id'], i['dag_run_id'])
            time.sleep(15)  # 等待1秒，确保状态清除完成

def collect_xhs_notes(device_index=0, **context) -> None:
    """
    收集小红书笔记    
    Args:
        device_index: 设备索引
        **context: Airflow上下文参数字典
    
    Returns:
        None
    """
    # 获取输入参数
    keyword = context['dag_run'].conf.get('keyword') 
    max_notes = int(context['dag_run'].conf.get('max_notes'))
    email = context['dag_run'].conf.get('email')
    # 处理冲突
    deal_with_conflict(email)
    # 获取设备列表
    device_info_list = Variable.get("XHS_DEVICE_INFO_LIST", default_var=[], deserialize_json=True)
    
    # 根据email查找设备信息
    device_info = next((device for device in device_info_list if device.get('email') == email), None)
    if device_info:
        print(f"device_info: {device_info}")
    else:
        raise ValueError("email参数不能为空")
    
    # 创建数据库连接信息，用于process_note中检查笔记是否存在
    db_hook = BaseHook.get_connection("xhs_db").get_hook()
    
    # 获取设备信息
    try:
        device_ip = device_info.get('device_ip')
        appium_port = device_info.get('available_appium_ports')[device_index]
        device_id = device_info.get('phone_device_list')[device_index]
    except Exception as e:
        print(f"获取设备信息失败: {e}")
        print(f"跳过当前任务，因为获取设备信息失败")
        raise AirflowSkipException("设备信息获取失败")

    # 获取appium_server_url
    appium_server_url = f"http://{device_ip}:{appium_port}"
    
    print(f"选择设备 {device_id}, appium_server_url: {appium_server_url}")
    print(f"开始收集关键词 '{keyword}' 的小红书笔记... ，数量为'{max_notes}'")
    
    xhs = None
    try:
        # 初始化小红书操作器
        xhs = XHSOperator(appium_server_url=appium_server_url, force_app_launch=True, device_id=device_id)
        
        # 将xhs操作器实例传递给自定义Operator，以便在任务被取消时能够关闭
        if 'set_xhs_operator' in context:
            context['set_xhs_operator'](xhs)
        
        # 用于每收集三条笔记保存一次的工具函数
        batch_size = 3  # 每批次保存的笔记数量
        collected_notes = []  # 所有收集到的笔记
        current_batch = []  # 当前批次的笔记
        
        # 定义处理笔记的回调函数
        def process_note(note):
            nonlocal collected_notes, current_batch
            
            # 添加email信息到userInfo字段
            note['userInfo'] = email
            
            # 检查笔记URL是否已存在
            note_url = note.get('note_url', '')
            if note_url:
                # 直接查询数据库
                db_conn = db_hook.get_conn()
                cursor = db_conn.cursor()
                try:
                    cursor.execute("SELECT 1 FROM xhs_notes WHERE note_url = %s AND keyword = %s LIMIT 1", (note_url, keyword))
                    if cursor.fetchone():
                        print(f"笔记已存在，跳过: {note.get('title', '')}")
                        return
                    else:
                        print(f"笔记不存在，添加: {note.get('title', '')},{note.get('keyword', '')},{note.get('note_url', '')}, email: {email}")
                finally:
                    cursor.close()
                    db_conn.close()
                
            collected_notes.append(note)
            current_batch.append(note)
            
            # 当收集到3条笔记时保存到数据库
            if len(current_batch) >= batch_size:
                print(f"保存批次数据到数据库，当前批次包含 {len(current_batch)} 条笔记")
                save_notes_to_db(current_batch)
                current_batch = []  # 清空当前批次
        
        # 搜索关键词，并且开始收集
        print(f"搜索关键词: {keyword}")
        xhs.search_keyword(keyword, filters={
            "note_type": "图文"
        })
        
        print(f"开始收集笔记,计划收集{max_notes}条...")
        collected_titles = []

        print("---------------card----------------")
        xhs.print_all_elements()
        
        # 封装为函数 get_note_card
        get_note_card_init(xhs, collected_notes, collected_titles, max_notes, process_note, keyword)

        
        # 如果还有未保存的笔记，保存剩余的笔记
        if current_batch:
            print(f"保存剩余 {len(current_batch)} 条笔记到数据库")
            save_notes_to_db(current_batch)
        
        if not collected_notes:
            print(f"未找到关于 '{keyword}' 的笔记")
            return
            
        # 打印收集结果
        print("\n收集完成!")
        print(f"共收集到 {len(collected_notes)} 条笔记")
        
        # 提取笔记URL列表并存入XCom
        note_urls = [note.get('note_url', '') for note in collected_notes]
        context['ti'].xcom_push(key='note_urls', value=note_urls)
        context['ti'].xcom_push(key='keyword', value=keyword)
        
        return note_urls
            
    except Exception as e:
        error_msg = f"收集小红书笔记失败: {str(e)}"
        print(error_msg)
        raise
    finally:
        # 确保关闭小红书操作器
        if xhs:
            xhs.close()


def get_note_card_init(xhs, collected_notes, collected_titles, max_notes, process_note, keyword):
    """
    收集小红书笔记卡片
    """
    import time
    from appium.webdriver.common.appiumby import AppiumBy
    while len(collected_notes) < max_notes:
        try:
            print("获取所有笔记卡片元素")
            note_cards = []
            try:
                # 只保留原始方法
                note_cards = xhs.driver.find_elements(
                    by=AppiumBy.XPATH,
                    value="//android.widget.FrameLayout[@resource-id='com.xingin.xhs:id/-' and @clickable='true']"
                )
                print(f"获取笔记卡片成功，共{len(note_cards)}个")
            except Exception as e:
                print(f"获取笔记卡片失败: {e}")
            for note_card in note_cards:
                if len(collected_notes) >= max_notes:
                    break
                try:
                    title_element = note_card.find_element(
                        by=AppiumBy.XPATH,
                        value=".//android.widget.TextView[contains(@text, '')]"
                    )
                    note_title_and_text = title_element.text
                    author_element = note_card.find_element(
                        by=AppiumBy.XPATH,
                        value=".//android.widget.LinearLayout/android.widget.TextView[1]"
                    )
                    author = author_element.text
                    if note_title_and_text not in collected_titles:
                        print(f"收集笔记: {note_title_and_text}, 作者: {author}, 当前收集数量: {len(collected_notes)}")
                        
                        # 获取屏幕尺寸和元素位置
                        try:
                            screen_size = xhs.driver.get_window_size()
                            element_location = title_element.location
                            screen_height = screen_size['height']
                            element_y = element_location['y']
                            
                            # 检查元素是否位于屏幕高度的3/4以上
                            if element_y > screen_height * 0.25:
                                # 点击标题元素而不是整个卡片
                                print(f"元素位置正常，位于屏幕{element_y/screen_height:.2%}处，执行点击")
                                title_element.click()
                                time.sleep(0.5)
                            else:
                                print(f"元素位置过高，位于屏幕{element_y/screen_height:.2%}处，跳过点击")
                                continue
                        except Exception as e:
                            print(f"检测元素位置失败: {str(e)}，不执行点击")
                            # 默认点击标题元素
                            # title_element.click()
                            time.sleep(0.5)
                        note_data = xhs.get_note_data(note_title_and_text)
                        # time.sleep(0.5)
                        # xhs.bypass_share()
                        if note_data:
                            note_data['keyword'] = keyword
                            collected_titles.append(note_title_and_text)
                            process_note(note_data)
                        back_btn = xhs.driver.find_element( by=AppiumBy.XPATH,
                        value="//android.widget.Button[@content-desc='返回']")
                        back_btn.click()
                        time.sleep(0.5)
                except Exception as e:
                    print(f"处理笔记卡片失败: {str(e)}")
                    continue
            if len(collected_notes) < max_notes:
                xhs.scroll_down()
                time.sleep(0.5)
        except Exception as e:
            print(f"收集笔记失败: {str(e)}")
            import traceback
            print(traceback.format_exc())
            break

with DAG(
    dag_id='notes_collector',
    default_args={'owner': 'yuchangongzhu', 'depends_on_past': False, 'start_date': datetime(2024, 1, 1)},
    description='定时收集小红书笔记',
    schedule_interval=None,
    tags=['小红书'],
    catchup=False,
    max_active_runs=5,
) as dag:

    for index in range(10):
        PythonOperator(
            task_id=f'collect_xhs_notes_{index}',
            python_callable=collect_xhs_notes,
            op_kwargs={
                'device_index': index,
            },
            provide_context=True,
           
        )
