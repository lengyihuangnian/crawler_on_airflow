#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowSkipException
from appium.webdriver.common.appiumby import AppiumBy

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
            content TEXT,
            likes INT DEFAULT 0,
            collects INT DEFAULT 0,
            comments INT DEFAULT 0,
            note_url TEXT,
            collect_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP  
        )
        """)
        db_conn.commit()
        
        # 准备插入数据的SQL语句
        insert_sql = """
        INSERT INTO xhs_notes 
        (keyword, title, author, content, likes, collects, comments, note_url, collect_time) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # 批量插入笔记数据
        insert_data = []
        for note in notes:
            insert_data.append((
                note.get('keyword', ''),
                note.get('title', ''),
                note.get('author', ''),
                note.get('content', ''),
                note.get('likes', 0),
                note.get('collects', 0),
                note.get('comments', 0),
                note.get('note_url', ''),
                note.get('collect_time', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            ))
        
        cursor.executemany(insert_sql, insert_data)
        db_conn.commit()
        
        print(f"成功保存 {len(notes)} 条笔记到数据库")
        
    except Exception as e:
        db_conn.rollback()
        print(f"保存笔记到数据库失败: {str(e)}")
        raise
    finally:
        cursor.close()
        db_conn.close()


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
    keyword = context['dag_run'].conf.get('keyword', '广州探店') 
    max_notes = int(context['dag_run'].conf.get('max_notes', 5))
    email = context['dag_run'].conf.get('email')
    
    # 获取设备列表
    device_info_list = Variable.get("XHS_DEVICE_INFO_LIST", default_var=[], deserialize_json=True)
    
    # 根据email查找设备信息
    device_info = next((device for device in device_info_list if device.get('email') == email), None)
    if device_info:
        print(f"device_info: {device_info}")
    else:
        raise ValueError("email参数不能为空")
    
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
        
        # 用于每收集三条笔记保存一次的工具函数
        batch_size = 3  # 每批次保存的笔记数量
        collected_notes = []  # 所有收集到的笔记
        current_batch = []  # 当前批次的笔记
        
        # 定义处理笔记的回调函数
        def process_note(note):
            nonlocal collected_notes, current_batch
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
        
        # 封装为函数 get_note_card
        get_note_card(xhs, collected_notes, collected_titles, max_notes, process_note, keyword)

        
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
                        note_card.click()
                        time.sleep(0.5)
                        note_data = xhs.get_note_data(note_title_and_text)
                        if note_data:
                            note_data['keyword'] = keyword
                            collected_titles.append(note_title_and_text)
                            process_note(note_data)
                        xhs.driver.press_keycode(4)
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


def get_note_card(xhs, collected_notes, collected_titles, max_notes, process_note, keyword):
    """
    新版资源ID路径的收集逻辑
    """
    import time
    from appium.webdriver.common.appiumby import AppiumBy
    while len(collected_notes) < max_notes:
        try:
            note_cards = []
            try:
                note_cards = xhs.driver.find_elements(
                    by=AppiumBy.XPATH,
                    value='//android.widget.FrameLayout[@resource-id="com.xingin.xhs:id/0_resource_name_obfuscated" and @clickable="true"]'
                )
                print(f"获取新版资源ID笔记卡片成功，共{len(note_cards)}个")
                print("---------------card----------------")
                xhs.print_all_elemnts()
            except Exception as e:
                print(f"获取资源ID笔记卡片失败: {e}")
            for note_card in note_cards:
                if len(collected_notes) >= max_notes:
                    break
                try:
                    # 获取标题 - 笔记卡片上部的标题文本
                    title_elements = note_card.find_elements(
                        by=AppiumBy.XPATH,
                        value=".//android.widget.LinearLayout/android.widget.TextView[@resource-id='com.xingin.xhs:id/0_resource_name_obfuscated']"
                    )
                    # 通常第一个文本元素是标题
                    note_title_and_text = title_elements[0].text if title_elements else ''
                    
                    # 获取作者 - 笔记卡片下部的作者名称
                    author_elements = note_card.find_elements(
                        by=AppiumBy.XPATH,
                        value=".//android.widget.TextView[@resource-id='com.xingin.xhs:id/0_resource_name_obfuscated' and contains(@text, '')]"
                    )
                    author = author_elements[0].text
                    if note_title_and_text not in collected_titles:
                        print(f"收集笔记: {note_title_and_text}, 作者: {author}, 当前收集数量: {len(collected_notes)}")
                        note_card.click()
                        time.sleep(0.5)
                        note_data = xhs.get_note_data(note_title_and_text)
                        if note_data:
                            note_data['keyword'] = keyword
                            collected_titles.append(note_title_and_text)
                            process_note(note_data)
                        xhs.driver.press_keycode(4)
                        time.sleep(0.5)
                except Exception as e:
                    print(f"处理新版资源ID笔记卡片失败: {str(e)}")
                    continue
            if len(collected_notes) < max_notes:
                xhs.scroll_down()
                time.sleep(0.5)
        except Exception as e:
            print(f"收集新版资源ID笔记失败: {str(e)}")
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
