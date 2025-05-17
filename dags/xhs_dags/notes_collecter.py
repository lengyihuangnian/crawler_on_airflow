#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable
from airflow.hooks.base import BaseHook
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


def collect_xhs_notes(device_index_override=None, port_index_override=None, **context) -> None:
    """
    收集小红书笔记    
    Args:
        device_index_override: 可选的设备索引覆盖参数
        port_index_override: 可选的端口索引覆盖参数
        **context: Airflow上下文参数字典
    
    Returns:
        None
    """
    # 获取任务实例对象，用于XCom传递数据
    ti = context['ti']
    # 获取关键词，默认为"AI客服"
    keyword = (context['dag_run'].conf.get('keyword', '广州探店') 
              if context['dag_run'].conf 
              else '广州探店')
    
    # 获取最大收集笔记数，默认为5
    max_notes = int(context['dag_run'].conf.get('max_notes', 5)
                if context['dag_run'].conf
                else 5)
    
    # 从配置中获取参数
    conf = context.get('dag_run').conf if context.get('dag_run') else {}
    
    # 获取email参数，用于查找设备信息
    email = conf.get('email') if conf else None
    
    # 端口和设备ID索引（后面用于选择列表中的项）
    # 优先使用函数传入的override参数，如果没有再使用conf中的设置
    port_index = port_index_override if port_index_override is not None else (int(conf.get('port_index', 0)) if conf else 0)
    device_index = device_index_override if device_index_override is not None else (int(conf.get('device_index', 0)) if conf else 0)
    
    # 获取设备列表
    device_info_list = Variable.get("XHS_DEVICE_INFO_LIST", default_var=[], deserialize_json=True)
    
    # 根据email查找设备信息，如果没有提供email，则使用默认用户名
    if email:
        device_info = next((device for device in device_info_list if device.get('email') == email), None)
        print(f"根据email '{email}' 查找设备信息")
    else:
        # 默认使用用户名查找
        target_username = "lucy"  # 默认用户名
        device_info = next((device for device in device_info_list if device.get('username') == target_username), None)
        print(f"使用默认用户名 '{target_username}' 查找设备信息")
    
    print(f"获取到的设备信息: \n{device_info}")
    
    # 如果找不到设备信息，使用默认值
    device_ip = device_info.get('device_ip', '42.193.193.179') if device_info else '42.193.193.179'
    
    # 获取可用的appium端口列表
    available_ports = device_info.get('available_appium_ports', [6030]) if device_info else [6030]
    # 根据索引选择端口，确保索引有效
    if port_index < 0 or port_index >= len(available_ports):
        port_index = 0
        print(f"警告: 端口索引 {port_index} 超出范围，使用默认端口索引 0")
    device_port = available_ports[port_index]
    print(f"使用端口索引 {port_index}，选择端口 {device_port}")
    
    # 获取可用的设备ID列表
    available_devices = device_info.get('phone_device_list', ['c2c56d1b0107']) if device_info else ['c2c56d1b0107']
    # 根据索引选择设备ID，确保索引有效
    if device_index < 0 or device_index >= len(available_devices):
        device_index = 0
        print(f"警告: 设备索引 {device_index} 超出范围，使用默认设备索引 0")
    device_id = available_devices[device_index]
    print(f"使用设备索引 {device_index}，选择设备 {device_id}")
    appium_server_url = f"http://{device_ip}:{device_port}"

    #test
    # appium_server_url = 'http://42.193.193.179:6010'
    # device_id = 'c2c5819d0107'

    print(f"开始收集关键词 '{keyword}' 的小红书笔记... ，数量为'{max_notes}")
    
    try:
        # 初始化小红书操作器
        xhs = XHSOperator(appium_server_url=appium_server_url, force_app_launch=True, device_id=device_id)
        xhs.print_all_elements()
        
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
        
        while len(collected_notes) < max_notes:
            try:
                # 获取所有笔记卡片元素
                print("获取所有笔记卡片元素")
                note_cards = xhs.driver.find_elements(
                    by=AppiumBy.XPATH,
                    value="//android.widget.FrameLayout[@resource-id=\"com.xingin.xhs:id/0_resource_name_obfuscated\"]"
                )
                print(f"获取所有笔记卡片元素成功,共{len(note_cards)}个")
                
                for note_card in note_cards:
                    if len(collected_notes) >= max_notes:
                        break
                        
                    try:
                        # 获取笔记标题
                        title_element = note_card.find_element(
                            by=AppiumBy.XPATH,
                            value=".//android.widget.TextView[contains(@text, '')]"
                        )
                        note_title_and_text = title_element.text
                        
                        # 获取作者信息
                        author_element = note_card.find_element(
                            by=AppiumBy.XPATH,
                            value=".//android.widget.LinearLayout/android.widget.TextView[1]"
                        )
                        author = author_element.text
                        
                        if note_title_and_text not in collected_titles:
                            print(f"收集笔记: {note_title_and_text}, 作者: {author}, 当前收集数量: {len(collected_notes)}")

                            # 点击笔记
                            note_card.click()
                            time.sleep(1)

                            # 获取笔记内容
                            note_data = xhs.get_note_data(note_title_and_text)
                            
                            # 如果笔记数据不为空，则添加到列表中并处理
                            if note_data:
                                note_data['keyword'] = keyword
                                collected_titles.append(note_title_and_text)
                                process_note(note_data)  # 处理笔记，可能会保存到数据库

                            # 返回上一页
                            xhs.driver.press_keycode(4)  # Android 返回键
                            time.sleep(1)
                    except Exception as e:
                        print(f"处理笔记卡片失败: {str(e)}")
                        continue
                
                # 滑动到下一页
                if len(collected_notes) < max_notes:
                    xhs.scroll_down()
                    time.sleep(1)
            
            except Exception as e:
                print(f"收集笔记失败: {str(e)}")
                import traceback
                print(traceback.format_exc())
                break
        
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
        ti.xcom_push(key='note_urls', value=note_urls)
        ti.xcom_push(key='keyword', value=keyword)
        
        return note_urls
            
    except Exception as e:
        error_msg = f"收集小红书笔记失败: {str(e)}"
        print(error_msg)
        raise
    finally:
        # 确保关闭小红书操作器
        if 'xhs' in locals():
            xhs.close()


# DAG 定义
default_args = {
    'owner': 'yuchangongzhu',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='notes_collector',
    default_args=default_args,
    description='定时收集小红书笔记',
    schedule_interval=None,
    tags=['小红书'],
    catchup=False,
    max_active_runs=1,
) as dag:

    # 创建多个并行的笔记收集任务，使用不同的设备索引和端口索引
    collect_notes_0 = PythonOperator(
        task_id='collect_xhs_notes_0',
        python_callable=collect_xhs_notes,
        op_kwargs={
            'device_index_override': 0,
            'port_index_override': 0
        },
        provide_context=True,
    )
    
    collect_notes_1 = PythonOperator(
        task_id='collect_xhs_notes_1',
        python_callable=collect_xhs_notes,
        op_kwargs={
            'device_index_override': 1,
            'port_index_override': 1
        },
        provide_context=True,
    )
    
    # 如果需要更多并行任务，可以根据需要添加
    # collect_notes_2 = PythonOperator(
    #     task_id='collect_xhs_notes_2',
    #     python_callable=collect_xhs_notes,
    #     op_kwargs={
    #         'device_index_override': 2,
    #         'port_index_override': 2
    #     },
    #     provide_context=True,
    # )
    
    # 所有任务以并行方式运行
    [collect_notes_0, collect_notes_1]
