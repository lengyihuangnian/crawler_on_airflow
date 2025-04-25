import os
import requests
import time
from typing import List, Dict
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from requests.exceptions import RequestException
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# OpenRouter API 基础 URL
OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"

# 在运行时初始化 OpenRouter 客户端
def get_openrouter_key():
    """获取 OpenRouter API key"""
    # 优先从 Airflow 变量中获取
    try:
        from airflow.models import Variable
        api_key = Variable.get("OPENROUTER_API_KEY", default_var=None)
    except Exception as e:
        print(f"从 Airflow Variable 获取 API key 失败: {e}")
        api_key = None
    
    # 如果Airflow变量中没有，尝试从环境变量获取（作为备选方案）
    if not api_key:
        api_key = os.getenv("OPENROUTER_API_KEY")
    
    if not api_key:
        raise ValueError("在 Airflow 变量和环境变量中均未找到 OpenRouter API key")
        
    return api_key

def get_comments_from_db(comment_ids=None, limit=100):
    """
    从数据库获取评论数据
    
    :param comment_ids: 可选，评论 ID 列表，如果提供则仅获取这些 ID 的评论
    :param limit: 获取记录的最大数量（当comment_ids为空时使用）
    :return: 评论列表
    """
    # 使用Airflow的BaseHook获取数据库连接
    db_hook = BaseHook.get_connection("xhs_db").get_hook()
    db_conn = db_hook.get_conn()
    cursor = db_conn.cursor()  # 标准游标
    
    comments = []
    try:
        # 构建查询
        if comment_ids and len(comment_ids) > 0:
            # 如果提供了具体的comment_ids，则只获取这些评论
            format_strings = ','.join(['%s'] * len(comment_ids))
            query = f"SELECT id, author, content, note_url FROM xhs_comments WHERE id IN ({format_strings})"
            # 确保 params 是一个元组
            params = tuple(comment_ids)
        else:
            # 如果没有提供具体的comment_ids，则获取最新的评论
            query = f"SELECT id, author, content, note_url FROM xhs_comments ORDER BY id DESC LIMIT {limit}"
            params = []
        
        # 执行查询
        cursor.execute(query, params)
        result = cursor.fetchall()
        
        # 获取列名
        columns = [desc[0] for desc in cursor.description]
        
        # 将元组结果转换为字典列表
        comments = []
        for row in result:
            comment = dict(zip(columns, row))
            comments.append(comment)
        
        print(f"从数据库获取了 {len(comments)} 条评论")
        
    except Exception as e:
        print(f"数据库查询失败: {str(e)}")
    finally:
        cursor.close()
        db_conn.close()
    
    return comments

def generate_comment_replies(comments: List[Dict[str, str]], reply_prompt: str) -> List[Dict[str, str]]:
    """
    批量生成评论回复
    
    :param comments: 评论列表，每个评论是包含 'author', 'content', 'id' 等键的字典
    :param reply_prompt: 回复提示，如“回复建议话术”
    :return: 带有回复内容的评论列表
    """
    results = []
    
    total_comments = len(comments)
    print(f"准备为 {total_comments} 条评论生成回复...")
    
    for i, comment in enumerate(comments, 1):
        try:
            print(f"正在为第 {i}/{total_comments} 条评论生成回复...")
            author = comment.get('author', '')
            content = comment.get('content', '')
            
            # 每5个评论后暂停，避免请求过于频繁
            if i > 1 and i % 5 == 0:
                print(f"已完成 {i}/{total_comments} 条评论的回复生成，短暂停后继续...")
                time.sleep(2)
            
            # 生成回复
            reply = generate_single_reply(content, author, reply_prompt)
            
            # 添加原始评论及生成的回复
            result = comment.copy()
            result['reply'] = reply
            results.append(result)
            
        except Exception as e:
            print(f"生成评论回复时出错: {str(e)}")
            # 当单个评论出错时，添加默认回复并继续处理其他评论
            result = comment.copy()
            result['reply'] = "谢谢您的评论，我们会认真参考。"  # 默认回复
            result['error'] = str(e)  # 记录错误信息
            results.append(result)
            
            # 出错后暂停一会再继续
            time.sleep(3)
    
    print(f"完成全部 {total_comments} 条评论的回复生成")
    return results

def generate_single_reply(content: str, author: str, reply_prompt: str) -> str:
    """
    使用 AI 生成单个评论的回复
    
    :param content: 评论内容
    :param author: 评论作者
    :param reply_prompt: 回复提示
    :return: 生成的回复内容，控制在20个字符以内
    """
    # 如果评论为空，返回默认回复
    if not content or content.strip() == "":
        print("评论内容为空，返回默认回复")
        return "感谢关注！"
    
    prompt = f"""
请作为小红书博主，根据以下用户评论生成一个最多20个字符的简短回复。回复必须非常简洁，少于20个中文字符。

评论作者: {author}
评论内容: {content}
回复建议: {reply_prompt}

请直接给出回复内容，不要包含任何形式的引导或解释。回复必须控制在2-20个中文字符之内！
"""
    # 获取 OpenRouter API key
    api_key = get_openrouter_key()
    
    # 设置请求头
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    # 请求体
    data = {
        "model": "deepseek/deepseek-chat",  # 使用 Deepseek 聊天模型
        "messages": [
            {"role": "system", "content": "你是一个小红书博主，无论什么都要用中文回答，并且注重示好和语调。你的回复必须控制在2-20个字符之内。"}, 
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.7,
        "max_tokens": 50  # 限制生成的回复长度
    }
    
    # 最多尝试3次
    max_retries = 3
    retry_delay = 2  # 初始等待时间（秒）
    
    for attempt in range(max_retries):
        try:
            # 发送请求
            print(f"发送请求到 OpenRouter 生成回复 ({attempt + 1}/{max_retries})")
            response = requests.post(OPENROUTER_API_URL, headers=headers, json=data, timeout=30)
            response.raise_for_status()  # 如果请求失败，抛出异常
            
            # 解析响应
            result = response.json()["choices"][0]["message"]["content"].strip()
            
            # 确保回复在20个字符以内
            if len(result) > 20:
                print(f"生成的回复超过20个字符({len(result)})，进行截断: {result}")
                result = result[:20]
            
            return result
            
        except RequestException as e:
            # 打印错误信息
            print(f"请求失败 ({attempt + 1}/{max_retries}): {str(e)}")
            
            # 如果还有重试机会，则等待后重试
            if attempt < max_retries - 1:
                wait_time = retry_delay * (2 ** attempt)  # 指数退避策略
                print(f"将在 {wait_time} 秒后重试...")
                time.sleep(wait_time)
            else:
                print("达到最大重试次数，返回默认回复")
                return "感谢您的评论，我们非常重视您的反馈！"
        except Exception as e:
            # 捕获其他类型的异常
            print(f"生成回复时发生异常: {str(e)}")
            return "感谢您的评论！"

def save_replies_to_db(results):
    """
    将生成的回复保存到数据库的comment_reply表中
    
    :param results: 包含评论和生成的回复的列表
    :return: 成功保存的记录数
    """
    # 使用Airflow的BaseHook获取数据库连接
    db_hook = BaseHook.get_connection("xhs_db").get_hook()
    db_conn = db_hook.get_conn()
    cursor = db_conn.cursor()
    
    saved_count = 0
    errors = 0
    
    try:
        # 确保comment_reply表存在
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS comment_reply (
            id INT AUTO_INCREMENT PRIMARY KEY,
            comment_id INT NOT NULL,
            author VARCHAR(255),
            content TEXT,
            reply TEXT NOT NULL,
            note_url VARCHAR(512),
            generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_sent BOOLEAN DEFAULT FALSE,
            UNIQUE KEY unique_comment (comment_id)
        )
        """)
        db_conn.commit()
        
        # 插入或更新记录
        for result in results:
            try:
                comment_id = result.get('id')
                if not comment_id:
                    print(f"警告: 跳过没有ID的评论记录")
                    continue
                
                # 使用INSERT...ON DUPLICATE KEY UPDATE确保更新已存在的记录
                query = """
                INSERT INTO comment_reply 
                (comment_id, author, content, reply, note_url)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                author = VALUES(author),
                content = VALUES(content),
                reply = VALUES(reply),
                note_url = VALUES(note_url),
                generated_at = CURRENT_TIMESTAMP,
                is_sent = FALSE
                """
                
                # 准备参数
                params = (
                    comment_id,
                    result.get('author', ''),
                    result.get('content', ''),
                    result.get('reply', ''),
                    result.get('note_url', '')
                )
                
                # 执行插入/更新
                cursor.execute(query, params)
                saved_count += 1
                
            except Exception as e:
                print(f"保存评论ID {result.get('id', 'unknown')} 的回复时出错: {str(e)}")
                errors += 1
        
        # 提交事务
        db_conn.commit()
        print(f"成功保存 {saved_count} 条回复到comment_reply表，{errors} 条失败")
        
    except Exception as e:
        db_conn.rollback()
        print(f"数据库操作失败: {str(e)}")
        raise
    finally:
        cursor.close()
        db_conn.close()
    
    return saved_count

def run_replies_generation(**context):
    """
    Airflow任务：生成评论回复
    """
    try:
        # 从dag run配置或参数中获取回复提示
        reply_prompt = context.get('dag_run').conf.get('reply_prompt') \
            if context.get('dag_run') and context.get('dag_run').conf \
            else "友善、专业地回复，我是卖网球拍的，尽量提供有价值的信息，并邀请用户继续关注"
        
        # 从dag run配置或参数中获取评论 ID 列表
        comment_ids = context.get('dag_run').conf.get('comment_ids') \
            if context.get('dag_run') and context.get('dag_run').conf \
            else [1,2]
        
        # 获取评论数据
        if comment_ids:
            print(f"正在获取指定的 {len(comment_ids)} 条评论数据...")
        else:
            print("未指定评论 ID，将获取最新的评论数据...")
            comment_ids = None  # 确保如果传入的是空列表，也能正确处理
        
        # 限制每次处理的评论数量，防止过多
        limit = 20
        comments = get_comments_from_db(comment_ids=comment_ids, limit=limit)
        
        if not comments:
            print("没有找到符合条件的评论数据")
            return
        
        print(f"共获取到 {len(comments)} 条评论，开始生成回复...")
        
        # 生成评论回复
        results = generate_comment_replies(comments, reply_prompt)
        
        # 输出生成的回复
        print("\n===== 评论回复生成结果 =====\n")
        for result in results:
            print(f"作者: {result.get('author', '')}")
            print(f"评论: {result.get('content', '')}")
            print(f"回复: {result.get('reply', '')}")
            print("----------------------")
        
        # 将分析结果传递到下一个任务
        context['ti'].xcom_push(key='reply_results', value=results)
        context['ti'].xcom_push(key='reply_prompt', value=reply_prompt)
        
        return results
        
    except Exception as e:
        error_msg = f"生成评论回复失败: {str(e)}"
        print(error_msg)
        raise

def save_replies_to_database(**context):
    """
    Airflow任务：保存生成的回复到数据库
    """
    try:
        # 从上一个任务获取结果
        ti = context['ti']
        results = ti.xcom_pull(task_ids='generate_replies', key='reply_results')
        
        if not results:
            print("没有找到生成的回复结果，无法保存到数据库")
            return 0
        
        print(f"准备保存 {len(results)} 条回复结果到数据库...")
        
        # 保存结果到数据库
        saved_count = save_replies_to_db(results)
        
        return saved_count
        
    except Exception as e:
        error_msg = f"保存回复结果失败: {str(e)}"
        print(error_msg)
        raise

# DAG 定义
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    dag_id='xhs_reply_generator',
    default_args=default_args,
    description='为小红书评论生成回复并保存到数据库',
    schedule_interval=None,
    tags=['小红书', 'AI回复', '评论处理'],
    catchup=False,
)

# 任务1: 生成回复内容
generate_replies_task = PythonOperator(
    task_id='generate_replies',
    python_callable=run_replies_generation,
    provide_context=True,
    dag=dag,
)

# 任务2: 保存回复到数据库
save_replies_task = PythonOperator(
    task_id='save_replies',
    python_callable=save_replies_to_database,
    provide_context=True,
    dag=dag,
)

# 设置任务依赖关系
generate_replies_task >> save_replies_task