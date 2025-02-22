#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Optimized Webhook Server using FastAPI
======================================

功能：
1. 接收GitHub的webhook请求，自动更新代码

使用方法：
1. 安装依赖:
   pip install -r requirements.txt

2. 创建 .env 文件并配置环境变量:
   AIRFLOW_BASE_URL=<Your Airflow Base URL>
   AIRFLOW_USERNAME=<Your Airflow Username>
   AIRFLOW_PASSWORD=<Your Airflow Password>
   RATE_LIMIT_UPDATE=<Rate limit for /update endpoint, e.g., "10/minute">
   RATE_LIMIT_WCF=<Rate limit for /wcf_callback endpoint, e.g., "100/minute">

3. 运行服务器:
   使用 Uvicorn 启动:
   uvicorn webhook_server:app --host 0.0.0.0 --port 5000 --workers 2

   或者使用 Gunicorn 提高并发性能:
   gunicorn webhook_server:app -w 2 -k uvicorn.workers.UvicornWorker -b 0.0.0.0:5000

   或者后台运行 Gunicorn:
   nohup gunicorn webhook_server:app -w 2 -k uvicorn.workers.UvicornWorker -b 0.0.0.0:5000 > webhook.log 2>&1 &
"""

import os
import re
import subprocess
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import time

import asyncio
import httpx
from fastapi import FastAPI, Request, status
from fastapi.responses import PlainTextResponse, JSONResponse
from dotenv import load_dotenv

# =====================
# Configuration
# =====================

# Load environment variables from .env file
load_dotenv()

# Repository Path
REPO_PATH = os.path.dirname(os.path.abspath(__file__))

# 设置时区为中国时区
os.environ['TZ'] = 'Asia/Shanghai'
try:
    time.tzset()
except AttributeError:
    # Windows 不支持 time.tzset()
    pass

# =====================
# Logging Configuration
# =====================

def setup_logging():
    logger = logging.getLogger("webhook_server")
    logger.setLevel(logging.INFO)

    # 将日志输出到标准输出，这样可以通过docker logs查看
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(name)s - %(message)s'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

logger = setup_logging()

# =====================
# FastAPI App Initialization
# =====================

app = FastAPI(title="Optimized Webhook Server")

# =====================
# Routes
# =====================

@app.post("/update", response_class=PlainTextResponse)
async def update_code(request: Request):
    """
    处理GitHub webhook请求，执行代码更新
    """
    try:
        logger.info('接收到GitHub webhook请求')
        
        # 使用loop.run_in_executor替代asyncio.to_thread
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, execute_git_commands)

        logger.info('代码更新成功')
        return PlainTextResponse(content="更新成功", status_code=status.HTTP_200_OK)
    except subprocess.CalledProcessError as git_error:
        logger.error(f'Git命令执行失败: {git_error}')
        return PlainTextResponse(content="Git命令执行失败", status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
    except Exception as e:
        logger.error(f'代码更新失败: {e}')
        return PlainTextResponse(content="更新失败", status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

@app.get("/health")
async def health_check():
    """
    健康检查端点
    """
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"status": "healthy", "timestamp": datetime.now().isoformat()}
    )

# =====================
# Helper Functions
# =====================

def execute_git_commands():
    """
    执行git fetch和git reset命令以更新代码
    """
    try:
        logger.info('开始执行git fetch --all')
        subprocess.run(['git', 'fetch', '--all'], check=True, cwd=REPO_PATH)
        logger.info('git fetch --all 成功')

        logger.info('开始执行git reset --hard origin/main')
        subprocess.run(['git', 'reset', '--hard', 'origin/main'], check=True, cwd=REPO_PATH)
        logger.info('git reset --hard origin/main 成功')
    except subprocess.CalledProcessError as e:
        logger.error(f'Git命令执行失败: {e}')
        raise

# =====================
# Global Exception Handlers
# =====================

@app.exception_handler(404)
async def not_found_handler(request: Request, exc):
    logger.warning(f'未找到的路由: {request.url.path}')
    return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content={"error": "Not found"})

@app.exception_handler(500)
async def internal_error_handler(request: Request, exc):
    logger.error(f'服务器内部错误: {exc}')
    return JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content={"error": "Internal server error"})


# =====================
# Main Entry
# =====================

# Note: For production, use a WSGI server like Uvicorn or Gunicorn to handle concurrency