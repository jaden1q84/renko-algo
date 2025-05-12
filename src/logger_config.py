import logging
import os
from datetime import datetime
from typing import Optional

def create_formatter() -> logging.Formatter:
    """
    创建日志格式化器
    
    Returns:
        logging.Formatter: 配置好的日志格式化器
    """
    return logging.Formatter(
        '%(asctime)s - %(processName)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def setup_file_handler(formatter: logging.Formatter) -> logging.FileHandler:
    """
    设置文件日志处理器
    
    Args:
        formatter (logging.Formatter): 日志格式化器
    
    Returns:
        logging.FileHandler: 配置好的文件处理器
    """
    os.makedirs('logs', exist_ok=True)
    file_handler = logging.FileHandler(
        f"logs/batch_log_{datetime.now().strftime('%Y-%m-%d')}.log"
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    return file_handler

def setup_console_handler(formatter: logging.Formatter) -> logging.StreamHandler:
    """
    设置控制台日志处理器
    
    Args:
        formatter (logging.Formatter): 日志格式化器
    
    Returns:
        logging.StreamHandler: 配置好的控制台处理器
    """
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    return console_handler

def clear_existing_handlers(logger: logging.Logger) -> None:
    """
    清除日志记录器现有的所有处理器
    
    Args:
        logger (logging.Logger): 要清理的日志记录器
    """
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

def setup_logger(enable_console: bool = True) -> logging.Logger:
    """
    配置并返回根日志记录器
    
    Args:
        enable_console (bool, optional): 是否启用控制台日志输出. 默认为 True.
    
    Returns:
        logging.Logger: 配置好的根日志记录器
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # 清除现有处理器
    clear_existing_handlers(logger)
    
    # 创建并配置格式化器
    formatter = create_formatter()
    
    # 添加文件处理器
    logger.addHandler(setup_file_handler(formatter))
    
    # 如果启用控制台输出，添加控制台处理器
    if enable_console:
        logger.addHandler(setup_console_handler(formatter))
    
    return logger 
