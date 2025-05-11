import logging
import os
from datetime import datetime

def setup_logger(enable_console=True):
    """
    配置日志记录器
    
    Args:
        console_log (bool): 是否启用控制台日志
    """
    # 确保logs目录存在
    os.makedirs('logs', exist_ok=True)
    
    # 获取根日志记录器
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # 清除现有的处理器
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # 创建格式化器
    formatter = logging.Formatter('%(asctime)s - %(processName)s - %(levelname)s - %(message)s',
                                datefmt='%Y-%m-%d %H:%M:%S')
    
    # 创建文件处理器
    file_handler = logging.FileHandler(f"logs/batch_log_{datetime.now().strftime('%Y-%m-%d')}.log")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # # 如果不是批量处理模式，添加控制台处理器
    if enable_console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler) 
