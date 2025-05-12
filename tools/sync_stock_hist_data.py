import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pandas as pd
from datetime import datetime, timedelta
import akshare as ak
import yfinance as yf
from src.data_fetcher import DataFetcher
import json
import argparse
import concurrent.futures
import logging

# 配置日志
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

def sync_one_stock_hist_data(symbol, start_date, end_date, interval='1d', query_method='yfinance'):
    """
    同步一只股票的历史数据
    """
    data_fetcher = DataFetcher()
    return data_fetcher.prepare_db_data(symbol, start_date, end_date, interval, query_method)

def sync_stocks_hist_data(symbol_list, start_date, end_date, interval='1d', query_method='yfinance', threads=1):
    """
    同步多个股票历史数据，支持多线程
    """
    # 获取所有股票代码
    with open(symbol_list, 'r', encoding='utf-8') as f:
        symbol_list = json.load(f)

    def process_symbol(symbol):
        try:
            logger.info(f"同步{symbol}: {start_date} -> {end_date}的{interval}历史数据")
            sync_one_stock_hist_data(symbol, start_date, end_date, interval, query_method)
        except Exception as e:
            import traceback
            logger.error(f"处理symbol {symbol} 时出错: {str(e)}，详细错误信息: {traceback.format_exc()}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        futures = [executor.submit(process_symbol, symbol) for symbol in symbol_list]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"处理symbol时出错: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='同步股票历史数据')
    parser.add_argument('--symbol_list', type=str, help='股票代码列表的json文件路径')
    parser.add_argument('--symbol', type=str, help='股票代码')
    parser.add_argument('--start_date', type=str, help='开始日期，格式如2024-01-01')
    parser.add_argument('--end_date', type=str, help='结束日期，格式如2024-12-31')
    parser.add_argument('--interval', type=str, default='1d', help='数据间隔，默认为1d，可选1d, 1wk, 1mo')
    parser.add_argument('--query_method', type=str, default='yfinance', help='查询方式，默认为yfinance，可选akshare, yfinance')
    parser.add_argument('--threads', type=int, default=1, help='线程数，默认为1')
    args = parser.parse_args()

    if not args.symbol and not args.symbol_list:
        parser.error('必须指定 --symbol 或 --symbol_list 至少一个参数')

    if args.start_date is None:
        args.start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    if args.end_date is None:
        args.end_date = datetime.now().strftime('%Y-%m-%d') 

    if args.symbol_list:
        sync_stocks_hist_data(args.symbol_list, args.start_date, args.end_date, args.interval, args.query_method, args.threads)
    else:
        sync_one_stock_hist_data(args.symbol, args.start_date, args.end_date, args.interval, args.query_method)
