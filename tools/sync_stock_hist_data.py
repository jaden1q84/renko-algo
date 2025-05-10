import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pandas as pd
from datetime import datetime, timedelta
import akshare as ak
import yfinance as yf
from src.database import DataBase
import json
import argparse
import concurrent.futures

QUERY_METHOD = 'yfinance'
STOCK_HIST_DATA_DB = DataBase('data/stock_hist_data.db')

def get_db_first_date(symbol, interval='1d'):
    """
    获取第一条数据日期
    """
    db_first_date = STOCK_HIST_DATA_DB.get_first_date(symbol, interval)
    if db_first_date is None:
        return None
    else:
        return pd.to_datetime(db_first_date)

def get_db_last_date(symbol, interval='1d'):
    """
    检查最后一条数据日期
    """
    db_last_date = STOCK_HIST_DATA_DB.get_last_date(symbol, interval)
    if db_last_date is None:
        return None
    else:
        return pd.to_datetime(db_last_date)

def get_one_stock_hist_data(symbol, start_date, end_date, interval='1d', query_method=QUERY_METHOD):
    """
    获取一只股票的历史数据
    """
    if pd.to_datetime(start_date) > pd.to_datetime(end_date):
        raise ValueError(f"开始日期{start_date}不能大于结束日期{end_date}")

    query_df = pd.DataFrame()
    db_start_date = get_db_first_date(symbol, interval)
    db_end_date = get_db_last_date(symbol, interval)

    if db_start_date is not None and db_end_date is not None:
        print(f"股票{symbol}已有历史数据: {db_start_date} -> {db_end_date}")
        if db_start_date > pd.to_datetime(end_date):
            end_date = (db_start_date - timedelta(days=1)).strftime('%Y-%m-%d') # ok
        elif db_end_date < pd.to_datetime(start_date):
            start_date = (db_end_date + timedelta(days=1)).strftime('%Y-%m-%d') # ok
        elif pd.to_datetime(start_date) < db_start_date and db_start_date < pd.to_datetime(end_date):
            end_date = (db_start_date - timedelta(days=1)).strftime('%Y-%m-%d')
        elif pd.to_datetime(start_date) < db_end_date and db_end_date < pd.to_datetime(end_date):
            start_date = (db_end_date + timedelta(days=1)).strftime('%Y-%m-%d')
        elif pd.to_datetime(start_date) >= db_start_date and db_end_date >= pd.to_datetime(end_date):
            return query_df
        print(f"[TODO]股票{symbol}的历史数据: {start_date} -> {end_date}")
    else:
        print(f"股票{symbol}没有历史数据，从{start_date}到{end_date}获取数据")

    # interval到period的映射
    interval_map = {'1d': 'daily', '1wk': 'weekly', '1mo': 'monthly'}
    if interval not in interval_map:
        raise ValueError(f"A/H股仅支持interval为'1d', '1wk', '1mo'，收到: {interval}")
    period = interval_map[interval]  
    
    query_start_date = start_date
    query_end_date = (pd.to_datetime(end_date) + timedelta(days=1)).strftime('%Y-%m-%d')
    print(f"\n*************使用{query_method}获取{symbol}: {query_start_date} -> {query_end_date}的数据")

    # 获取历史数据
    if query_method == 'akshare':
        if symbol.endswith('.HK'):
            query_df = ak.stock_hk_hist(symbol=symbol.split('.')[0], period=period, 
                                start_date=query_start_date.replace('-', ''), end_date=query_end_date.replace('-', ''), adjust="qfq")
        else:
            query_df = ak.stock_zh_a_hist(symbol=symbol.split('.')[0], period=period, 
                                    start_date=query_start_date.replace('-', ''), end_date=query_end_date.replace('-', ''), adjust="qfq")
        # 字段兼容
        query_df.rename(columns={
            '日期': 'Date',
            '开盘': 'Open',
            '收盘': 'Close',
            '最高': 'High',
            '最低': 'Low',
            '成交量': 'Volume',
            '成交额': 'Turnover',
        }, inplace=True)
    
    elif query_method == 'yfinance':
        # 如果symbol不是.HK，则认为是A股，6开头需要补.SS结尾，否则补.SZ结尾
        yf_symbol = symbol
        if not symbol.endswith('.HK'):
            if symbol.startswith('6'):
                yf_symbol = f"{symbol}.SS"
            else:
                yf_symbol = f"{symbol}.SZ"
        else:
            # 港股减掉第1个数字
            yf_symbol = symbol[1:]

        ticker = yf.Ticker(yf_symbol)
        query_df = ticker.history(start=query_start_date, end=query_end_date, interval=interval)
    else:
        raise ValueError(f"不支持的query_method: {query_method}")
    
    if not query_df.empty:
        if 'Date' not in query_df.columns:
            query_df = query_df.reset_index()

        # 保持DB兼容性，添加 Turnover 列，默认值为 -1
        if 'Turnover' not in query_df.columns:
            query_df['Turnover'] = -1

        # Date列去掉时区，只保留日期
        query_df['Date'] = query_df['Date'].dt.date
        query_df['Date'] = pd.to_datetime(query_df['Date'])
        query_df.set_index('Date', inplace=True)

        print(f"获取了 {len(query_df)} 条数据")

    return query_df

def sync_stock_hist_data(symbol_list, start_date, end_date, interval='1d', query_method=QUERY_METHOD, threads=1):
    """
    同步股票历史数据，支持多线程
    """
    # 获取所有股票代码
    with open(symbol_list, 'r', encoding='utf-8') as f:
        symbol_list = json.load(f)

    def process_symbol(symbol):
        print(f"同步{symbol}: {start_date} -> {end_date}的{interval}历史数据")
        query_df = get_one_stock_hist_data(symbol, start_date, end_date, interval, query_method)
        if not query_df.empty:
            STOCK_HIST_DATA_DB.insert(symbol, query_df, interval)

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        futures = [executor.submit(process_symbol, symbol) for symbol in symbol_list]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"处理symbol时出错: {e}")

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
        sync_stock_hist_data(args.symbol_list, args.start_date, args.end_date, args.interval, args.query_method, args.threads)
    else:
        query_df = get_one_stock_hist_data(args.symbol, args.start_date, args.end_date, args.interval, args.query_method)
        if not query_df.empty:
            STOCK_HIST_DATA_DB.insert(args.symbol, query_df, args.interval)
