import argparse
from datetime import datetime, timedelta
from renko_backtester import RenkoBacktester
from data_fetcher import DataFetcher
import json
import os
import concurrent.futures
from config import RenkoConfig
import threading

def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='Renko策略回测程序')
    parser.add_argument('--symbol', required=False, help='股票代码，例如：688041')
    parser.add_argument('--start_date', default=None, help='开始日期，格式：YYYY-MM-DD')
    parser.add_argument('--end_date', default=None, help='结束日期，格式：YYYY-MM-DD')
    parser.add_argument('--renko_mode', choices=['atr', 'daily'], default='atr', 
                       help='Renko生成模式：atr（基于ATR）或daily（基于日线），默认atr')
    parser.add_argument('--atr_period', type=int, default=10, help='ATR周期（仅当renko_mode=atr时有效）')
    parser.add_argument('--atr_multiplier', type=float, default=0.5, help='ATR乘数（仅当renko_mode=atr时有效）')
    parser.add_argument('--buy_trend_length', type=int, default=3, help='买入信号所需的趋势长度')
    parser.add_argument('--sell_trend_length', type=int, default=3, help='卖出信号所需的趋势长度')
    parser.add_argument('--optimize', action='store_true', help='是否进行参数优化')
    parser.add_argument('--max_iterations', type=int, default=None, help='最大优化迭代次数')
    parser.add_argument('--brick_size', type=float, default=None, help='砖块颗粒度')
    parser.add_argument('--threads', type=int, default=None, help='多线程数量')
    parser.add_argument('--save_data', action='store_true', help='是否保存中间Renko、portfolio等中间数据文件，默认不保存')
    parser.add_argument('--symbol_list', default=None, help='股票代码列表配置文件（JSON数组），如config/symbol_list.json')
    args = parser.parse_args()
    if not args.symbol and not args.symbol_list:
        parser.error('必须指定 --symbol 或 --symbol_list 至少一个参数')
    if args.start_date is None:
        args.start_date = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
    if args.end_date is None:
        args.end_date = datetime.now().strftime('%Y-%m-%d')
    return args

def main():
    """主函数"""
    args = parse_arguments()
    config = RenkoConfig()
    data_fetcher = DataFetcher(use_db_cache=config.use_db_cache, use_csv_cache=config.use_csv_cache, query_method=config.query_method)
    data_fetcher.init_stock_info()
    list_best_results = []
    list_best_results_lock = threading.Lock()

    if args.symbol_list:
        # 读取symbol_list.json
        with open(args.symbol_list, 'r', encoding='utf-8') as f:
            symbol_list = json.load(f)

        # 并发回测
        def run_for_symbol(symbol):
            import copy
            args_copy = copy.deepcopy(args)
            args_copy.symbol = symbol
            args_copy.optimize = True
            backtester = RenkoBacktester(args_copy, data_fetcher)
            best_result = backtester.run_backtest()
            backtester.plot_results()
            # append要加锁
            with list_best_results_lock:
                list_best_results.append(best_result)
        max_workers = args.threads if args.threads else min(4, len(symbol_list))
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            executor.map(run_for_symbol, symbol_list)
    else:
        backtester = RenkoBacktester(args, data_fetcher)
        backtester.run_backtest()
        backtester.plot_results()
if __name__ == "__main__":
    main() 