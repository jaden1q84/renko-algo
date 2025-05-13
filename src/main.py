import argparse
import json
import os
import glob
import copy
import logging
import multiprocessing
import sys
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor, wait
from config import RenkoConfig
from renko_backtester import RenkoBacktester
from data_fetcher import DataFetcher
from logger_config import setup_logger

# 确保logs目录存在
os.makedirs('logs', exist_ok=True)

logger = logging.getLogger(__name__)

def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='Renko策略回测程序')
    parser.add_argument('--symbol', required=False, help='股票代码，例如：688041')
    parser.add_argument('--start_date', default=None, help='开始日期，格式：YYYY-MM-DD')
    parser.add_argument('--end_date', default=None, help='结束日期，格式：YYYY-MM-DD')
    parser.add_argument('--renko_mode', choices=['atr', 'daily'], default='atr', help='Renko生成模式：atr（基于ATR）或daily（基于日线），默认atr')
    parser.add_argument('--atr_period', type=int, default=10, help='ATR周期（仅当renko_mode=atr时有效）')
    parser.add_argument('--atr_multiplier', type=float, default=0.5, help='ATR乘数（仅当renko_mode=atr时有效）')
    parser.add_argument('--buy_trend_length', type=int, default=3, help='买入信号所需的趋势长度')
    parser.add_argument('--sell_trend_length', type=int, default=3, help='卖出信号所需的趋势长度')
    parser.add_argument('--optimize', action='store_true', help='是否进行参数优化')
    parser.add_argument('--max_iterations', type=int, default=None, help='最大优化迭代次数')
    parser.add_argument('--brick_size', type=float, default=None, help='砖块颗粒度')
    parser.add_argument('--threads', type=int, default=None, help='每个进程的多线程数量')
    parser.add_argument('--workers', type=int, default=1, help='多进程数量，默认1')
    parser.add_argument('--save_data', action='store_true', help='是否保存中间Renko、portfolio等中间数据文件，默认不保存')
    parser.add_argument('--symbol_list', default=None, help='股票代码列表配置文件（JSON数组），如config/symbol_list.json')
    parser.add_argument('--replace', action='store_true', help='是否替换已存在的文件，默认不替换')

    args = parser.parse_args()
    if not args.symbol and not args.symbol_list:
        parser.error('必须指定 --symbol 或 --symbol_list 至少一个参数')
    if args.start_date is None:
        args.start_date = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
    if args.end_date is None:
        args.end_date = datetime.now().strftime('%Y-%m-%d')
    return args

def check_result_file(symbol, args):
    """检查结果文件是否存在"""
    if args.replace:
        return False
    logger.info(f"[CHECK]检查结果文件是否存在: {symbol} {args.start_date} {args.end_date}")
    output_dir = f"results/{datetime.now().strftime('%Y-%m-%d')}"
    pattern = f"{output_dir}/*{symbol}*{args.start_date}*{args.end_date}.png"
    matching_files = glob.glob(pattern)
    if matching_files:
        logger.info(f"[SKIP]结果文件已存在: {' , '.join(matching_files)}")
        return True
    return False

def run_single_backtest(symbol, args, batch_mode=False):
    """单只股票回测流程"""
    try:
        # 日志配置：先输出第1条日志，然后批量模式下不输出到控制台
        setup_logger()
        logger.info(f"[START]开始回测股票 {symbol}")

        args_copy = copy.deepcopy(args)
        args_copy.symbol = symbol

        if check_result_file(symbol, args_copy):
            return
        
        # 正式关闭批量处理的控制台日志
        setup_logger(not batch_mode)

        config = RenkoConfig()
        data_fetcher = DataFetcher(use_db_cache=config.use_db_cache, use_csv_cache=config.use_csv_cache, query_method=config.query_method)
        data_fetcher.init_stock_info()
        logger.info("数据获取器初始化完成")
        data_fetcher.prepare_db_data(args_copy.symbol, args_copy.start_date, args_copy.end_date)

        backtester = RenkoBacktester(args_copy, data_fetcher)
        backtester.run_backtest()
        result_path = backtester.plot_results()
        
        # 打开批量处理的控制台日志，输出结果
        setup_logger()
        logger.info(f"[DONE]完成回测股票 {symbol}。结果保存到: {result_path}")
    except Exception as e:
        logger.error(f"处理股票 {symbol} 时发生错误: {str(e)}", exc_info=True)
        raise

def run_batch_backtest(symbol_list, args):
    """批量回测流程"""
    max_workers = args.workers
    logger.info(f"使用 {max_workers} 个进程进行处理")
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(run_single_backtest, symbol, args, True) for symbol in symbol_list]
        wait(futures)
    logger.info("所有股票处理完成")

def main():
    try:
        args = parse_arguments()
        setup_logger()  # 主进程日志
        logger.info(f"程序启动，参数: {args}")

        if args.symbol_list:
            logger.info(f"开始读取股票列表文件: {args.symbol_list}")
            with open(args.symbol_list, 'r', encoding='utf-8') as f:
                symbol_list = json.load(f)
            logger.info(f"成功读取 {len(symbol_list)} 个股票代码")
            run_batch_backtest(symbol_list, args)
        else:
            run_single_backtest(args.symbol, args)
    except Exception as e:
        logger.error(f"程序执行过程中发生错误: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    multiprocessing.set_start_method('spawn')
    main() 