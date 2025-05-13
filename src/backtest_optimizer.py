import pandas as pd
import numpy as np
from typing import Dict, Tuple, List
import logging
from renko_generator import RenkoGenerator
from strategy import RenkoStrategy
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from config import RenkoConfig

class BacktestOptimizer:
    def __init__(self, data: pd.DataFrame, args: dict, config_path: str = "config/config.yaml"):
        """
        初始化回测优化器
        
        Args:
            data (pd.DataFrame): 原始K线数据
            args (dict): 参数
            config_path (str): 配置文件路径
        """
        self.data = data
        self.args = args
        self.config = RenkoConfig(config_path)
        self.initial_capital = self.config.initial_capital
        self.results = []
        self.results_lock = threading.Lock()  # 用于线程安全的锁
        self.best_result = None
        self.logger = logging.getLogger(__name__)
        self._apply_args_to_config()
        self._log_config()

    def _apply_args_to_config(self):
        if self.args.threads is not None:
            self.logger.info(f"**********使用--threads {self.args.threads}个线程**********")
            self.config.max_threads = self.args.threads
        if self.args.max_iterations is not None:
            self.logger.info(f"**********使用--max_iterations {self.args.max_iterations}次迭代**********")
            self.config.max_iterations = self.args.max_iterations

    def _log_config(self):
        self.logger.info("**********************本次优化器配置参数**********************")
        self.logger.info(f"初始资金: {self.initial_capital}")
        self.logger.info(f"最大迭代次数: {self.config.max_iterations}")
        self.logger.info(f"最大线程数: {self.config.max_threads}")
        self.logger.info(f"ATR周期选项: {self.config.atr_periods}")
        self.logger.info(f"ATR倍数选项: {self.config.atr_multipliers}")
        self.logger.info(f"趋势长度选项: {self.config.trend_lengths}")
        self.logger.info("**************************************************************")

    def run_optimization(self):
        """
        运行参数优化
        """
        self.logger.info("开始参数优化...")
        tasks = self._generate_tasks()
        tasks = tasks[:self.config.max_iterations]
        self._execute_tasks(tasks)
        self._print_optimization_results()
        
    def _generate_tasks(self) -> List[Tuple]:
        """生成所有参数组合任务"""
        tasks = []
        # daily模式
        for buy_length in self.config.trend_lengths:
            for sell_length in self.config.trend_lengths:
                tasks.append(('daily', None, None, buy_length, sell_length))
        # atr模式
        for period in self.config.atr_periods:
            for multiplier in self.config.atr_multipliers:
                for buy_length in self.config.trend_lengths:
                    for sell_length in self.config.trend_lengths:
                        tasks.append(('atr', period, multiplier, buy_length, sell_length))
        return tasks

    def _execute_tasks(self, tasks: List[Tuple]):
        """多线程执行所有参数组合任务"""
        with ThreadPoolExecutor(max_workers=self.config.max_threads) as executor:
            futures = []
            for task in tasks:
                mode, period, multiplier, buy_length, sell_length = task
                if mode == 'daily':
                    future = executor.submit(self._run_single_test, mode, None, None, buy_length, sell_length)
                else:
                    future = executor.submit(self._run_single_test, mode, period, multiplier, buy_length, sell_length)
                futures.append(future)
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"任务执行失败: {str(e)}")

    def _run_single_test(self, mode, period, multiplier, buy_length, sell_length):
        """统一处理单个参数组合的回测逻辑"""
        if mode == 'daily':
            self.logger.info(f"测试daily模式 - 买入趋势长度: {buy_length}, 卖出趋势长度: {sell_length}")
            renko_gen = RenkoGenerator(mode='daily', symbol=self.args.symbol, save_data=getattr(self.args, 'save_data', False))
            renko_data = renko_gen.generate_renko(self.data)
            brick_size = None
        else:
            self.logger.info(f"=========测试ATR模式 - 周期: {period}, 倍数: {multiplier}, 买入趋势长度: {buy_length}, 卖出趋势长度: {sell_length}=========")
            renko_gen = RenkoGenerator(mode='atr', atr_period=period, atr_multiplier=multiplier, symbol=self.args.symbol, save_data=getattr(self.args, 'save_renko_data', False))
            renko_data = renko_gen.generate_renko(self.data)
            brick_size = renko_gen.get_brick_size() if not renko_data.empty else None
        if renko_data.empty:
            self.logger.warning("砖型图数据为空，跳过此参数组合")
            return
        strategy = RenkoStrategy(buy_trend_length=buy_length, sell_trend_length=sell_length, symbol=self.args.symbol, save_data=getattr(self.args, 'save_data', False))
        signals = strategy.calculate_signals(renko_data)
        portfolio = strategy.backtest(renko_data, signals, self.initial_capital)
        final_return = (portfolio.iloc[-1]['total'] - self.initial_capital) / self.initial_capital
        result = {
            'symbol': self.args.symbol,
            'start_date': self.args.start_date,
            'end_date': self.args.end_date,
            'mode': mode,
            'atr_period': period,
            'atr_multiplier': multiplier,
            'buy_trend_length': buy_length,
            'sell_trend_length': sell_length,
            'return': final_return,
            'portfolio': portfolio,
            'brick_size': brick_size,
            'last_signal': signals.iloc[-1].signal,
            'last_signal_date': signals.iloc[-1].date.strftime('%Y-%m-%d'),
            'last_price': renko_data.iloc[-1].close
        }
        if mode == 'atr':
            result['renko_data'] = renko_data
            result['signals'] = signals
        with self.results_lock:
            self.results.append(result)

    def _print_optimization_results(self):
        """输出优化结果"""
        self.logger.info("优化结果汇总:")
        self.logger.info("-" * 80)
        sorted_results = self._get_sorted_results()
        for i, result in enumerate(sorted_results, 1):
            mode_str = f"模式: {result['mode']}"
            if result['mode'] == 'atr':
                mode_str += f", ATR周期: {result['atr_period']}, ATR倍数: {result['atr_multiplier']}"
            mode_str += f", 买入趋势长度: {result['buy_trend_length']}, 卖出趋势长度: {result['sell_trend_length']}"
            self.logger.info(f"{i}. {mode_str}")
            self.logger.info(f"收益率: {result['return']:.2%}")
        self.best_result = sorted_results[-1]
        self._log_best_result()

    def _get_sorted_results(self):
        return sorted(self.results, key=lambda x: x['return'], reverse=False)

    def _log_best_result(self):
        r = self.best_result
        self.logger.info("========================最优参数组合=========================")
        self.logger.info(f"股票代码: {r['symbol']}")
        self.logger.info(f"模式: {r['mode']}")
        self.logger.info(f"买入趋势长度: {r['buy_trend_length']}")
        self.logger.info(f"卖出趋势长度: {r['sell_trend_length']}")
        self.logger.info(f"收益率: {r['return']:.2%}")
        self.logger.info(f"最后信号: {r['last_signal']}")
        self.logger.info(f"最后信号日期: {r['last_signal_date']}")
        self.logger.info(f"最后信号价格: {r['last_price']:.2f}")
        if r['mode'] == 'atr':
            self.logger.info(f"ATR周期: {r['atr_period']}")
            self.logger.info(f"ATR倍数: {r['atr_multiplier']}")
            self.logger.info(f"砖型大小: {r['brick_size']:.2f}")
        params_str = f"--symbol {r['symbol']} --start_date {r['start_date']} --end_date {r['end_date']} " + \
                    f"--renko_mode {r['mode']} --atr_period {r['atr_period']} --atr_multiplier {r['atr_multiplier']} " + \
                    f"--buy_trend_length {r['buy_trend_length']} --sell_trend_length {r['sell_trend_length']} "
        if r['brick_size'] is not None:
            params_str += f"--brick_size {r['brick_size']:.2f}"
        self.logger.info(f"运行参数: {params_str}")
        self.logger.info("===========================================================")

    def get_best_result(self) -> Dict:
        """获取最优参数组合"""
        if self.best_result is None:
            sorted_results = self._get_sorted_results()
            self.best_result = sorted_results[-1]
        return self.best_result 