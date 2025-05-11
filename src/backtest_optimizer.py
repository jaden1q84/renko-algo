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
        
        # 配置日志
        logging.basicConfig(level=logging.INFO,
                          format='%(asctime)s - %(levelname)s - %(message)s',
                          datefmt='%Y-%m-%d %H:%M:%S')
        self.logger = logging.getLogger(__name__)

        if self.args.threads is not None:
            self.logger.info(f"**********使用--threads {self.args.threads}个线程**********")
            self.config.max_threads = self.args.threads
        if self.args.max_iterations is not None:
            self.logger.info(f"**********使用--max_iterations {self.args.max_iterations}次迭代**********")
            self.config.max_iterations = self.args.max_iterations

        # 打印当前的配置参数
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
        
        # 从配置中获取参数范围
        atr_periods = self.config.atr_periods
        atr_multipliers = self.config.atr_multipliers
        trend_lengths = self.config.trend_lengths
        
        # 创建任务列表
        tasks = []
        
        # 添加daily模式任务
        for buy_length in trend_lengths:
            for sell_length in trend_lengths:
                tasks.append(('daily', None, None, buy_length, sell_length))
        
        # 添加atr模式任务
        for period in atr_periods:
            for multiplier in atr_multipliers:
                for buy_length in trend_lengths:
                    for sell_length in trend_lengths:
                        tasks.append(('atr', period, multiplier, buy_length, sell_length))
        
        # 限制任务数量
        tasks = tasks[:self.config.max_iterations]
        
        # 使用线程池执行任务
        with ThreadPoolExecutor(max_threads=self.config.max_threads) as executor:
            futures = []
            for task in tasks:
                mode, period, multiplier, buy_length, sell_length = task
                if mode == 'daily':
                    future = executor.submit(self._test_daily_mode, buy_length, sell_length)
                else:
                    future = executor.submit(self._test_atr_mode, period, multiplier, buy_length, sell_length)
                futures.append(future)
            
            # 等待所有任务完成
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"任务执行失败: {str(e)}")
        
        # 输出优化结果
        self._print_optimization_results()
        
    def _test_daily_mode(self, buy_trend_length: int, sell_trend_length: int):
        """测试daily模式"""
        self.logger.info(f"测试daily模式 - 买入趋势长度: {buy_trend_length}, 卖出趋势长度: {sell_trend_length}")
        
        # 生成砖型图
        renko_gen = RenkoGenerator(mode='daily', symbol=self.args.symbol, 
                                   save_data=getattr(self.args, 'save_data', False))
        renko_data = renko_gen.generate_renko(self.data)
        
        # 运行策略
        strategy = RenkoStrategy(buy_trend_length=buy_trend_length, 
                               sell_trend_length=sell_trend_length,
                               symbol=self.args.symbol,
                               save_data=getattr(self.args, 'save_data', False))
        signals = strategy.calculate_signals(renko_data)
        portfolio = strategy.backtest(renko_data, signals, self.initial_capital)
        
        # 计算收益率
        final_return = (portfolio.iloc[-1]['total'] - self.initial_capital) / self.initial_capital
        
        # 使用锁来安全地添加结果
        with self.results_lock:
            self.results.append({
                'symbol': self.args.symbol,
                'start_date': self.args.start_date,
                'end_date': self.args.end_date,
                'mode': 'daily',
                'atr_period': None,
                'atr_multiplier': None,
                'buy_trend_length': buy_trend_length,
                'sell_trend_length': sell_trend_length,
                'return': final_return,
                'portfolio': portfolio,
                'brick_size': 0,
                'last_signal': signals.iloc[-1].signal,
                'last_signal_date': signals.iloc[-1].date.strftime('%Y-%m-%d'),
                'last_price': renko_data.iloc[-1].close
            })
        
    def _test_atr_mode(self, atr_period: int, atr_multiplier: float, buy_trend_length: int, sell_trend_length: int):
        """测试ATR模式"""
        self.logger.info(f"=========测试ATR模式 - 周期: {atr_period}, 倍数: {atr_multiplier}, "
                        f"买入趋势长度: {buy_trend_length}, 卖出趋势长度: {sell_trend_length}=========")
        
        # 生成砖型图
        renko_gen = RenkoGenerator(mode='atr', atr_period=atr_period, atr_multiplier=atr_multiplier,
                                 symbol=self.args.symbol, save_data=getattr(self.args, 'save_renko_data', False))
        renko_data = renko_gen.generate_renko(self.data)
        brick_size = renko_gen.get_brick_size()
        
        # 运行策略
        strategy = RenkoStrategy(buy_trend_length=buy_trend_length, 
                               sell_trend_length=sell_trend_length,
                               symbol=self.args.symbol,
                               save_data=getattr(self.args, 'save_data', False))
        signals = strategy.calculate_signals(renko_data)
        portfolio = strategy.backtest(renko_data, signals, self.initial_capital)
        
        # 计算收益率
        final_return = (portfolio.iloc[-1]['total'] - self.initial_capital) / self.initial_capital
        
        # 使用锁来安全地添加结果
        with self.results_lock:
            self.results.append({
                'symbol': self.args.symbol,
                'mode': 'atr',
                'atr_period': atr_period,
                'atr_multiplier': atr_multiplier,
                'buy_trend_length': buy_trend_length,
                'sell_trend_length': sell_trend_length,
                'start_date': self.args.start_date,
                'end_date': self.args.end_date,
                'return': final_return,
                'renko_data': renko_data,
                'portfolio': portfolio,
                'signals': signals,
                'brick_size': brick_size,
                'last_signal': signals.iloc[-1].signal,
                'last_signal_date': signals.iloc[-1].date.strftime('%Y-%m-%d'),
                'last_price': renko_data.iloc[-1].close
            })
        
    def _print_optimization_results(self):
        """输出优化结果"""
        self.logger.info("优化结果汇总:")
        self.logger.info("-" * 80)
        
        # 按收益率排序
        sorted_results = sorted(self.results, key=lambda x: x['return'], reverse=False)
        
        for i, result in enumerate(sorted_results, 1):
            mode_str = f"模式: {result['mode']}"
            if result['mode'] == 'atr':
                mode_str += f", ATR周期: {result['atr_period']}, ATR倍数: {result['atr_multiplier']}"
            mode_str += f", 买入趋势长度: {result['buy_trend_length']}, 卖出趋势长度: {result['sell_trend_length']}"
                
            self.logger.info(f"{i}. {mode_str}")
            self.logger.info(f"收益率: {result['return']:.2%}")
            
        # 输出最优参数
        self.best_result = sorted_results[-1]
        self.logger.info("========================最优参数组合=========================")
        self.logger.info(f"股票代码: {self.best_result['symbol']}")
        self.logger.info(f"模式: {self.best_result['mode']}")
        if self.best_result['mode'] == 'atr':
            self.logger.info(f"ATR周期: {self.best_result['atr_period']}")
            self.logger.info(f"ATR倍数: {self.best_result['atr_multiplier']}")
        self.logger.info(f"买入趋势长度: {self.best_result['buy_trend_length']}")
        self.logger.info(f"卖出趋势长度: {self.best_result['sell_trend_length']}")
        self.logger.info(f"收益率: {self.best_result['return']:.2%}")
        self.logger.info(f"砖型大小: {self.best_result['brick_size']:.2f}")
        self.logger.info(f"最后信号: {self.best_result['last_signal']}")
        self.logger.info(f"最后信号日期: {self.best_result['last_signal_date']}")
        self.logger.info(f"最后信号价格: {self.best_result['last_price']:.2f}")
        self.logger.info("===========================================================")

    def get_best_result(self) -> Dict:
        """获取最优参数组合"""
        if self.best_result == None:
            sorted_results = sorted(self.results, key=lambda x: x['return'], reverse=False)
            self.best_result = sorted_results[-1]
        return self.best_result 