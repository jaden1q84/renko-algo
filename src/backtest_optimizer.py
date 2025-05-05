import pandas as pd
import numpy as np
from typing import Dict, Tuple, List
import logging
from renko_generator import RenkoGenerator
from strategy import RenkoStrategy
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

class BacktestOptimizer:
    def __init__(self, data: pd.DataFrame, initial_capital: float = 1000000):
        """
        初始化回测优化器
        
        Args:
            data (pd.DataFrame): 原始K线数据
            initial_capital (float): 初始资金
        """
        self.data = data
        self.initial_capital = initial_capital
        self.results = []
        self.results_lock = threading.Lock()  # 用于线程安全的锁
        
        # 配置日志
        logging.basicConfig(level=logging.INFO,
                          format='%(asctime)s - %(levelname)s - %(message)s',
                          datefmt='%Y-%m-%d %H:%M:%S')
        self.logger = logging.getLogger(__name__)
        
    def run_optimization(self, max_iterations: int = 10000, max_workers: int = 8):
        """
        运行参数优化
        
        Args:
            max_iterations (int): 最大迭代次数
            max_workers (int): 最大线程数
        """
        self.logger.info("开始参数优化...")
        
        # 定义参数范围
        atr_periods = [2, 3, 5, 10]
        atr_multipliers = [0.2, 0.5, 1.0, 1.5, 2.0]
        trend_lengths = [2, 3, 5]  # 趋势长度参数范围
        
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
        tasks = tasks[:max_iterations]
        
        # 使用线程池执行任务
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
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
        renko_gen = RenkoGenerator(mode='daily', symbol=self.data.name if hasattr(self.data, 'name') else None)
        renko_data = renko_gen.generate_renko(self.data)
        
        # 运行策略
        strategy = RenkoStrategy(buy_trend_length=buy_trend_length, 
                               sell_trend_length=sell_trend_length,
                               symbol=self.data.name if hasattr(self.data, 'name') else None)
        signals = strategy.calculate_signals(renko_data)
        portfolio = strategy.backtest(renko_data, signals, self.initial_capital)
        
        # 计算收益率
        final_return = (portfolio.iloc[-1]['total'] - self.initial_capital) / self.initial_capital
        
        # 使用锁来安全地添加结果
        with self.results_lock:
            self.results.append({
                'mode': 'daily',
                'atr_period': None,
                'atr_multiplier': None,
                'buy_trend_length': buy_trend_length,
                'sell_trend_length': sell_trend_length,
                'return': final_return,
                'portfolio': portfolio
            })
        
    def _test_atr_mode(self, atr_period: int, atr_multiplier: float, buy_trend_length: int, sell_trend_length: int):
        """测试ATR模式"""
        self.logger.info(f"测试ATR模式 - 周期: {atr_period}, 倍数: {atr_multiplier}, "
                        f"买入趋势长度: {buy_trend_length}, 卖出趋势长度: {sell_trend_length}")
        
        # 生成砖型图
        renko_gen = RenkoGenerator(mode='atr', atr_period=atr_period, atr_multiplier=atr_multiplier,
                                 symbol=self.data.name if hasattr(self.data, 'name') else None)
        renko_data = renko_gen.generate_renko(self.data)
        
        # 运行策略
        strategy = RenkoStrategy(buy_trend_length=buy_trend_length, 
                               sell_trend_length=sell_trend_length,
                               symbol=self.data.name if hasattr(self.data, 'name') else None)
        signals = strategy.calculate_signals(renko_data)
        portfolio = strategy.backtest(renko_data, signals, self.initial_capital)
        
        # 计算收益率
        final_return = (portfolio.iloc[-1]['total'] - self.initial_capital) / self.initial_capital
        
        # 使用锁来安全地添加结果
        with self.results_lock:
            self.results.append({
                'mode': 'atr',
                'atr_period': atr_period,
                'atr_multiplier': atr_multiplier,
                'buy_trend_length': buy_trend_length,
                'sell_trend_length': sell_trend_length,
                'return': final_return,
                'portfolio': portfolio
            })
        
    def _print_optimization_results(self):
        """输出优化结果"""
        self.logger.info("优化结果汇总:")
        self.logger.info("-" * 80)
        
        # 按收益率排序
        sorted_results = sorted(self.results, key=lambda x: x['return'], reverse=True)
        
        for i, result in enumerate(sorted_results, 1):
            mode_str = f"模式: {result['mode']}"
            if result['mode'] == 'atr':
                mode_str += f", ATR周期: {result['atr_period']}, ATR倍数: {result['atr_multiplier']}"
            mode_str += f", 买入趋势长度: {result['buy_trend_length']}, 卖出趋势长度: {result['sell_trend_length']}"
                
            self.logger.info(f"{i}. {mode_str}")
            self.logger.info(f"   收益率: {result['return']:.2%}")
            
        # 输出最优参数
        best_result = sorted_results[0]
        self.logger.info("最优参数组合:")
        self.logger.info(f"模式: {best_result['mode']}")
        if best_result['mode'] == 'atr':
            self.logger.info(f"ATR周期: {best_result['atr_period']}")
            self.logger.info(f"ATR倍数: {best_result['atr_multiplier']}")
        self.logger.info(f"买入趋势长度: {best_result['buy_trend_length']}")
        self.logger.info(f"卖出趋势长度: {best_result['sell_trend_length']}")
        self.logger.info(f"收益率: {best_result['return']:.2%}")
        
    def get_best_parameters(self) -> Dict:
        """获取最优参数组合"""
        sorted_results = sorted(self.results, key=lambda x: x['return'], reverse=True)
        return sorted_results[0] 