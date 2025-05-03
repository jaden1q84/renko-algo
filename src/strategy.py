import pandas as pd
import numpy as np

class RenkoStrategy:
    def __init__(self, brick_size, trend_length=3):
        """
        初始化砖型图策略
        
        Args:
            brick_size (float): 砖块大小
            trend_length (int): 趋势判断长度
        """
        self.brick_size = brick_size
        self.trend_length = trend_length
        self.position = 0  # 0表示空仓，1表示多仓，-1表示空仓
        
    def calculate_signals(self, renko_data):
        """
        计算交易信号
        
        Args:
            renko_data (pd.DataFrame): 砖型图数据
            
        Returns:
            pd.DataFrame: 包含交易信号的数据框
        """
        signals = pd.DataFrame(index=renko_data.index)
        signals['signal'] = 0
        
        # 计算趋势
        trend = renko_data['trend'].rolling(self.trend_length).sum()
        
        # 生成交易信号
        for i in range(self.trend_length, len(renko_data)):
            if trend[i] >= self.trend_length and self.position <= 0:
                # 上升趋势，买入信号
                signals.iloc[i] = 1
                self.position = 1
            elif trend[i] <= -self.trend_length and self.position >= 0:
                # 下降趋势，卖出信号
                signals.iloc[i] = -1
                self.position = -1
                
        return signals
        
    def backtest(self, renko_data, signals, initial_capital=100000):
        """
        回测策略
        
        Args:
            renko_data (pd.DataFrame): 砖型图数据
            signals (pd.DataFrame): 交易信号
            initial_capital (float): 初始资金
            
        Returns:
            pd.DataFrame: 回测结果
        """
        portfolio = pd.DataFrame(index=renko_data.index)
        portfolio['holdings'] = 0
        portfolio['cash'] = initial_capital
        portfolio['total'] = initial_capital
        
        position = 0
        for i in range(len(renko_data)):
            if signals.iloc[i]['signal'] == 1 and position <= 0:
                # 买入
                position = 1
                portfolio.iloc[i]['holdings'] = portfolio.iloc[i]['cash']
                portfolio.iloc[i]['cash'] = 0
            elif signals.iloc[i]['signal'] == -1 and position >= 0:
                # 卖出
                position = -1
                portfolio.iloc[i]['cash'] = portfolio.iloc[i]['holdings']
                portfolio.iloc[i]['holdings'] = 0
                
            # 更新总资产
            if position == 1:
                portfolio.iloc[i]['total'] = portfolio.iloc[i]['holdings'] * (1 + (renko_data.iloc[i]['close'] - renko_data.iloc[i]['open']) / renko_data.iloc[i]['open'])
            else:
                portfolio.iloc[i]['total'] = portfolio.iloc[i]['cash']
                
        return portfolio 