import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import tushare as ts
from datetime import datetime, timedelta

class RenkoBacktester:
    def __init__(self, symbol, brick_size=None, atr_period=14, brick_method='atr', start_date=None, end_date=None, token = None):
        """
        初始化Renko回测系统
        
        参数:
        symbol (str): 股票代码
        brick_size (float): 砖块大小，如果为None则使用ATR计算
        atr_period (int): 计算ATR的周期
        brick_method (str): 砖块大小计算方法，'atr'或'fixed'
        start_date (str): 回测开始日期
        end_date (str): 回测结束日期
        """
        self.symbol = symbol
        self.brick_size = brick_size
        self.atr_period = atr_period
        self.brick_method = brick_method
        self.token = token
        self.data = None

        # 如果没有指定日期，使用默认值
        if start_date is None:
            self.start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        else:
            self.start_date = start_date
            
        if end_date is None:
            self.end_date = datetime.now().strftime('%Y-%m-%d')
        else:
            self.end_date = end_date
            
        # 下载数据并预处理
        if self.token:
            ts.set_token(self.token)
        self.pro = ts.pro_api()
        self.download_data()
        self.renko_data = None
        
    def download_data(self):
        """下载股票历史数据"""
        start_date = self.start_date.replace('-', '')
        end_date = self.end_date.replace('-', '')
        if self.symbol.endswith('.HK'):
            self.data = self.pro.hk_daily(ts_code=self.symbol, start_date=start_date, end_date=end_date)
        else:
            self.data = self.pro.daily(ts_code=self.symbol, start_date=start_date, end_date=end_date)
        
        if self.data.empty:
            raise ValueError(f"无法获取{self.symbol}的数据")
        
        # 计算ATR用于设置砖块大小
        high_low = self.data['high'] - self.data['low']
        high_close = np.abs(self.data['high'] - self.data['close'].shift())
        low_close = np.abs(self.data['low'] - self.data['close'].shift())
        
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = np.max(ranges, axis=1)
        self.data['ATR'] = true_range.rolling(self.atr_period).mean()
        
    def create_renko_chart(self):
        """创建Renko图数据"""
        if self.brick_method == 'atr' and self.brick_size is None:
            # 使用ATR的平均值作为砖块大小
            self.brick_size = self.data['ATR'].mean()
            print(f"基于ATR设置的砖块大小: {self.brick_size:.2f}")
        elif self.brick_size is None:
            # 使用价格范围的一小部分作为砖块大小
            price_range = self.data['high'].max() - self.data['low'].min()
            self.brick_size = price_range * 0.01  # 使用价格范围的1%作为砖块大小
            print(f"基于价格范围设置的砖块大小: {self.brick_size:.2f}")
        
        # 初始化Renko数据
        renko_prices = []
        renko_directions = []
        
        # 使用收盘价初始化第一个砖块
        current_price = self.data['close'].iloc[0]
        renko_prices.append(current_price)
        renko_directions.append(0)  # 0表示初始砖块
        
        # 遍历价格数据构建Renko图
        for close in self.data['close']:
            # 计算当前价格与最新砖块价格的差距
            price_diff = close - renko_prices[-1]
            
            # 向上移动
            while price_diff >= self.brick_size:
                new_price = renko_prices[-1] + self.brick_size
                renko_prices.append(new_price)
                renko_directions.append(1)  # 1表示向上的砖块
                price_diff -= self.brick_size
                
            # 向下移动
            while price_diff <= -self.brick_size:
                new_price = renko_prices[-1] - self.brick_size
                renko_prices.append(new_price)
                renko_directions.append(-1)  # -1表示向下的砖块
                price_diff += self.brick_size
        
        # 创建Renko数据框
        self.renko_data = pd.DataFrame({
            'price': renko_prices,
            'direction': renko_directions
        })
        
        return self.renko_data
        
    def generate_signals(self):
        """基于Renko图生成交易信号"""
        if self.renko_data is None:
            self.create_renko_chart()
            
        signals = []
        position = 0  # 0表示不持仓，1表示多头，-1表示空头
        
        # 遍历Renko砖块生成信号
        for i in range(2, len(self.renko_data)):
            # 寻找趋势变化点
            prev_direction = self.renko_data['direction'].iloc[i-1]
            curr_direction = self.renko_data['direction'].iloc[i]
            
            signal = 0
            
            # 趋势反转策略
            # 当连续两个砖块向上时买入
            if prev_direction == 1 and curr_direction == 1 and position <= 0:
                signal = 1
                position = 1
            # 当连续两个砖块向下时卖出
            elif prev_direction == -1 and curr_direction == -1 and position >= 0:
                signal = -1
                position = -1
                
            signals.append(signal)
            
        # 为前两个砖块添加信号（无法生成信号）
        signals = [0, 0] + signals
        self.renko_data['signal'] = signals
        
        return self.renko_data
        
    def backtest(self):
        """执行回测并计算性能指标"""
        if 'signal' not in self.renko_data.columns:
            self.generate_signals()
            
        # 映射Renko砖块回原始价格时间序列
        signals_mapped = pd.Series(0, index=self.data.index)
        
        # 计算每日收益
        self.data['returns'] = self.data['close'].pct_change()
        
        # 跟踪每个交易日对应的Renko砖块
        current_brick = 0
        positions = []
        
        for date, row in self.data.iterrows():
            close_price = row['close']
            
            # 找到当前价格对应的砖块
            while current_brick < len(self.renko_data) - 1:
                next_brick_price = self.renko_data['price'].iloc[current_brick + 1]
                
                # 如果价格达到下一个砖块，移动到下一个砖块
                if (self.renko_data['direction'].iloc[current_brick + 1] == 1 and close_price >= next_brick_price) or \
                   (self.renko_data['direction'].iloc[current_brick + 1] == -1 and close_price <= next_brick_price):
                    current_brick += 1
                else:
                    break
            
            # 记录当前的信号
            if current_brick < len(self.renko_data):
                signal = self.renko_data['signal'].iloc[current_brick]
                signals_mapped[date] = signal
            
            # 跟踪仓位
            if len(positions) == 0:
                if signals_mapped[date] != 0:
                    positions.append(signals_mapped[date])
            else:
                if signals_mapped[date] == -positions[-1]:  # 如果信号与当前持仓相反
                    positions.append(signals_mapped[date])
        
        # 创建策略收益
        self.data['position'] = signals_mapped.shift(1).fillna(0)
        self.data['strategy_returns'] = self.data['position'] * self.data['returns']
        
        # 计算累积收益
        self.data['cumulative_returns'] = (1 + self.data['returns']).cumprod() - 1
        self.data['strategy_cumulative_returns'] = (1 + self.data['strategy_returns']).cumprod() - 1
        
        # 计算性能指标
        total_return = self.data['strategy_cumulative_returns'].iloc[-1]
        annual_return = (1 + total_return) ** (252 / len(self.data)) - 1
        
        sharpe_ratio = (self.data['strategy_returns'].mean() / self.data['strategy_returns'].std()) * np.sqrt(252)
        
        # 计算最大回撤
        strategy_cumulative = (1 + self.data['strategy_returns']).cumprod()
        running_max = strategy_cumulative.cummax()
        drawdown = (strategy_cumulative / running_max) - 1
        max_drawdown = drawdown.min()
        
        performance = {
            'Total Return': total_return,
            'Annual Return': annual_return,
            'Sharpe Ratio': sharpe_ratio,
            'Max Drawdown': max_drawdown
        }
        
        return performance
    
    def plot_results(self):
        """绘制回测结果"""
        if self.renko_data is None or 'signal' not in self.renko_data.columns:
            self.backtest()
        
        plt.figure(figsize=(15, 10))
        
        # 绘制股价和Renko图
        plt.subplot(2, 1, 1)
        plt.plot(self.data.index, self.data['close'], label='Price', alpha=0.5)
        
        # 标记买入和卖出点
        buy_signals = self.data[self.data['position'] == 1].index
        sell_signals = self.data[self.data['position'] == -1].index
        
        plt.scatter(buy_signals, self.data.loc[buy_signals, 'close'], marker='^', color='g', s=100, label='Buy Signal')
        plt.scatter(sell_signals, self.data.loc[sell_signals, 'close'], marker='v', color='r', s=100, label='Sell Signal')
        
        plt.title(f'{self.symbol} Renko Backtest (Brick Size: ${self.brick_size:.2f})')
        plt.ylabel('Price')
        plt.legend()
        
        # 绘制累积收益
        plt.subplot(2, 1, 2)
        plt.plot(self.data.index, self.data['cumulative_returns'], label='Holding', alpha=0.5)
        plt.plot(self.data.index, self.data['strategy_cumulative_returns'], label='Renko')
        plt.title('Cumulative Returns Comparison')
        plt.ylabel('Cumulative Returns')
        plt.legend()
        
        plt.tight_layout()
        plt.show()

# 使用示例
if __name__ == "__main__":
    import argparse

    # 创建参数解析器
    parser = argparse.ArgumentParser(description='Renko Backtester 参数设置')
    parser.add_argument('--symbol', type=str, required=True, help='股票代码')
    parser.add_argument('--brick_method', type=str, default='atr', help='砖块大小计算方法，atr或fixed')
    parser.add_argument('--brick_size', type=float, default=2.0, help='砖块大小')
    parser.add_argument('--atr_period', type=int, default=14, help='ATR周期')
    parser.add_argument('--start_date', type=str, required=True, help='回测开始日期')
    parser.add_argument('--end_date', type=str, required=True, help='回测结束日期')
    parser.add_argument('--token', type=str, required=True, help='Tushare API token')

    # 解析参数
    args = parser.parse_args()

    # 创建回测器实例
    renko_tester = RenkoBacktester(
        symbol=args.symbol,
        brick_method=args.brick_method,
        brick_size=args.brick_size,
        atr_period=args.atr_period,
        start_date=args.start_date,
        end_date=args.end_date,
        token=args.token
    )
    
    # 创建Renko图数据
    renko_data = renko_tester.create_renko_chart()
    print(f"生成了 {len(renko_data)} 个Renko砖块") 
    
    # 生成交易信号
    renko_tester.generate_signals()

    # 回测
    performance = renko_tester.backtest()
    print("\n性能指标:")
    for key, value in performance.items():
        print(f"{key}: {value:.4f}")  
      
    # 执行回测并绘制结果
    renko_tester.plot_results()