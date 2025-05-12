import matplotlib
matplotlib.use('Agg')

from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import mplfinance as mpf
import os
import pandas as pd
import threading

class RenkoPlotter:
    _plot_lock = threading.Lock()  # 类变量，所有实例共享

    def __init__(self, output_dir='results', recent_signal_days=3, target_return=15):
        self.output_dir = output_dir
        self.recent_signal_days = recent_signal_days
        self.target_return = target_return
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # 添加成员变量
        self.renko_data = None
        self.portfolio_value = None
        self.signals = None
        self.symbol = None
        self.symbol_name = None
        self.result = None
        self.start_date = None
        self.end_date = None
        
        plt.rcParams['font.sans-serif'] = ['PingFang SC', 'sans-serif', 'Microsoft YaHei', 'SimHei', 'Heiti TC', 'Arial Unicode MS']  # 指定中文字体
        plt.rcParams['axes.unicode_minus'] = False    # 正常显示负号
        
    def set_data(self, result):
        """设置绘图所需的数据"""
        self.result = result
        self.renko_data = result['renko_data']
        self.portfolio_value = result['portfolio']
        self.signals = result['signals']
        self.symbol = result['symbol']
        self.symbol_name = result['symbol_name']
        self.start_date = result['start_date']
        self.end_date = result['end_date']
    
    def plot_results(self):
        """绘制回测结果"""
        result_path = None  # 用于存储结果路径
        with RenkoPlotter._plot_lock:
            if any(v is None for v in [self.renko_data, self.portfolio_value, self.signals, self.symbol]):
                raise ValueError("请先使用set_data方法设置数据")
            
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 9))
            
            # 绘制Renko图
            self._plot_renko_chart(ax1)
            self._plot_signals(ax1)
            
            # 绘制投资组合价值
            self._plot_portfolio_value(ax2)
            
            # 保存和显示结果
            ax1.margins(y=0.2)
            ax2.margins(y=0.2)
            result_path = self._save_and_show_plot(fig)
        
        return result_path
    
    def _plot_renko_chart(self, ax):
        """绘制K线图表"""
        title = f'{self.symbol} - {self.symbol_name} - {self.start_date} ~ {self.end_date}'
        if self.result:
            brick_size_str = "NA" if self.result['brick_size'] is None else f"{self.result['brick_size']:.2f}"
            title += f"\n\nBest Params: mode={self.result['mode']}, brick_size=¥{brick_size_str}, buy_trend_length={self.result['buy_trend_length']}, "
            title += f"sell_trend_length={self.result['sell_trend_length']}, atr_period={self.result['atr_period']}, atr_multiplier={self.result['atr_multiplier']}"
        ax.set_title(title, fontsize=10)
        
        # 准备K线图数据
        df = self.renko_data.copy()
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        
        # 设置红涨绿跌的样式
        style = mpf.make_mpf_style(marketcolors=mpf.make_marketcolors(up='r', down='g', edge='inherit', wick='inherit', volume='inherit'))
        
        # 绘制K线图
        mpf.plot(df, type='candle', volume=False, show_nontrading=False, ax=ax, style=style)
        ax.grid(True)

    def _plot_signals(self, ax):
        """绘制交易信号"""
        buy_signals = self.signals[self.signals['signal'] == 1]
        sell_signals = self.signals[self.signals['signal'] == -1]
        
        # 绘制第一个砖块
        first_brick_price = self.renko_data.iloc[0]['close']
        first_brick_date = self.renko_data.iloc[0]['date']
        offset_price = self.renko_data.iloc[0]['high'] * 1.01
        ax.annotate(f'I: {first_brick_price:.2f}\n{first_brick_date.strftime("%Y%m%d")}', 
                    xy=(0, offset_price),
                    xytext=(0, 10),
                    textcoords='offset points',
                    ha='center',
                    va='bottom',
                    color='blue',
                    fontsize=7)
        
        # 绘制买入信号
        for i in buy_signals.index:
            date = self.renko_data.iloc[i]['date']
            price = self.renko_data.iloc[i]['close']
            # 将标记点向上移动1%
            offset_price = self.renko_data.iloc[i]['high'] * 1.01
            ax.scatter(i, offset_price, color='red', marker='^')
            ax.annotate(f'B: {price:.2f}\n{date.strftime("%Y%m%d")}', 
                       xy=(i, offset_price),
                       xytext=(0, 10),
                       textcoords='offset points',
                       ha='center',
                       va='bottom',
                       color='red',
                       fontsize=7)
        
        # 绘制卖出信号
        for i in sell_signals.index:
            date = self.renko_data.iloc[i]['date']
            price = self.renko_data.iloc[i]['close']
            # 将标记点向上移动1%
            offset_price = self.renko_data.iloc[i]['open'] * 1.01
            ax.scatter(i, offset_price, color='green', marker='v')
            ax.annotate(f'S: {price:.2f}\n{date.strftime("%Y%m%d")}', 
                       xy=(i, offset_price),
                       xytext=(0, 10),
                       textcoords='offset points',
                       ha='center',
                       va='bottom',
                       color='green',
                       fontsize=7)
            
        # 如果最后一个砖块没有BS信号，就绘制最后价格和日期
        if self.signals.iloc[-1]['signal'] == 0:
            last_brick_price = self.renko_data.iloc[-1]['close']
            last_brick_date = self.renko_data.iloc[-1]['date']
            offset_price = self.renko_data.iloc[-1]['high'] * 1.01
            ax.annotate(f'N: {last_brick_price:.2f}\n{last_brick_date.strftime("%Y%m%d")}', 
                        xy=(len(self.renko_data)-1, offset_price),
                        xytext=(0, 10),
                        textcoords='offset points',
                        ha='center',
                        va='bottom',
                        color='blue',
                        fontsize=7)
            
    def _plot_portfolio_value(self, ax):
        """绘制投资组合价值"""
        ax.set_title(f'Portfolio Value', fontsize=10)
        ax.plot(self.portfolio_value.index, self.portfolio_value['total'], 'b-')
        
        max_idx = self.portfolio_value['total'].idxmax()
        last_idx = self.portfolio_value.index[-1]
        
        if max_idx != last_idx:
            self._annotate_portfolio_point(ax, max_idx, 'Max', 'yellow')
        self._annotate_portfolio_point(ax, last_idx, 'Final', 'lightblue')
        
        # 设置X轴标签
        ax.set_xlabel('Index')
        ax.grid(True)
    
    def _annotate_portfolio_point(self, ax, idx, label, color):
        """标注投资组合的关键点"""
        value = self.portfolio_value.iloc[idx]['total']
        initial_value = self.portfolio_value['total'].iloc[0]
        ratio = (value / initial_value - 1) * 100
        
        ax.annotate(f'{label}: {ratio:+.1f}%', 
                   xy=(idx, value),
                   xytext=(0, 10),
                   textcoords='offset points',
                   ha='center',
                   va='bottom',
                   bbox=dict(boxstyle='round,pad=0.5', fc=color, alpha=0.5),
                   arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0'),
                   fontsize=7)
        
    def _save_and_show_plot(self, fig):
        """保存和显示图表"""
        # 获取最近N天的信号
        action = "NA"

        # 最终受益大于X%才标记买卖信号
        value = self.portfolio_value.iloc[-1]['total']
        initial_value = self.portfolio_value['total'].iloc[0]
        ratio = (value / initial_value - 1) * 100
        if ratio > self.target_return:
            recent_signals = self.signals.iloc[-self.recent_signal_days:]
            for idx, signal in recent_signals.iterrows():
                if signal['signal'] != 0:
                    action = "Buy" if signal['signal'] == 1 else "Sell"

        output_dir = f"{self.output_dir}/{datetime.now().strftime('%Y-%m-%d')}"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        file_name = f"[{action}]{self.symbol}-{self.symbol_name}-{self.start_date}-{self.end_date}.png"
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, file_name))

        log_dir = "results"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        log_file = os.path.join(log_dir, f"backtest-results-{datetime.now().strftime('%Y-%m-%d')}.log")
        with open(log_file, 'a') as log:
            log.write(file_name + '\n')

        plt.close(fig)
        return f"{output_dir}/{file_name}"