import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import mplfinance as mpf
import os
import pandas as pd

class RenkoPlotter:
    def __init__(self, output_dir='data'):
        self.output_dir = output_dir
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # 添加成员变量
        self.renko_data = None
        self.portfolio_value = None
        self.signals = None
        self.symbol = None
        self.best_params = None
        self.start_date = None
        self.end_date = None
        
    def set_data(self, renko_data, portfolio_value, signals, symbol, best_params=None):
        """设置绘图所需的数据"""
        self.renko_data = renko_data
        self.portfolio_value = portfolio_value
        self.signals = signals
        self.symbol = symbol
        self.best_params = best_params
        self.start_date = renko_data.iloc[0]['date'].strftime('%Y-%m-%d')
        self.end_date = renko_data.iloc[-1]['date'].strftime('%Y-%m-%d')
    
    def plot_results(self, showout=1):
        """绘制回测结果"""
        if any(v is None for v in [self.renko_data, self.portfolio_value, self.signals, self.symbol]):
            raise ValueError("请先使用set_data方法设置数据")
            
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(24, 12))
        
        # 绘制Renko图
        self._plot_renko_chart(ax1)
        self._plot_signals(ax1)
        
        # 绘制投资组合价值
        self._plot_portfolio_value(ax2)
        
        # 保存和显示结果
        self._save_and_show_plot(showout)
    
    def _plot_renko_chart(self, ax):
        """绘制K线图表"""
        title = f'{self.symbol} - {self.start_date} ~ {self.end_date}'
        if self.best_params:
            brick_size_str = "NA" if self.best_params['brick_size'] is None else f"{self.best_params['brick_size']:.2f}"
            title += f"\nBest Params: mode={self.best_params['mode']}, brick_size=¥{brick_size_str}, buy_trend_length={self.best_params['buy_trend_length']}, "
            title += f"sell_trend_length={self.best_params['sell_trend_length']}, atr_period={self.best_params['atr_period']}, atr_multiplier={self.best_params['atr_multiplier']}"
        ax.set_title(title)
        
        # 准备K线图数据
        df = self.renko_data.copy()
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        
        # 设置红涨绿跌的样式
        style = mpf.make_mpf_style(marketcolors=mpf.make_marketcolors(up='r', down='g', edge='inherit', wick='inherit', volume='inherit'))
        
        # 绘制K线图
        mpf.plot(df, type='candle', volume=False, show_nontrading=False, ax=ax, style=style)
        
        self._format_date_axis(ax)

    def _plot_signals(self, ax):
        """绘制交易信号"""
        buy_signals = self.signals[self.signals['signal'] == 1]
        sell_signals = self.signals[self.signals['signal'] == -1]
        
        # 绘制买入信号
        for i in buy_signals.index:
            date = self.renko_data.iloc[i]['date']
            price = self.renko_data.iloc[i]['real_close']
            # 将标记点向上移动1%
            offset_price = self.renko_data.iloc[i]['high'] * 1.01
            ax.scatter(i, offset_price, color='red', marker='^')
            ax.annotate(f'B\n{price:.2f}@{date.strftime("%Y-%m-%d")}', 
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
            price = self.renko_data.iloc[i]['real_close']
            # 将标记点向上移动1%
            offset_price = self.renko_data.iloc[i]['high'] * 1.01
            ax.scatter(i, offset_price, color='green', marker='v')
            ax.annotate(f'S\n{price:.2f}@{date.strftime("%Y-%m-%d")}', 
                       xy=(i, offset_price),
                       xytext=(0, 10),
                       textcoords='offset points',
                       ha='center',
                       va='bottom',
                       color='green',
                       fontsize=7)
        
        # 添加图例
        handles, labels = ax.get_legend_handles_labels()
        if handles:
            ax.legend(handles, labels, loc='upper left')
    
    def _plot_portfolio_value(self, ax):
        """绘制投资组合价值"""
        ax.set_title(f'Portfolio Value - {self.symbol}')
        ax.plot(self.portfolio_value.index, self.portfolio_value['total'], 'b-')
        
        max_idx = self.portfolio_value['total'].idxmax()
        last_idx = self.portfolio_value.index[-1]
        
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
                   arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0'))
    
    def _format_date_axis(self, ax):
        """格式化日期轴"""
        # 获取当前图表的数据
        lines = ax.get_lines()
        if lines:
            x_data = lines[0].get_xdata()
            if len(x_data) > 0:
                # 设置X轴刻度为日期
                ax.set_xticks(x_data)
                ax.set_xticklabels([pd.to_datetime(x).strftime('%Y-%m-%d') for x in x_data])
                plt.xticks(rotation=45)
        ax.grid(True)
    
    def _save_and_show_plot(self, showout):
        """保存和显示图表"""
        file_name = f"{self.output_dir}/{self.symbol}-{self.start_date}-{self.end_date}.png"
        plt.tight_layout()
        plt.savefig(file_name)
        
        if showout:
            plt.show() 