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
    
    def plot_results(self, renko_data, portfolio_value, signals, symbol, best_params=None, showout=1):
        """绘制回测结果"""
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))
        
        # 绘制Renko图
        self._plot_renko_chart(ax1, renko_data, symbol, best_params)
        self._plot_signals(ax1, renko_data, signals)
        
        # 绘制投资组合价值
        self._plot_portfolio_value(ax2, portfolio_value, symbol)
        
        # 保存和显示结果
        self._save_and_show_plot(symbol, showout)
    
    def _plot_renko_chart(self, ax, renko_data, symbol, best_params):
        """绘制K线图表"""
        title = f'{symbol}'
        if best_params:
            title += f'\nBest Params: mode={best_params["mode"]}, brick_size={best_params["brick_size"]}, buy_trend_length={best_params["buy_trend_length"]}, sell_trend_length={best_params["sell_trend_length"]}, atr_period={best_params["atr_period"]}, atr_multiplier={best_params["atr_multiplier"]}'
        ax.set_title(title)
        
        # 准备K线图数据
        df = renko_data.copy()
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        
        # 设置红涨绿跌的样式
        style = mpf.make_mpf_style(marketcolors=mpf.make_marketcolors(up='r', down='g', edge='inherit', wick='inherit', volume='inherit'))
        
        # 绘制K线图
        mpf.plot(df, type='candle', volume=False, show_nontrading=False, ax=ax, style=style)
        
        self._format_date_axis(ax)

    def _plot_signals(self, ax, renko_data, signals):
        """绘制交易信号"""
        buy_signals = signals[signals['signal'] == 1]
        sell_signals = signals[signals['signal'] == -1]
        
        # 绘制买入信号
        for i in buy_signals.index:
            date = renko_data.iloc[i]['date']
            price = renko_data.iloc[i]['close']
            # 将标记点向上移动5%
            offset_price = price * 1.07
            ax.scatter(i, offset_price, color='red', marker='^')
            ax.annotate(f'B\n{date.strftime("%Y-%m-%d")}\n{price:.2f}', 
                       xy=(i, offset_price),
                       xytext=(0, 10),
                       textcoords='offset points',
                       ha='center',
                       va='bottom',
                       color='red',
                       fontsize=7)
        
        # 绘制卖出信号
        for i in sell_signals.index:
            date = renko_data.iloc[i]['date']
            price = renko_data.iloc[i]['close']
            # 将标记点向上移动5%
            offset_price = price * 1.07
            ax.scatter(i, offset_price, color='green', marker='v')
            ax.annotate(f'S\n{date.strftime("%Y-%m-%d")}\n{price:.2f}', 
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
    
    def _plot_portfolio_value(self, ax, portfolio_value, symbol):
        """绘制投资组合价值"""
        ax.set_title(f'Portfolio Value - {symbol}')
        ax.plot(portfolio_value.index, portfolio_value['total'], 'b-')
        
        max_idx = portfolio_value['total'].idxmax()
        last_idx = portfolio_value.index[-1]
        
        self._annotate_portfolio_point(ax, portfolio_value, max_idx, 'Max', 'yellow')
        self._annotate_portfolio_point(ax, portfolio_value, last_idx, 'Final', 'lightblue')
        
        # 设置X轴标签
        ax.set_xlabel('Index')
        ax.grid(True)
    
    def _annotate_portfolio_point(self, ax, portfolio_value, idx, label, color):
        """标注投资组合的关键点"""
        value = portfolio_value.iloc[idx]['total']
        initial_value = portfolio_value['total'].iloc[0]
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
    
    def _save_and_show_plot(self, symbol, showout):
        """保存和显示图表"""
        file_name = f"{self.output_dir}/{symbol}.png"
        plt.tight_layout()
        plt.savefig(file_name)
        
        if showout:
            plt.show() 