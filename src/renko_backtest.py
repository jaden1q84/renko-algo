import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import mplfinance as mpf
import os
import pandas as pd
from data_fetcher import DataFetcher
from renko_generator import RenkoGenerator
from strategy import RenkoStrategy
from backtest_optimizer import BacktestOptimizer

class RenkoBacktest:
    def __init__(self, args):
        self.args = args
        self.fetcher = DataFetcher(token=args.token)
        
    def plot_results(self, renko_data, portfolio_value, signals, symbol, best_params=None, showout=1):
        """绘制回测结果"""
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))
        
        # 绘制Renko图
        self._plot_renko_chart(ax1, renko_data, symbol, best_params)
        self._plot_signals(ax1, renko_data, signals)
        
        # 绘制投资组合价值
        self._plot_portfolio_value(ax2, renko_data, portfolio_value, symbol)
        
        # 保存和显示结果
        self._save_and_show_plot(symbol, showout)
    
    def _plot_renko_chart(self, ax, renko_data, symbol, best_params):
        """绘制K线图表"""
        title = f'{symbol}'
        if best_params:
            title += f'\nBest Params: mode={best_params["mode"]}, buy_trend_length={best_params["buy_trend_length"]}, sell_trend_length={best_params["sell_trend_length"]}, atr_period={best_params["atr_period"]}, atr_multiplier={best_params["atr_multiplier"]}'
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
        
        for i in buy_signals.index:
            self._plot_signal(ax, renko_data, i, '↑', 'r', 5)
        for i in sell_signals.index:
            self._plot_signal(ax, renko_data, i, '↓', 'g', -5)
    
    def _plot_signal(self, ax, renko_data, idx, symbol, color, offset):
        """绘制单个交易信号"""
        date = renko_data.iloc[idx]['date']
        price = renko_data.iloc[idx]['close']
        ax.annotate(f'{symbol}\n{date.strftime("%Y-%m-%d")}\n{price:.2f}', 
                   xy=(date, price), 
                   xytext=(0, offset),
                   textcoords='offset points', 
                   color=color, 
                   fontsize=7,
                   ha='center', 
                   va='bottom' if offset > 0 else 'top')
    
    def _plot_portfolio_value(self, ax, renko_data, portfolio_value, symbol):
        """绘制投资组合价值"""
        ax.set_title(f'Portfolio Value - {symbol}')
        ax.plot(renko_data['date'], portfolio_value['total'], 'g-')
        
        max_idx = portfolio_value['total'].idxmax()
        last_idx = portfolio_value.index[-1]
        initial_value = portfolio_value['total'].iloc[0]
        
        self._annotate_portfolio_point(ax, renko_data, portfolio_value, max_idx, 'Max', 'yellow')
        self._annotate_portfolio_point(ax, renko_data, portfolio_value, last_idx, 'Final', 'lightblue')
        
        """格式化日期轴"""
        ax.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%Y-%m-%d'))
        ax.xaxis.set_major_locator(plt.matplotlib.dates.AutoDateLocator())
        plt.xticks(rotation=45)
        ax.grid(True)
    
    def _annotate_portfolio_point(self, ax, renko_data, portfolio_value, idx, label, color):
        """标注投资组合的关键点"""
        date = renko_data.iloc[idx]['date']
        value = portfolio_value.iloc[idx]['total']
        initial_value = portfolio_value['total'].iloc[0]
        ratio = (value / initial_value - 1) * 100
        
        ax.annotate(f'{label}: {ratio:+.1f}%', 
                   xy=(date, value),
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
        if not os.path.exists('data'):
            os.makedirs('data')
        
        file_name = f"data/{symbol}_{self.args.start_date}_{self.args.end_date}.png"
        plt.tight_layout()
        plt.savefig(file_name)
        
        if showout:
            plt.show()
    
    def run_backtest(self):
        """运行回测"""
        # 获取数据
        df = self.fetcher.get_historical_data(self.args.symbol, self.args.start_date, self.args.end_date)
        if df is None:
            print("无法获取数据")
            return
        
        print(f"获取到{len(df)}条数据")
        
        if self.args.optimize:
            self._run_optimized_backtest(df)
        else:
            self._run_standard_backtest(df)
    
    def _run_optimized_backtest(self, df):
        """运行优化后的回测"""
        optimizer = BacktestOptimizer(df)
        optimizer.run_optimization(max_iterations=self.args.max_iterations)
        best_params = optimizer.get_best_parameters()
        
        print("=== 最佳参数 ===")
        print(f"模式: {best_params['mode']}, ATR周期: {best_params['atr_period']}, ATR倍数: {best_params['atr_multiplier']}, "
              f"买入趋势长度: {best_params['buy_trend_length']}, 卖出趋势长度: {best_params['sell_trend_length']}")
        print("================")
        
        self._run_backtest_with_params(df, best_params, showout=not self.args.batch)
    
    def _run_standard_backtest(self, df):
        """运行标准回测"""
        params = {
            'mode': self.args.renko_mode,
            'atr_period': self.args.atr_period,
            'atr_multiplier': self.args.atr_multiplier,
            'buy_trend_length': self.args.buy_trend_length,
            'sell_trend_length': self.args.sell_trend_length
        }
        
        self._run_backtest_with_params(df, params, showout=not self.args.batch)
    
    def _run_backtest_with_params(self, df, params, showout):
        """使用指定参数运行回测"""
        renko_gen = RenkoGenerator(mode=params['mode'],
                                 atr_period=params['atr_period'],
                                 atr_multiplier=params['atr_multiplier'])
        renko_data = renko_gen.generate_renko(df)
        
        strategy = RenkoStrategy(buy_trend_length=params['buy_trend_length'],
                               sell_trend_length=params['sell_trend_length'])
        signals = strategy.calculate_signals(renko_data)
        portfolio_value = strategy.backtest(renko_data, signals, initial_capital=1000000)
        
        # 计算收益
        initial_capital = portfolio_value['total'].iloc[0]
        final_capital = portfolio_value['total'].iloc[-1]
        return_pct = (final_capital - initial_capital) / initial_capital * 100
        
        print(f"初始资金: {initial_capital:.2f}")
        print(f"最终资金: {final_capital:.2f}")
        print(f"收益率: {return_pct:.2f}%")
        
        self.plot_results(renko_data, portfolio_value, signals, self.args.symbol, params, showout) 
