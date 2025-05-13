import matplotlib
matplotlib.use('Agg')

from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import mplfinance as mpf
import os
import pandas as pd
import threading
from typing import Optional, Dict, Any

class RenkoPlotter:
    """
    用于绘制Renko回测结果的工具类。
    """
    _plot_lock = threading.Lock()  # 类变量，所有实例共享

    def __init__(self, output_dir: str = 'results', recent_signal_days: int = 3, target_return: float = 15):
        self.output_dir: str = output_dir
        self.recent_signal_days: int = recent_signal_days
        self.target_return: float = target_return
        self._ensure_dir_exists(output_dir)

        # 数据成员
        self.renko_data: Optional[pd.DataFrame] = None
        self.portfolio_value: Optional[pd.DataFrame] = None
        self.signals: Optional[pd.DataFrame] = None
        self.symbol: Optional[str] = None
        self.symbol_name: Optional[str] = None
        self.result: Optional[Dict[str, Any]] = None
        self.start_date: Optional[str] = None
        self.end_date: Optional[str] = None

        # 中文字体设置
        plt.rcParams['font.sans-serif'] = ['PingFang SC', 'sans-serif', 'Microsoft YaHei', 'SimHei', 'Heiti TC', 'Arial Unicode MS']
        plt.rcParams['axes.unicode_minus'] = False

    def set_data(self, result: Dict[str, Any]):
        """
        设置绘图所需的数据。
        """
        self.result = result
        self.renko_data = result['renko_data']
        self.portfolio_value = result['portfolio']
        self.signals = result['signals']
        self.symbol = result['symbol']
        self.symbol_name = result['symbol_name']
        self.start_date = result['start_date']
        self.end_date = result['end_date']

    def plot_results(self) -> str:
        """
        绘制回测结果，返回图片保存路径。
        """
        with RenkoPlotter._plot_lock:
            self._validate_data()
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 9))
            self._plot_renko_chart(ax1)
            self._plot_signals(ax1)
            self._plot_portfolio_value(ax2)
            ax1.margins(y=0.2)
            ax2.margins(y=0.2)
            result_path = self._save_and_log_plot(fig)
        return result_path

    def _validate_data(self):
        """
        检查数据是否已设置。
        """
        if any(v is None for v in [self.renko_data, self.portfolio_value, self.signals, self.symbol]):
            raise ValueError("请先使用set_data方法设置数据")

    def _plot_renko_chart(self, ax):
        """
        绘制Renko K线图。
        """
        title = self._get_chart_title()
        ax.set_title(title, fontsize=10)
        df = self.renko_data.copy()
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        style = mpf.make_mpf_style(marketcolors=mpf.make_marketcolors(up='r', down='g', edge='inherit', wick='inherit', volume='inherit'))
        mpf.plot(df, type='candle', volume=False, show_nontrading=False, ax=ax, style=style)
        ax.grid(True)

    def _get_chart_title(self) -> str:
        """
        生成图表标题。
        """
        title = f'{self.symbol} - {self.symbol_name} - {self.start_date} ~ {self.end_date}'
        if self.result:
            brick_size_str = "NA" if self.result['brick_size'] is None else f"{self.result['brick_size']:.2f}"
            title += (f"\n\nBest Params: mode={self.result['mode']}, brick_size=¥{brick_size_str}, "
                      f"buy_trend_length={self.result['buy_trend_length']}, "
                      f"sell_trend_length={self.result['sell_trend_length']}, "
                      f"atr_period={self.result['atr_period']}, atr_multiplier={self.result['atr_multiplier']}")
        return title

    def _plot_signals(self, ax):
        """
        绘制买卖信号。
        """
        buy_signals = self.signals[self.signals['signal'] == 1]
        sell_signals = self.signals[self.signals['signal'] == -1]
        self._plot_first_brick(ax)
        self._plot_signal_points(ax, buy_signals, marker='^', color='red', label_prefix='B')
        self._plot_signal_points(ax, sell_signals, marker='v', color='green', label_prefix='S', use_open=True)
        self._plot_last_brick_if_needed(ax)

    def _plot_first_brick(self, ax):
        """
        标注第一个砖块。
        """
        first = self.renko_data.iloc[0]
        offset_price = first['high'] * 1.01
        ax.annotate(f'I: {first["close"]:.2f}\n{first["date"].strftime("%Y%m%d")}',
                    xy=(0, offset_price),
                    xytext=(0, 10),
                    textcoords='offset points',
                    ha='center', va='bottom', color='blue', fontsize=7)

    def _plot_signal_points(self, ax, signals, marker, color, label_prefix, use_open=False):
        """
        绘制买入或卖出信号点。
        """
        for i in signals.index:
            row = self.renko_data.iloc[i]
            date = row['date']
            price = row['close']
            offset_price = row['open'] * 1.01 if use_open else row['high'] * 1.01
            ax.scatter(i, offset_price, color=color, marker=marker)
            ax.annotate(f'{label_prefix}: {price:.2f}\n{date.strftime("%Y%m%d")}',
                        xy=(i, offset_price),
                        xytext=(0, 10),
                        textcoords='offset points',
                        ha='center', va='bottom', color=color, fontsize=7)

    def _plot_last_brick_if_needed(self, ax):
        """
        如果最后一个砖块没有买卖信号，则标注最后价格。
        """
        if self.signals.iloc[-1]['signal'] == 0:
            last = self.renko_data.iloc[-1]
            offset_price = last['high'] * 1.01
            ax.annotate(f'N: {last["close"]:.2f}\n{last["date"].strftime("%Y%m%d")}',
                        xy=(len(self.renko_data)-1, offset_price),
                        xytext=(0, 10),
                        textcoords='offset points',
                        ha='center', va='bottom', color='blue', fontsize=7)

    def _plot_portfolio_value(self, ax):
        """
        绘制投资组合价值曲线。
        """
        ax.set_title('Portfolio Value', fontsize=10)
        ax.plot(self.portfolio_value.index, self.portfolio_value['total'], 'b-')
        max_idx = self.portfolio_value['total'].idxmax()
        last_idx = self.portfolio_value.index[-1]
        if max_idx != last_idx:
            self._annotate_portfolio_point(ax, max_idx, 'Max', 'yellow')
        self._annotate_portfolio_point(ax, last_idx, 'Final', 'lightblue')
        ax.set_xlabel('Index')
        ax.grid(True)

    def _annotate_portfolio_point(self, ax, idx, label, color):
        """
        标注投资组合关键点。
        """
        value = self.portfolio_value.iloc[idx]['total']
        initial_value = self.portfolio_value['total'].iloc[0]
        ratio = (value / initial_value - 1) * 100
        ax.annotate(f'{label}: {ratio:+.1f}%',
                    xy=(idx, value),
                    xytext=(0, 10),
                    textcoords='offset points',
                    ha='center', va='bottom',
                    bbox=dict(boxstyle='round,pad=0.5', fc=color, alpha=0.5),
                    arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0'),
                    fontsize=7)

    def _save_and_log_plot(self, fig) -> str:
        """
        保存图表并记录日志，返回图片路径。
        """
        action = self._get_recent_action()
        output_dir = os.path.join(self.output_dir, datetime.now().strftime('%Y-%m-%d'))
        self._ensure_dir_exists(output_dir)
        file_name = f"[{action}]{self.symbol}-{self.symbol_name}-{self.start_date}-{self.end_date}.png"
        plt.tight_layout()
        file_path = os.path.join(output_dir, file_name)
        plt.savefig(file_path)
        self._log_result(file_name)
        plt.close(fig)
        return file_path

    def _get_recent_action(self) -> str:
        """
        获取最近N天的信号动作。
        """
        value = self.portfolio_value.iloc[-1]['total']
        initial_value = self.portfolio_value['total'].iloc[0]
        ratio = (value / initial_value - 1) * 100
        if ratio > self.target_return:
            recent_signals = self.signals.iloc[-self.recent_signal_days:]
            for _, signal in recent_signals.iterrows():
                if signal['signal'] != 0:
                    return "Buy" if signal['signal'] == 1 else "Sell"
        return "NA"

    def _log_result(self, file_name: str):
        """
        记录结果到日志文件。
        """
        log_dir = "results"
        self._ensure_dir_exists(log_dir)
        log_file = os.path.join(log_dir, f"backtest-results-{datetime.now().strftime('%Y-%m-%d')}.log")
        with open(log_file, 'a') as log:
            log.write(file_name + '\n')

    @staticmethod
    def _ensure_dir_exists(path: str):
        """
        确保目录存在。
        """
        if not os.path.exists(path):
            os.makedirs(path)