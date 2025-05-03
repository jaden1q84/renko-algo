import matplotlib.pyplot as plt
import argparse
from data_fetcher import DataFetcher
from renko_generator import RenkoGenerator
from strategy import RenkoStrategy
from backtest_optimizer import BacktestOptimizer

def plot_results(renko_data, portfolio_value, signals, symbol, best_params=None):
    """绘制回测结果"""
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))
    
    # 绘制Renko图
    title = f'{symbol}'
    if best_params:
        title += f'\nBest Params: mode={best_params["mode"]}, atr_period={best_params["atr_period"]}, atr_multiplier={best_params["atr_multiplier"]}'
    ax1.set_title(title)
    ax1.plot(renko_data['date'], renko_data['close'], 'b-')
    
    # 设置日期格式
    ax1.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%Y-%m-%d'))
    ax1.xaxis.set_major_locator(plt.matplotlib.dates.AutoDateLocator())
    plt.xticks(rotation=45)
    
    # 绘制交易信号
    buy_signals = signals[signals['signal'] == 1]
    sell_signals = signals[signals['signal'] == -1]
    
    # 绘制买入信号（红色上箭头）
    for i in buy_signals.index:
        date = renko_data.iloc[i]['date']
        price = renko_data.iloc[i]['close']
        ax1.annotate('↑', xy=(date, price), xytext=(0, 10),
                    textcoords='offset points', color='r', fontsize=12,
                    ha='center', va='bottom')
    
    # 绘制卖出信号（绿色下箭头）
    for i in sell_signals.index:
        date = renko_data.iloc[i]['date']
        price = renko_data.iloc[i]['close']
        ax1.annotate('↓', xy=(date, price), xytext=(0, -10),
                    textcoords='offset points', color='g', fontsize=12,
                    ha='center', va='top')
    
    ax1.grid(True)
    
    # 绘制投资组合价值
    ax2.set_title(f'Portfolio Value - {symbol}')
    ax2.plot(renko_data['date'], portfolio_value['total'], 'g-')
    
    # 找出最高点和最后一个点
    max_idx = portfolio_value['total'].idxmax()
    last_idx = portfolio_value.index[-1]
    initial_value = portfolio_value['total'].iloc[0]
    
    # 标注最高点
    max_date = renko_data.iloc[max_idx]['date']
    max_value = portfolio_value.iloc[max_idx]['total']
    max_ratio = (max_value / initial_value - 1) * 100
    ax2.annotate(f'Max: +{max_ratio:.1f}%', 
                xy=(max_date, max_value),
                xytext=(0, 10),
                textcoords='offset points',
                ha='center',
                va='bottom',
                bbox=dict(boxstyle='round,pad=0.5', fc='yellow', alpha=0.5),
                arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0'))
    
    # 标注最后一个点
    last_date = renko_data.iloc[last_idx]['date']
    last_value = portfolio_value.iloc[last_idx]['total']
    last_ratio = (last_value / initial_value - 1) * 100
    ax2.annotate(f'Final: {last_ratio:+.1f}%', 
                xy=(last_date, last_value),
                xytext=(0, 10),
                textcoords='offset points',
                ha='center',
                va='bottom',
                bbox=dict(boxstyle='round,pad=0.5', fc='lightblue', alpha=0.5),
                arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0'))
    
    ax2.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%Y-%m-%d'))
    ax2.xaxis.set_major_locator(plt.matplotlib.dates.AutoDateLocator())
    plt.xticks(rotation=45)
    ax2.grid(True)
    
    plt.tight_layout()
    plt.show()

def main():
    # 设置命令行参数
    parser = argparse.ArgumentParser(description='Renko策略回测程序')
    parser.add_argument('--token', required=True, help='API访问令牌')
    parser.add_argument('--symbol', required=True, help='股票代码，例如：688041.SH')
    parser.add_argument('--start_date', required=True, help='开始日期，格式：YYYY-MM-DD')
    parser.add_argument('--end_date', required=True, help='结束日期，格式：YYYY-MM-DD')
    parser.add_argument('--renko_mode', choices=['atr', 'daily'], default='daily', 
                       help='Renko生成模式：atr（基于ATR）或daily（基于日线）')
    parser.add_argument('--atr_period', type=int, default=14, help='ATR周期（仅当renko_mode=atr时有效）')
    parser.add_argument('--atr_multiplier', type=float, default=1.0, help='ATR乘数（仅当renko_mode=atr时有效）')
    parser.add_argument('--buy_trend_length', type=int, default=3, help='买入信号所需的趋势长度')
    parser.add_argument('--sell_trend_length', type=int, default=3, help='卖出信号所需的趋势长度')
    parser.add_argument('--optimize', action='store_true', help='是否进行参数优化')
    parser.add_argument('--max_iterations', type=int, default=100, help='最大优化迭代次数')
    
    args = parser.parse_args()
    
    # 初始化数据获取器
    fetcher = DataFetcher(token=args.token)
    
    # 获取数据
    df = fetcher.get_historical_data(args.symbol, args.start_date, args.end_date)
    if df is None:
        print("无法获取数据")
        return
        
    print(f"获取到{len(df)}条数据")
    
    if args.optimize:
        # 运行参数优化
        optimizer = BacktestOptimizer(df)
        optimizer.run_optimization(max_iterations=args.max_iterations)
        
        # 获取最优参数
        best_params = optimizer.get_best_parameters()
        
        # 使用最优参数运行回测
        renko_gen = RenkoGenerator(mode=best_params['mode'],
                                 atr_period=best_params['atr_period'],
                                 atr_multiplier=best_params['atr_multiplier'])
        renko_data = renko_gen.generate_renko(df)
        
        strategy = RenkoStrategy(buy_trend_length=args.buy_trend_length,
                               sell_trend_length=args.sell_trend_length)
        signals = strategy.calculate_signals(renko_data)
        portfolio_value = strategy.backtest(renko_data, signals, initial_capital=1000000)
        
        # 绘制结果
        plot_results(renko_data, portfolio_value, signals, args.symbol, best_params)
    else:
        # 使用指定参数运行回测
        renko_gen = RenkoGenerator(mode=args.renko_mode, 
                                 atr_period=args.atr_period, 
                                 atr_multiplier=args.atr_multiplier)
        renko_data = renko_gen.generate_renko(df)
        
        strategy = RenkoStrategy(buy_trend_length=args.buy_trend_length,
                               sell_trend_length=args.sell_trend_length)
        signals = strategy.calculate_signals(renko_data)
        portfolio_value = strategy.backtest(renko_data, signals, initial_capital=1000000)
        
        # 计算收益
        initial_capital = portfolio_value['total'].iloc[0]
        final_capital = portfolio_value['total'].iloc[-1]
        return_pct = (final_capital - initial_capital) / initial_capital * 100
        
        print(f"初始资金: {initial_capital:.2f}")
        print(f"最终资金: {final_capital:.2f}")
        print(f"收益率: {return_pct:.2f}%")
        
        # 绘制结果
        plot_results(renko_data, portfolio_value, signals, args.symbol)

if __name__ == "__main__":
    main() 