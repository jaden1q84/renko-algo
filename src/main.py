import matplotlib.pyplot as plt
import argparse
from data_fetcher import DataFetcher
from renko_generator import RenkoGenerator
from strategy import RenkoStrategy

def plot_results(renko_data, portfolio_value, signals, symbol):
    """绘制回测结果"""
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))
    
    # 绘制Renko图
    ax1.set_title(f'{symbol}')
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
    
    args = parser.parse_args()
    
    # 初始化数据获取器
    fetcher = DataFetcher(token=args.token)
    
    # 获取数据
    df = fetcher.get_historical_data(args.symbol, args.start_date, args.end_date)
    if df is None:
        print("无法获取数据")
        return
        
    print(f"获取到{len(df)}条数据")
    
    # 生成Renko数据
    renko_gen = RenkoGenerator(mode=args.renko_mode, 
                             atr_period=args.atr_period, 
                             atr_multiplier=args.atr_multiplier)
    renko_data = renko_gen.generate_renko(df)
    
    # 运行策略
    strategy = RenkoStrategy(buy_trend_length=3, sell_trend_length=3)
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