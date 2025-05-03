import matplotlib.pyplot as plt
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
    # 初始化数据获取器
    fetcher = DataFetcher(token='d3a7ac8e53bcf84eee623c02fdf50c87f8eaff131cffba926407d070')
    
    # 获取数据
    symbol = '300454.SZ'  # 深信服科技
    # symbol = '688041.SH' # 海光信息
    start_date = '2024-05-01'
    end_date = '2025-05-01'
    
    df = fetcher.get_historical_data(symbol, start_date, end_date)
    if df is None:
        print("无法获取数据")
        return
        
    print(f"获取到{len(df)}条数据")
    
    # 生成Renko数据
    renko_gen = RenkoGenerator()  # 不再需要设置砖块大小
    renko_data = renko_gen.generate_renko(df)
    
    # 运行策略
    strategy = RenkoStrategy(buy_trend_length=3, sell_trend_length=3)  # 不再需要设置砖块大小
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
    plot_results(renko_data, portfolio_value, signals, symbol)

if __name__ == "__main__":
    main() 