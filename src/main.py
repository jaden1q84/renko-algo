import matplotlib.pyplot as plt
from data_fetcher import DataFetcher
from renko_generator import RenkoGenerator
from strategy import RenkoStrategy

def plot_results(renko_data, portfolio_value, signals):
    """绘制回测结果"""
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))
    
    # 绘制Renko图
    ax1.set_title('Renko Chart')
    ax1.plot(renko_data.index, renko_data['close'], 'b-')
    
    # 绘制交易信号
    buy_signals = signals[signals['signal'] == 1]
    sell_signals = signals[signals['signal'] == -1]
    
    # 绘制买入信号（红色上箭头）
    for date in buy_signals.index:
        price = renko_data.loc[date, 'close']
        ax1.annotate('↑', xy=(date, price), xytext=(0, 10),
                    textcoords='offset points', color='r', fontsize=12,
                    ha='center', va='bottom')
    
    # 绘制卖出信号（绿色下箭头）
    for date in sell_signals.index:
        price = renko_data.loc[date, 'close']
        ax1.annotate('↓', xy=(date, price), xytext=(0, -10),
                    textcoords='offset points', color='g', fontsize=12,
                    ha='center', va='top')
    
    ax1.grid(True)
    
    # 绘制投资组合价值
    ax2.set_title('Portfolio Value')
    ax2.plot(portfolio_value.index, portfolio_value['total'], 'g-')
    ax2.grid(True)
    
    plt.tight_layout()
    plt.show()

def main():
    # 初始化数据获取器
    fetcher = DataFetcher(token='d3a7ac8e53bcf84eee623c02fdf50c87f8eaff131cffba926407d070')
    
    # 获取数据
    symbol = '300454.SZ'  # 深信服科技
    start_date = '2024-01-01'
    end_date = '2024-12-31'
    
    df = fetcher.get_historical_data(symbol, start_date, end_date)
    if df is None:
        print("无法获取数据")
        return
        
    print(f"获取到{len(df)}条数据")
    
    # 生成Renko数据
    renko_gen = RenkoGenerator()  # 不再需要设置砖块大小
    renko_data = renko_gen.generate_renko(df)
    
    # 运行策略
    strategy = RenkoStrategy(trend_length=3)  # 不再需要设置砖块大小
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
    plot_results(renko_data, portfolio_value, signals)

if __name__ == "__main__":
    main() 