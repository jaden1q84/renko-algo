import matplotlib.pyplot as plt
from data_fetcher import DataFetcher
from renko_generator import RenkoGenerator
from strategy import RenkoStrategy

def plot_results(renko_data, portfolio):
    """
    绘制回测结果
    
    Args:
        renko_data (pd.DataFrame): 砖型图数据
        portfolio (pd.DataFrame): 回测结果
    """
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))
    
    # 绘制砖型图
    for i in range(len(renko_data)):
        if renko_data.iloc[i]['trend'] == 1:
            color = 'green'
        else:
            color = 'red'
            
        ax1.plot([renko_data.index[i], renko_data.index[i]],
                [renko_data.iloc[i]['open'], renko_data.iloc[i]['close']],
                color=color, linewidth=2)
                
    # 绘制资产曲线
    ax2.plot(portfolio.index, portfolio['total'], label='Total Value')
    ax2.plot(portfolio.index, portfolio['cash'], label='Cash')
    ax2.plot(portfolio.index, portfolio['holdings'], label='Holdings')
    
    ax1.set_title('Renko Chart')
    ax2.set_title('Portfolio Value')
    ax2.legend()
    
    plt.tight_layout()
    plt.show()

def main():
    # 初始化数据获取器
    fetcher = DataFetcher()
    
    # 获取数据
    symbol = 'AAPL'
    start_date = '2023-01-01'
    end_date = '2024-01-01'
    data = fetcher.get_historical_data(symbol, start_date, end_date)
    
    if data is None:
        print("无法获取数据")
        return
        
    # 生成砖型图
    brick_size = 2.0  # 可以根据需要调整砖块大小
    renko_gen = RenkoGenerator(brick_size)
    renko_data = renko_gen.generate_renko(data)
    
    # 运行策略
    strategy = RenkoStrategy(brick_size, trend_length=3)
    portfolio = strategy.backtest(renko_data)
    
    # 计算收益率
    initial_value = portfolio['total'].iloc[0]
    final_value = portfolio['total'].iloc[-1]
    returns = (final_value - initial_value) / initial_value * 100
    
    print(f"初始资金: {initial_value:.2f}")
    print(f"最终资金: {final_value:.2f}")
    print(f"收益率: {returns:.2f}%")
    
    # 绘制结果
    plot_results(renko_data, portfolio)

if __name__ == "__main__":
    main() 