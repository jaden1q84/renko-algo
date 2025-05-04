import argparse
from renko_backtester import RenkoBacktester

def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='Renko策略回测程序')
    parser.add_argument('--token', required=True, help='API访问令牌')
    parser.add_argument('--symbol', required=True, help='股票代码，例如：688041.SH')
    parser.add_argument('--start_date', required=True, help='开始日期，格式：YYYY-MM-DD')
    parser.add_argument('--end_date', required=True, help='结束日期，格式：YYYY-MM-DD')
    parser.add_argument('--renko_mode', choices=['atr', 'daily'], default='daily', 
                       help='Renko生成模式：atr（基于ATR）或daily（基于日线）')
    parser.add_argument('--atr_period', type=int, default=10, help='ATR周期（仅当renko_mode=atr时有效）')
    parser.add_argument('--atr_multiplier', type=float, default=0.5, help='ATR乘数（仅当renko_mode=atr时有效）')
    parser.add_argument('--buy_trend_length', type=int, default=3, help='买入信号所需的趋势长度')
    parser.add_argument('--sell_trend_length', type=int, default=3, help='卖出信号所需的趋势长度')
    parser.add_argument('--optimize', action='store_true', help='是否进行参数优化')
    parser.add_argument('--max_iterations', type=int, default=500, help='最大优化迭代次数')
    parser.add_argument('--batch', action='store_true', help='是否以批处理模式运行（不显示图形）')
    return parser.parse_args()

def main():
    """主函数"""
    args = parse_arguments()
    backtester = RenkoBacktester(args)
    backtester.run_backtest()

if __name__ == "__main__":
    main() 