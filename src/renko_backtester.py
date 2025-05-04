from data_fetcher import DataFetcher
from renko_generator import RenkoGenerator
from strategy import RenkoStrategy
from backtest_optimizer import BacktestOptimizer
from renko_plotter import RenkoPlotter

class RenkoBacktester:
    def __init__(self, args):
        self.args = args
        self.fetcher = DataFetcher(token=args.token)
        self.plotter = RenkoPlotter()
        
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
            'sell_trend_length': self.args.sell_trend_length,
            'brick_size': None
        }
        
        self._run_backtest_with_params(df, params, showout=not self.args.batch)
    
    def _run_backtest_with_params(self, df, params, showout):
        """使用指定参数运行回测"""
        renko_gen = RenkoGenerator(mode=params['mode'],
                                 atr_period=params['atr_period'],
                                 atr_multiplier=params['atr_multiplier'],
                                 symbol=self.args.symbol)
        renko_data = renko_gen.generate_renko(df)
        params['brick_size'] = round(renko_gen.get_brick_size(), 1)
        
        strategy = RenkoStrategy(buy_trend_length=params['buy_trend_length'],
                               sell_trend_length=params['sell_trend_length'],
                               symbol=self.args.symbol)
        signals = strategy.calculate_signals(renko_data)
        portfolio_value = strategy.backtest(renko_data, signals, initial_capital=1000000)
        
        # 计算收益
        initial_capital = portfolio_value['total'].iloc[0]
        final_capital = portfolio_value['total'].iloc[-1]
        return_pct = (final_capital - initial_capital) / initial_capital * 100
        
        print(f"初始资金: {initial_capital:.2f}")
        print(f"最终资金: {final_capital:.2f}")
        print(f"收益率: {return_pct:.2f}%")
        
        # 使用绘图器绘制结果
        self.plotter.plot_results(renko_data, portfolio_value, signals, self.args.symbol, params, showout) 