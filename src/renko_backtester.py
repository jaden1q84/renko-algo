from data_fetcher import DataFetcher
from renko_generator import RenkoGenerator
from strategy import RenkoStrategy
from backtest_optimizer import BacktestOptimizer
from renko_plotter import RenkoPlotter
import logging

class RenkoBacktester:
    def __init__(self, args):
        self.args = args
        self.symbol_name = None
        self.fetcher = DataFetcher()
        self.plotter = RenkoPlotter()
        # 配置日志
        logging.basicConfig(level=logging.INFO,
                          format='%(asctime)s - %(levelname)s - %(message)s',
                          datefmt='%Y-%m-%d %H:%M:%S')
        self.logger = logging.getLogger(__name__)
        
    def run_backtest(self):
        """运行回测"""
        # 获取数据
        df = self.fetcher.get_historical_data(self.args.symbol, self.args.start_date, self.args.end_date)
        if df is None:
            self.logger.error("无法获取数据")
            return
        
        self.symbol_name = self.fetcher.get_symbol_name(self.args.symbol)
        self.logger.info(f"获取到{len(df)}条数据")
        self.logger.debug(f"股票信息: {self.symbol_name}")
        
        if self.args.optimize:
            self._run_optimized_backtest(df)
        else:
            self._run_standard_backtest(df)
    
    def _run_optimized_backtest(self, df):
        """运行优化后的回测"""
        optimizer = BacktestOptimizer(df, self.args)
        optimizer.run_optimization()
        best_params = optimizer.get_best_parameters()
        
        self.logger.info("========================最优参数组合=========================")
        self.logger.info(f"模式: {best_params['mode']}, ATR周期: {best_params['atr_period']}, ATR倍数: {best_params['atr_multiplier']}, "
                    f"买入趋势长度: {best_params['buy_trend_length']}, 卖出趋势长度: {best_params['sell_trend_length']}\n"
                    f"--renko_mode {best_params['mode']} --atr_period {best_params['atr_period']} --atr_multiplier {best_params['atr_multiplier']} "
                    f"--buy_trend_length {best_params['buy_trend_length']} --sell_trend_length {best_params['sell_trend_length']} --brick_size {best_params['brick_size']:.2f}")
        self.logger.info(f"最后信号: {best_params['last_signal']}")
        self.logger.info(f"最后信号日期: {best_params['last_signal_date']}")
        self.logger.info(f"最后信号价格: {best_params['last_price']:.2f}")
        self.logger.info("===========================================================")
        
        self._run_backtest_with_params(df, best_params)
    
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
        
        self._run_backtest_with_params(df, params)
    
    def _run_backtest_with_params(self, df, params):
        """使用指定参数运行回测"""
        renko_gen = RenkoGenerator(mode=params['mode'],
                                 atr_period=params['atr_period'],
                                 atr_multiplier=params['atr_multiplier'],
                                 symbol=self.args.symbol,
                                 brick_size=self.args.brick_size,
                                 save_data=getattr(self.args, 'save_data', False))
        renko_data = renko_gen.generate_renko(df)
        params['brick_size'] = renko_gen.get_brick_size() if params['brick_size'] is None else params['brick_size']
        brick_size_str = "NA" if params['brick_size'] is None else f"{params['brick_size']:.2f}"
        
        strategy = RenkoStrategy(buy_trend_length=params['buy_trend_length'],
                               sell_trend_length=params['sell_trend_length'],
                               symbol=self.args.symbol,
                               save_data=getattr(self.args, 'save_data', False))
        signals = strategy.calculate_signals(renko_data)
        portfolio_value = strategy.backtest(renko_data, signals, initial_capital=1000000)
        
        # 计算收益
        initial_capital = portfolio_value['total'].iloc[0]
        final_capital = portfolio_value['total'].iloc[-1]
        return_pct = (final_capital - initial_capital) / initial_capital * 100
        
        self.logger.info(f"回测参数：--symbol {self.args.symbol} --start_date {self.args.start_date} --end_date {self.args.end_date} "
                         f"--renko_mode {params['mode']} --atr_period {params['atr_period']} --atr_multiplier {params['atr_multiplier']} "
                         f"--buy_trend_length {params['buy_trend_length']} --sell_trend_length {params['sell_trend_length']} --brick_size {brick_size_str}")
        self.logger.info(f"初始资金: {initial_capital:.2f}")
        self.logger.info(f"最终资金: {final_capital:.2f}")
        self.logger.info(f"收益率: {return_pct:.2f}%")
        self.logger.info(f"最后2个信号: {signals.iloc[-2].signal}, {signals.iloc[-1].signal}, 日期: {signals.iloc[-1].date.strftime('%Y-%m-%d')}, 价格: {renko_data.iloc[-1].close:.2f}")
        
        # 使用绘图器绘制结果
        self.plotter.set_data(renko_data, portfolio_value, signals, self.args.symbol, self.symbol_name, params)
        result_path = self.plotter.plot_results()
        self.logger.info(f"回测结果已保存到: {result_path}")
        return result_path
