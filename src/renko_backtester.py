from data_fetcher import DataFetcher
from renko_generator import RenkoGenerator
from strategy import RenkoStrategy
from backtest_optimizer import BacktestOptimizer
from renko_plotter import RenkoPlotter
import logging
from config import RenkoConfig

class RenkoBacktester:
    def __init__(self, args, data_fetcher):
        self.args = args
        self.symbol_name = None
        self.optimizer = None
        self.fetcher = data_fetcher
        self.result = None
        config = RenkoConfig()
        self.plotter = RenkoPlotter(recent_signal_days=config.recent_signal_days, target_return=config.target_return)
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
            self.result = self._run_optimized_backtest(df)
        else:
            self.result = self._run_standard_backtest(df)
        return self.result

    def _run_optimized_backtest(self, df):
        """运行优化后的回测"""
        self.optimizer = BacktestOptimizer(df, self.args)
        self.optimizer.run_optimization()
        best_result = self.optimizer.get_best_result()
        best_result['symbol_name'] = self.symbol_name
        
        self.logger.info("========================最优参数组合=========================")
        self.logger.info(f"模式: {best_result['mode']}, ATR周期: {best_result['atr_period']}, ATR倍数: {best_result['atr_multiplier']}, "
                    f"买入趋势长度: {best_result['buy_trend_length']}, 卖出趋势长度: {best_result['sell_trend_length']}\n"
                    f"--symbol {best_result['symbol']} --start_date {best_result['start_date']} --end_date {best_result['end_date']} "
                    f"--renko_mode {best_result['mode']} --atr_period {best_result['atr_period']} --atr_multiplier {best_result['atr_multiplier']} "
                    f"--buy_trend_length {best_result['buy_trend_length']} --sell_trend_length {best_result['sell_trend_length']} --brick_size {best_result['brick_size']:.2f}")
        self.logger.info(f"最后信号: {best_result['last_signal']}")
        self.logger.info(f"最后信号日期: {best_result['last_signal_date']}")
        self.logger.info(f"最后信号价格: {best_result['last_price']:.2f}")
        self.logger.info("===========================================================")
        
        return best_result
    
    def _run_standard_backtest(self, df):
        """运行标准回测"""
        params = {
            'symbol': self.args.symbol,
            'mode': self.args.renko_mode,
            'atr_period': self.args.atr_period,
            'atr_multiplier': self.args.atr_multiplier,
            'buy_trend_length': self.args.buy_trend_length,
            'sell_trend_length': self.args.sell_trend_length,
            'brick_size': self.args.brick_size,
            }
        
        return self._run_backtest_with_params(df, params)
    
    def _run_backtest_with_params(self, df, params):
        """使用指定参数运行回测"""
        renko_gen = RenkoGenerator(mode=params['mode'],
                                 atr_period=params['atr_period'],
                                 atr_multiplier=params['atr_multiplier'],
                                 symbol=params['symbol'],
                                 brick_size=params['brick_size'],
                                 save_data=getattr(self.args, 'save_data', False))
        renko_data = renko_gen.generate_renko(df)
        params['brick_size'] = renko_gen.get_brick_size() if params['brick_size'] is None else params['brick_size']
        brick_size_str = "NA" if params['brick_size'] is None else f"{params['brick_size']:.2f}"
        
        strategy = RenkoStrategy(buy_trend_length=params['buy_trend_length'],
                               sell_trend_length=params['sell_trend_length'],
                               symbol=params['symbol'],
                               save_data=getattr(self.args, 'save_data', False))
        signals = strategy.calculate_signals(renko_data)
        portfolio_value = strategy.backtest(renko_data, signals, initial_capital=1000000)
        
        # 计算收益
        initial_capital = portfolio_value['total'].iloc[0]
        final_capital = portfolio_value['total'].iloc[-1]
        return_pct = (final_capital - initial_capital) / initial_capital * 100

        result = {
                'symbol': params['symbol'],
                'mode': params['mode'],
                'atr_period': params['atr_period'],
                'atr_multiplier': params['atr_multiplier'],
                'buy_trend_length': params['buy_trend_length'],
                'sell_trend_length': params['sell_trend_length'],
                'brick_size': params['brick_size'],
                'start_date': self.args.start_date,
                'end_date': self.args.end_date,
                'return': return_pct,
                'renko_data': renko_data,
                'portfolio': portfolio_value,
                'signals': signals,
                'last_signal': signals.iloc[-1].signal,
                'last_signal_date': signals.iloc[-1].date.strftime('%Y-%m-%d'),
                'last_price': renko_data.iloc[-1].close,
                'symbol_name': self.symbol_name
        }
        
        # 打印回测参数
        params_str = f"回测参数：--symbol {result['symbol']} --start_date {self.args.start_date} --end_date {self.args.end_date} " + \
            f"--renko_mode {result['mode']} --atr_period {result['atr_period']} --atr_multiplier {result['atr_multiplier']} " + \
            f"--buy_trend_length {result['buy_trend_length']} --sell_trend_length {result['sell_trend_length']} "
        if result['brick_size'] is not None:
            params_str += f"--brick_size {result['brick_size']}"

        self.logger.info(params_str)
        self.logger.info(f"最后收益率: {result['return']:.2f}%, 初始资金: {initial_capital:.2f} -> 最终资金: {final_capital:.2f}")
        self.logger.info(f"最后2个信号: {signals.iloc[-2].signal}, {signals.iloc[-1].signal}, 日期: {signals.iloc[-1].date.strftime('%Y-%m-%d')}, 价格: {renko_data.iloc[-1].close:.2f}")
        
        return result

    def plot_results(self):
        """绘制回测结果"""
        if self.result is None:
            self.logger.error("回测结果为空，请先运行回测试")
            return

        # 使用绘图器绘制结果
        self.plotter.set_data(self.result)
        result_path = self.plotter.plot_results()
        self.logger.info(f"回测结果已保存到: {result_path}")
        return result_path
