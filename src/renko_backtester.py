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
        self.fetcher = data_fetcher
        self.symbol_name = None
        self.optimizer = None
        self.result = None
        self.config = RenkoConfig()
        self.plotter = RenkoPlotter(
            recent_signal_days=self.config.recent_signal_days,
            target_return=self.config.target_return
        )
        self.logger = logging.getLogger(__name__)

    def run_backtest(self):
        """运行回测主流程"""
        try:
            df = self._fetch_data()
            if df is None:
                return
            self.symbol_name = self.fetcher.get_symbol_name(self.args.symbol)
            self.logger.info(f"获取到{len(df)}条数据")
            self.logger.debug(f"股票信息: {self.symbol_name}")

            if self.args.optimize:
                self.result = self._run_optimized_backtest(df)
            else:
                self.result = self._run_standard_backtest(df)
            return self.result
        except Exception as e:
            self.logger.error(f"回测运行异常: {e}")
            return

    def _fetch_data(self):
        """获取历史数据"""
        df = self.fetcher.get_historical_data(
            self.args.symbol, self.args.start_date, self.args.end_date
        )
        if df is None:
            self.logger.error("无法获取数据")
        return df

    def _run_optimized_backtest(self, df):
        """运行参数优化回测"""
        self.optimizer = BacktestOptimizer(df, self.args)
        self.optimizer.run_optimization()
        best_result = self.optimizer.get_best_result()
        best_result['symbol_name'] = self.symbol_name
        return best_result

    def _run_standard_backtest(self, df):
        """运行标准回测"""
        params = self._collect_params()
        return self._run_backtest_with_params(df, params)

    def _collect_params(self):
        """收集回测参数"""
        return {
            'symbol': self.args.symbol,
            'mode': self.args.renko_mode,
            'atr_period': self.args.atr_period,
            'atr_multiplier': self.args.atr_multiplier,
            'buy_trend_length': self.args.buy_trend_length,
            'sell_trend_length': self.args.sell_trend_length,
            'brick_size': self.args.brick_size,
        }

    def _run_backtest_with_params(self, df, params):
        """使用指定参数运行回测"""
        renko_gen, renko_data = self._generate_renko_data(df, params)
        if renko_data is None or renko_data.empty:
            self.logger.warning("砖型图数据为空，跳过此参数组合")
            return

        params['brick_size'] = params['brick_size'] or self._get_brick_size(params, renko_gen)
        strategy = self._init_strategy(params)
        signals = strategy.calculate_signals(renko_data)
        portfolio_value = strategy.backtest(renko_data, signals, self.config.initial_capital)

        result = self._assemble_result(params, renko_data, signals, portfolio_value)
        self._log_backtest_result(result, signals, renko_data)
        return result

    def _generate_renko_data(self, df, params):
        """生成砖型图数据"""
        renko_gen = RenkoGenerator(
            mode=params['mode'],
            atr_period=params['atr_period'],
            atr_multiplier=params['atr_multiplier'],
            symbol=params['symbol'],
            brick_size=params['brick_size'],
            save_data=getattr(self.args, 'save_data', False)
        )
        return renko_gen, renko_gen.generate_renko(df)

    def _get_brick_size(self, params, renko_gen):
        """获取砖块大小"""
        return renko_gen.get_brick_size()

    def _init_strategy(self, params):
        """初始化策略"""
        return RenkoStrategy(
            buy_trend_length=params['buy_trend_length'],
            sell_trend_length=params['sell_trend_length'],
            symbol=params['symbol'],
            save_data=getattr(self.args, 'save_data', False)
        )

    def _assemble_result(self, params, renko_data, signals, portfolio_value):
        """组装回测结果"""
        initial_capital = portfolio_value['total'].iloc[0]
        final_capital = portfolio_value['total'].iloc[-1]
        return_pct = (final_capital - initial_capital) / initial_capital * 100

        return {
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

    def _log_backtest_result(self, result, signals, renko_data):
        """打印回测参数和结果"""
        params_str = (
            f"回测参数：--symbol {result['symbol']} --start_date {result['start_date']} --end_date {result['end_date']} "
            f"--renko_mode {result['mode']} --atr_period {result['atr_period']} --atr_multiplier {result['atr_multiplier']} "
            f"--buy_trend_length {result['buy_trend_length']} --sell_trend_length {result['sell_trend_length']} "
        )
        if result['brick_size'] is not None:
            params_str += f"--brick_size {result['brick_size']}"
        self.logger.info(params_str)
        self.logger.info(
            f"最后收益率: {result['return']:.2f}%, 初始资金: {result['portfolio']['total'].iloc[0]:.2f} -> 最终资金: {result['portfolio']['total'].iloc[-1]:.2f}"
        )
        self.logger.info(
            f"最后2个信号: {signals.iloc[-2].signal}, {signals.iloc[-1].signal}, 日期: {signals.iloc[-1].date.strftime('%Y-%m-%d')}, 价格: {renko_data.iloc[-1].close:.2f}"
        )

    def plot_results(self):
        """绘制回测结果"""
        if self.result is None or self.result['renko_data'].empty:
            self.logger.error("回测结果为空，请先运行回测试")
            return
        self.plotter.set_data(self.result)
        result_path = self.plotter.plot_results()
        self.logger.info(f"回测结果已保存到: {result_path}")
        return result_path
