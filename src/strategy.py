import pandas as pd
import numpy as np
import logging

class RenkoStrategy:
    COMMISSION_BUY = 0.00009
    COMMISSION_SELL = 0.0006
    LOT_SIZE = 100

    def __init__(self, buy_trend_length=3, sell_trend_length=3, symbol=None, save_data=False):
        """
        初始化Renko策略
        
        Args:
            buy_trend_length (int): 买入信号所需的趋势长度
            sell_trend_length (int): 卖出信号所需的趋势长度
            symbol (str): 股票代码
            save_data (bool): 是否保存回测结果到文件
        """
        self.buy_trend_length = buy_trend_length
        self.sell_trend_length = sell_trend_length
        self.symbol = symbol
        self.save_data = save_data
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"策略初始化完成 - 股票代码: {symbol}, 买入趋势长度: {buy_trend_length}, 卖出趋势长度: {sell_trend_length}")
        
    def calculate_signals(self, renko_data):
        """
        计算交易信号
        
        Args:
            renko_data (pd.DataFrame): 砖型图数据
            
        Returns:
            pd.DataFrame: 包含交易信号的数据框
        """
        self.logger.info("开始计算交易信号...")
        signals = pd.DataFrame(index=renko_data.index)
        signals['index'] = renko_data['index']
        signals['date'] = renko_data['date']
        signals['signal'] = 0
        
        trend = self._calculate_trend(renko_data)
        position = 0
        for i in range(max(self.buy_trend_length, self.sell_trend_length), len(renko_data)):
            current_trend = trend[i]
            current_price = renko_data.iloc[i]['close']
            date_str = renko_data.iloc[i]['date'].strftime('%Y-%m-%d')
            if self._is_buy_signal(current_trend, position):
                signals.loc[signals.index[i], 'signal'] = 1
                position = 1
                self.logger.info(f"生成买入信号 - 日期: {date_str}, 价格: {current_price:.2f}, 趋势值: {current_trend:.2f}")
            elif self._is_sell_signal(current_trend, position):
                signals.loc[signals.index[i], 'signal'] = -1
                position = 0
                self.logger.info(f"生成卖出信号 - 日期: {date_str}, 价格: {current_price:.2f}, 趋势值: {current_trend:.2f}")
            else:
                signals.loc[signals.index[i], 'signal'] = 0
        
        return signals
        
    def _calculate_trend(self, renko_data):
        window = max(self.buy_trend_length, self.sell_trend_length)
        return renko_data['trend'].rolling(window).sum()

    def _is_buy_signal(self, trend_value, position):
        return trend_value >= self.buy_trend_length and position == 0

    def _is_sell_signal(self, trend_value, position):
        return trend_value <= -self.sell_trend_length and position == 1

    def backtest(self, renko_data, signals, initial_capital=1000000):
        """
        回测策略
        
        Args:
            renko_data (pd.DataFrame): 砖型图数据
            signals (pd.DataFrame): 交易信号
            initial_capital (float): 初始资金
            
        Returns:
            pd.DataFrame: 回测结果
        """
        self.logger.info(f"开始回测 - 初始资金: {initial_capital:,.2f}")
        portfolio = self._init_portfolio(renko_data, initial_capital)
        buyin_cost = 0

        for i in range(1, len(renko_data)):
            current_idx = portfolio.index[i]
            prev_idx = portfolio.index[i-1]
            self._copy_prev_state(portfolio, current_idx, prev_idx)
            current_price = renko_data.loc[current_idx, 'close']
            signal = signals.loc[current_idx, 'signal']
            position = portfolio.loc[prev_idx, 'position']

            if signal == 1 and position == 0:
                buyin_cost = self._execute_buy(portfolio, current_idx, current_price)
            elif signal == -1 and position == 1:
                self._execute_sell(portfolio, current_idx, current_price, buyin_cost)
            if portfolio.loc[current_idx, 'position'] == 1:
                self._update_holdings(portfolio, current_idx, current_price)
            self._update_total(portfolio, current_idx)

        final_return = (portfolio.iloc[-1]['total'] - initial_capital) / initial_capital
        self.logger.info(f"回测完成 - 最终总资产: {portfolio.iloc[-1]['total']:,.2f}, 总收益率: {final_return:.2%}")
        
        # 保存portfolio到文件（根据参数）
        if self.save_data:
            self._save_data(portfolio)
        
        return portfolio 

    def _init_portfolio(self, renko_data, initial_capital):
        portfolio = pd.DataFrame(index=renko_data.index)
        portfolio['index'] = renko_data['index']
        portfolio['date'] = renko_data['date']
        portfolio['holdings'] = 0.0
        portfolio['shares'] = 0.0
        portfolio['cash'] = 0.0
        portfolio['total'] = 0.0
        portfolio['position'] = 0
        portfolio.loc[portfolio.index[0], 'cash'] = initial_capital
        portfolio.loc[portfolio.index[0], 'total'] = initial_capital
        portfolio.loc[portfolio.index[0], 'position'] = 0
        self.logger.info(f"初始化完成 - 日期: {portfolio.iloc[0]['date'].strftime('%Y-%m-%d')}, 现金: {initial_capital:,.2f}, 总资产: {initial_capital:,.2f}")
        return portfolio

    def _copy_prev_state(self, portfolio, current_idx, prev_idx):
        for col in ['position', 'shares', 'cash']:
            portfolio.loc[current_idx, col] = portfolio.loc[prev_idx, col]

    def _execute_buy(self, portfolio, idx, price):
        available_cash = portfolio.loc[idx, 'cash']
        shares = (available_cash // (price * self.LOT_SIZE)) * self.LOT_SIZE
        trade_amount = shares * price
        commission = trade_amount * self.COMMISSION_BUY
        total_cost = trade_amount + commission
        portfolio.loc[idx, 'position'] = 1
        portfolio.loc[idx, 'holdings'] = trade_amount
        portfolio.loc[idx, 'shares'] = shares
        portfolio.loc[idx, 'cash'] = available_cash - total_cost
        self.logger.info(f"【B-执行买入】 - 日期: {portfolio.loc[idx, 'date'].strftime('%Y-%m-%d')}, 价格: {price:.2f}, 买入股数: {shares}, 买入金额: {trade_amount:,.2f}, 交易费用: {commission:,.2f}, 持仓市值: {portfolio.loc[idx, 'holdings']:,.2f}, 剩余现金: {portfolio.loc[idx, 'cash']:,.2f}")
        return total_cost

    def _execute_sell(self, portfolio, idx, price, buyin_cost):
        sell_shares = portfolio.loc[idx, 'shares']
        sell_amount = sell_shares * price
        commission = sell_amount * self.COMMISSION_SELL
        total_sell = sell_amount - commission
        portfolio.loc[idx, 'position'] = 0
        portfolio.loc[idx, 'cash'] += total_sell
        portfolio.loc[idx, 'holdings'] = 0
        portfolio.loc[idx, 'shares'] = 0
        trade_return = (total_sell - buyin_cost) / buyin_cost if buyin_cost else 0
        self.logger.info(f"【S-执行卖出】 - 日期: {portfolio.loc[idx, 'date'].strftime('%Y-%m-%d')}, 价格: {price:.2f}, 卖出股数: {sell_shares}, 卖出金额: {sell_amount:,.2f}, 交易费用: {commission:,.2f}, 持仓市值: {portfolio.loc[idx, 'holdings']:,.2f}, 现金: {portfolio.loc[idx, 'cash']:,.2f}, 本次交易收益率: {trade_return:.2%}")

    def _update_holdings(self, portfolio, idx, price):
        portfolio.loc[idx, 'holdings'] = portfolio.loc[idx, 'shares'] * price
        self.logger.debug(f"【持仓收益】 - 日期: {portfolio.loc[idx, 'date'].strftime('%Y-%m-%d')}, 收盘价: {price:.2f}, 持仓市值: {portfolio.loc[idx, 'holdings']:,.2f}")

    def _update_total(self, portfolio, idx):
        portfolio.loc[idx, 'total'] = portfolio.loc[idx, 'holdings'] + portfolio.loc[idx, 'cash']

    def _save_data(self, portfolio):
        """
        保存投资组合到CSV文件
        """
        start_date = portfolio.iloc[0]['date'].strftime('%Y%m%d')
        end_date = portfolio.iloc[-1]['date'].strftime('%Y%m%d')
        file_name = f"data/{self.symbol}-portfolio-{start_date}-{end_date}.csv"
        portfolio.to_csv(file_name, index=False)
        self.logger.info(f"投资组合已保存至: {file_name}")
