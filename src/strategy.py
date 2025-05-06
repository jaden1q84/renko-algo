import pandas as pd
import numpy as np
import logging

class RenkoStrategy:
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
        # 配置日志
        logging.basicConfig(level=logging.INFO,
                          format='%(asctime)s - %(levelname)s - %(message)s',
                          datefmt='%Y-%m-%d %H:%M:%S')
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
        
        # 计算趋势
        trend = renko_data['trend'].rolling(max(self.buy_trend_length, self.sell_trend_length)).sum()
        self.logger.info(f"趋势计算完成，数据长度: {len(trend)}")
        
        # 生成交易信号
        position = 0  # 0表示空仓，1表示多仓
        for i in range(max(self.buy_trend_length, self.sell_trend_length), len(renko_data)):
            current_trend = trend[i]
            current_price = renko_data.iloc[i]['close']
            
            if current_trend >= self.buy_trend_length and position == 0:
                # 上升趋势，买入信号
                signals.loc[signals.index[i], 'signal'] = 1
                position = 1
                self.logger.info(f"生成买入信号 - 日期: {renko_data.iloc[i]['date'].strftime('%Y-%m-%d')}, "
                                f"价格: {current_price:.2f}, "
                                f"趋势值: {current_trend:.2f}")
            elif current_trend <= -self.sell_trend_length and position == 1:
                # 下降趋势，卖出信号
                signals.loc[signals.index[i], 'signal'] = -1
                position = 0
                self.logger.info(f"生成卖出信号 - 日期: {renko_data.iloc[i]['date'].strftime('%Y-%m-%d')}, "
                                f"价格: {current_price:.2f}, "
                                f"趋势值: {current_trend:.2f}")
            else:
                signals.loc[signals.index[i], 'signal'] = 0
        
        # 将signals保存成CSV文件
        # start_date = signals.iloc[0]['date'].strftime('%Y%m%d')
        # end_date = signals.iloc[-1]['date'].strftime('%Y%m%d')
        # file_name = f"data/signals-{self.symbol}-{start_date}-{end_date}.csv"
        # signals.to_csv(file_name, index=False)
        # self.logger.info(f"交易信号已保存至: {file_name}") 
        return signals
        
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
        portfolio = pd.DataFrame(index=renko_data.index)
        portfolio['index'] = renko_data['index']  # 添加index列
        portfolio['date'] = renko_data['date']  # 添加日期列
        portfolio['holdings'] = 0.0  # 持仓市值
        portfolio['shares'] = 0.0  # 持仓股数
        portfolio['cash'] = 0.0  # 现金
        portfolio['total'] = 0.0  # 总资产
        portfolio['position'] = 0  # 持仓状态
        
        # 初始化第一天的数据
        portfolio.loc[portfolio.index[0], 'cash'] = initial_capital
        portfolio.loc[portfolio.index[0], 'total'] = initial_capital
        portfolio.loc[portfolio.index[0], 'position'] = 0
        self.logger.info(f"初始化完成 - 日期: {portfolio.iloc[0]['date'].strftime('%Y-%m-%d')}, "
                         f"现金: {initial_capital:,.2f}, "
                         f"总资产: {initial_capital:,.2f}")
        buyin_cost = 0

        for i in range(1, len(renko_data)):
            current_date = portfolio.index[i]
            previous_date = portfolio.index[i-1]
            
            # 复制前一天的状态
            portfolio.loc[current_date, 'position'] = portfolio.loc[previous_date, 'position']
            portfolio.loc[current_date, 'shares'] = portfolio.loc[previous_date, 'shares']
            portfolio.loc[current_date, 'cash'] = portfolio.loc[previous_date, 'cash']
            
            current_price = renko_data.loc[current_date, 'close']
            previous_price = renko_data.loc[previous_date, 'close']
            
            if signals.loc[current_date, 'signal'] == 1 and portfolio.loc[previous_date, 'position'] == 0:
                # 买入信号，按100股取整计算
                available_cash = portfolio.loc[previous_date, 'cash']
                # 计算可买入的股数（向下取整到100的倍数）
                shares = (available_cash // (current_price * 100)) * 100
                # 计算买入金额（包含交易费用，假设交易费用为0.009%）
                trade_amount = shares * current_price
                commission = trade_amount * 0.00009
                total_cost = trade_amount + commission
                
                portfolio.loc[current_date, 'position'] = 1
                portfolio.loc[current_date, 'holdings'] = trade_amount
                portfolio.loc[current_date, 'shares'] = shares
                portfolio.loc[current_date, 'cash'] = available_cash - total_cost
                buyin_cost = total_cost
                # 记录买入日志
                self.logger.info(f"【B-执行买入】 - 日期: {portfolio.iloc[i]['date'].strftime('%Y-%m-%d')}, "
                               f"价格: {current_price:.2f}, "
                               f"买入股数: {shares}, "
                               f"买入金额: {trade_amount:,.2f}, "
                               f"交易费用: {commission:,.2f}, "
                               f"持仓市值: {portfolio.loc[current_date, 'holdings']:,.2f}, "
                               f"剩余现金: {portfolio.loc[current_date, 'cash']:,.2f}")
            elif signals.loc[current_date, 'signal'] == -1 and portfolio.loc[previous_date, 'position'] == 1:
                # 卖出信号，清仓
                portfolio.loc[current_date, 'position'] = 0
                
                # 计算卖出金额
                sell_shares = portfolio.loc[current_date, 'shares']
                sell_amount = sell_shares * current_price
                commission = sell_amount * 0.0006
                total_sell = sell_amount - commission

                # 计算卖出时的日内收益
                portfolio.loc[current_date, 'cash'] += total_sell
                portfolio.loc[current_date, 'holdings'] = 0
                portfolio.loc[current_date, 'shares'] = 0
                # 记录卖出日志
                trade_return = (total_sell - buyin_cost) / buyin_cost
                self.logger.info(f"【S-执行卖出】 - 日期: {portfolio.iloc[i]['date'].strftime('%Y-%m-%d')}, "
                               f"价格: {current_price:.2f}, "
                               f"卖出股数: {sell_shares}, "
                               f"卖出金额: {sell_amount:,.2f}, "
                               f"交易费用: {commission:,.2f}, "
                               f"持仓市值: {portfolio.loc[current_date, 'holdings']:,.2f}, "
                               f"现金: {portfolio.loc[current_date, 'cash']:,.2f}, "
                               f"本次交易收益率: {trade_return:.2%}")

            # 计算持仓收益
            if portfolio.loc[current_date, 'position'] == 1:
                portfolio.loc[current_date, 'holdings'] = portfolio.loc[current_date, 'shares'] * current_price
                self.logger.debug(f"【持仓收益】 - 日期: {portfolio.iloc[i]['date'].strftime('%Y-%m-%d')}, "
                                f"收盘价: {current_price:.2f}, "
                                f"持仓市值: {portfolio.loc[current_date, 'holdings']:,.2f}")
            
            # 更新总资产
            portfolio.loc[current_date, 'total'] = portfolio.loc[current_date, 'holdings'] + portfolio.loc[current_date, 'cash']

        # 计算最终收益
        final_return = (portfolio.iloc[-1]['total'] - initial_capital) / initial_capital
        self.logger.info(f"回测完成 - 最终总资产: {portfolio.iloc[-1]['total']:,.2f}, "
                        f"总收益率: {final_return:.2%}")
        
        # 保存portfolio到文件（根据参数）
        if self.save_data:
            self._save_data(portfolio)
        
        return portfolio 

    def _save_data(self, portfolio):
        """
        保存投资组合到CSV文件
        """
        start_date = portfolio.iloc[0]['date'].strftime('%Y%m%d')
        end_date = portfolio.iloc[-1]['date'].strftime('%Y%m%d')
        file_name = f"data/{self.symbol}-portfolio-{start_date}-{end_date}.csv"
        portfolio.to_csv(file_name, index=False)
        self.logger.info(f"投资组合已保存至: {file_name}")
