import pandas as pd
import numpy as np
from typing import Literal, Optional
import logging

class RenkoGenerator:
    def __init__(self, mode: Literal['daily', 'atr'] = 'atr', atr_period: int = 10, 
                 atr_multiplier: float = 0.5, symbol: Optional[str] = None, brick_size: Optional[float] = None, 
                 save_data: bool = False):
        """
        初始化砖型图生成器
        
        Args:
            mode (str): 生成模式，'daily'表示基于日K线，'atr'表示基于ATR
            atr_period (int): ATR计算周期
            atr_multiplier (float): ATR倍数，用于调整砖块大小
            symbol (str): 股票代码
            brick_size (float): 砖块颗粒度
            save_data (bool): 是否保存结果到文件，默认False
        """
        self.mode = mode
        self.atr_period = atr_period
        self.atr_multiplier = atr_multiplier
        self.symbol = symbol
        self.brick_size = brick_size
        self.save_data = save_data
        self.renko_data = pd.DataFrame(columns=['index', 'date', 'open', 'high', 'low', 'close', 'trend'])
        self.logger = logging.getLogger(__name__)
        self._validate_params()

    def _validate_params(self):
        if self.mode not in ['daily', 'atr']:
            raise ValueError("mode参数必须为 'daily' 或 'atr'")
        if self.atr_period <= 0:
            raise ValueError("atr_period 必须为正整数")
        if self.atr_multiplier <= 0:
            raise ValueError("atr_multiplier 必须为正数")
        if self.brick_size is not None and self.brick_size <= 0:
            raise ValueError("brick_size 必须为正数")

    def generate_renko(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        生成砖型图数据
        
        Args:
            data (pd.DataFrame): 原始K线数据
            
        Returns:
            pd.DataFrame: 砖型图数据
        """
        if self.mode == 'daily':
            self.renko_data = self._generate_renko(data, self._daily_brick_logic)
        else:
            if self.brick_size is None:
                self.brick_size = self._calculate_atr(data)
                if self.brick_size == 0:
                    self.logger.warning("ATR计算的砖块大小为0")
                    self.renko_data = pd.DataFrame()
                    return self.renko_data
                self.logger.info(f"ATR计算的砖块大小为: {self.brick_size:.2f}")
            else:
                self.logger.info(f"使用用户设置的砖块大小: {self.brick_size:.2f}")
            self.renko_data = self._generate_renko(data, self._atr_brick_logic)
            if self.save_data:
                self._save_data()
        return self.renko_data

    def _generate_renko(self, data: pd.DataFrame, brick_logic_func) -> pd.DataFrame:
        """
        通用砖型图生成主流程
        """
        renko_data = []
        current_price = data['Close'].iloc[0]
        index = 0
        for i in range(1, len(data)):
            current_price, index = brick_logic_func(data, i, current_price, index, renko_data)

        # ATR模式下补最后一块不完整砖
        if brick_logic_func == self._atr_brick_logic and renko_data:
            self._append_incomplete_brick(data, renko_data, index)
        return pd.DataFrame(renko_data)

    def _daily_brick_logic(self, data: pd.DataFrame, i: int, current_price: float, index: int, renko_data: list):
        """
        日K线模式下的砖块生成逻辑
        """
        price = data['Close'].iloc[i]
        price_change = price - current_price
        if price_change > 0:
            renko_data.append(self._make_brick(index, data.index[i], current_price, price, current_price, price, 1))
            index += 1
        elif price_change < 0:
            renko_data.append(self._make_brick(index, data.index[i], current_price, current_price, price, price, -1))
            index += 1
        return price, index

    def _atr_brick_logic(self, data: pd.DataFrame, i: int, current_price: float, index: int, renko_data: list):
        """
        ATR模式下的砖块生成逻辑
        """
        price = data['Close'].iloc[i]
        price_change = price - current_price
        num_bricks = abs(int(price_change / self.brick_size))
        if num_bricks > 0:
            direction = 1 if price_change > 0 else -1
            for _ in range(num_bricks):
                open_price = current_price
                close_price = current_price + direction * self.brick_size
                # 合并横盘砖块
                if renko_data and (close_price == renko_data[-1]['open'] or close_price == renko_data[-1]['close']):
                    renko_data.pop()
                renko_data.append(self._make_brick(index, data.index[i], open_price, max(open_price, close_price), min(open_price, close_price), close_price, direction))
                current_price = close_price
                index += 1
        return current_price, index

    def _append_incomplete_brick(self, data: pd.DataFrame, renko_data: list, index: int):
        """
        补充最后一块不完整砖（ATR模式专用）
        """
        last_brick_date = renko_data[-1]['date']
        last_k_date = data.index[-1]
        if last_brick_date != last_k_date:
            last_brick_price = renko_data[-1]['close']
            last_k_price = data['Close'].iloc[-1]
            renko_data.append(self._make_brick(
                index, last_k_date, last_brick_price,
                max(last_brick_price, last_k_price),
                min(last_brick_price, last_k_price),
                last_k_price, 0
            ))

    def _make_brick(self, index, date, open_, high, low, close, trend):
        """
        统一砖块字典生成
        """
        return {
            'index': index,
            'date': date,
            'open': open_,
            'high': high,
            'low': low,
            'close': close,
            'trend': trend
        }

    def _calculate_atr(self, data: pd.DataFrame) -> float:
        """
        计算ATR值
        
        Args:
            data (pd.DataFrame): 原始K线数据
            
        Returns:
            float: ATR值
        """
        high = data['High']
        low = data['Low']
        close = data['Close']
        
        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(self.atr_period).mean().iloc[-1]
        
        return atr * self.atr_multiplier
        
    def get_brick_size(self) -> Optional[float]:
        """
        获取当前砖块大小
        
        Returns:
            float: 砖块大小
        """
        return self.brick_size 

    def _save_data(self):
        """
        保存砖型图数据到文件
        """
        if self.renko_data.empty or self.symbol is None:
            self.logger.warning("砖型图数据为空或未设置symbol，未保存文件。")
            return
        start_date = self.renko_data.iloc[0]['date']
        end_date = self.renko_data.iloc[-1]['date']
        # 兼容date为datetime或str
        if hasattr(start_date, 'strftime'):
            start_date = start_date.strftime('%Y-%m-%d')
        if hasattr(end_date, 'strftime'):
            end_date = end_date.strftime('%Y-%m-%d')
        file_name = f"data/{self.symbol}-renko_data-{start_date}-{end_date}.csv"
        self.renko_data.to_csv(file_name, index=False)
        self.logger.info(f"砖型图数据已保存至: {file_name}") 