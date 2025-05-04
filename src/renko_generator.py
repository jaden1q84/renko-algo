import pandas as pd
import numpy as np
from typing import Literal

class RenkoGenerator:
    def __init__(self, mode: Literal['daily', 'atr'] = 'daily', atr_period: int = 10, atr_multiplier: float = 0.5):
        """
        初始化砖型图生成器
        
        Args:
            mode (str): 生成模式，'daily'表示基于日K线，'atr'表示基于ATR
            atr_period (int): ATR计算周期
            atr_multiplier (float): ATR倍数，用于调整砖块大小
        """
        self.mode = mode
        self.atr_period = atr_period
        self.atr_multiplier = atr_multiplier
        self.renko_data = pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'trend'])
        
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
        
    def generate_renko(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        生成砖型图数据
        
        Args:
            data (pd.DataFrame): 原始K线数据
            
        Returns:
            pd.DataFrame: 砖型图数据
        """
        if self.mode == 'daily':
            return self._generate_daily_renko(data)
        else:
            return self._generate_atr_renko(data)
            
    def _generate_daily_renko(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        基于日K线生成砖型图
        
        Args:
            data (pd.DataFrame): 原始K线数据
            
        Returns:
            pd.DataFrame: 砖型图数据
        """
        renko_data = []
        current_price = data['Close'].iloc[0]
        
        for i in range(1, len(data)):
            price = data['Close'].iloc[i]
            price_change = price - current_price
            
            if price_change > 0:
                renko_data.append({
                    'date': data.index[i],
                    'open': current_price,
                    'high': price,
                    'low': current_price,
                    'close': price,
                    'trend': 1
                })
            elif price_change < 0:
                renko_data.append({
                    'date': data.index[i],
                    'open': current_price,
                    'high': current_price,
                    'low': price,
                    'close': price,
                    'trend': -1
                })
            
            current_price = price
                        
        self.renko_data = pd.DataFrame(renko_data)
        return self.renko_data
        
    def _generate_atr_renko(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        基于ATR生成砖型图
        
        Args:
            data (pd.DataFrame): 原始K线数据
            
        Returns:
            pd.DataFrame: 砖型图数据
        """
        renko_data = []
        current_price = data['Close'].iloc[0]
        brick_size = self._calculate_atr(data)
        print(f"ATR计算的砖块大小为: {brick_size:.2f}")

        for i in range(1, len(data)):
            price = data['Close'].iloc[i]
            price_change = price - current_price
            
            # 计算需要多少个砖块
            num_bricks = abs(int(price_change / brick_size))
            
            if num_bricks > 0:
                direction = 1 if price_change > 0 else -1
                for _ in range(num_bricks):
                    if direction > 0:
                        renko_data.append({
                            'date': data.index[i],
                            'open': current_price,
                            'high': current_price + brick_size,
                            'low': current_price,
                            'close': current_price + brick_size,
                            'trend': 1
                        })
                        current_price += brick_size
                    else:
                        renko_data.append({
                            'date': data.index[i],
                            'open': current_price,
                            'high': current_price,
                            'low': current_price - brick_size,
                            'close': current_price - brick_size,
                            'trend': -1
                        })
                        current_price -= brick_size
                        
        self.renko_data = pd.DataFrame(renko_data)
        return self.renko_data 