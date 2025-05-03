import pandas as pd
import numpy as np

class RenkoGenerator:
    def __init__(self, brick_size):
        """
        初始化砖型图生成器
        
        Args:
            brick_size (float): 砖块大小，可以是固定值或ATR的倍数
        """
        self.brick_size = brick_size
        self.renko_data = pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'trend'])
        
    def calculate_atr(self, data, period=14):
        """
        计算ATR（平均真实波幅）
        
        Args:
            data (pd.DataFrame): 包含OHLC数据的数据框
            period (int): ATR计算周期
            
        Returns:
            pd.Series: ATR值
        """
        high = data['High']
        low = data['Low']
        close = data['Close']
        
        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(period).mean()
        
        return atr
        
    def generate_renko(self, data):
        """
        生成砖型图数据
        
        Args:
            data (pd.DataFrame): 原始K线数据
            
        Returns:
            pd.DataFrame: 砖型图数据
        """
        if isinstance(self.brick_size, str) and self.brick_size.startswith('ATR'):
            # 如果brick_size是ATR的倍数
            multiplier = float(self.brick_size.split('*')[1])
            atr = self.calculate_atr(data)
            brick_size = atr * multiplier
        else:
            brick_size = self.brick_size
            
        renko_data = []
        current_price = data['Close'].iloc[0]
        current_trend = 1  # 1表示上升趋势，-1表示下降趋势
        
        for i in range(1, len(data)):
            price = data['Close'].iloc[i]
            price_change = price - current_price
            
            if abs(price_change) >= brick_size:
                num_bricks = int(abs(price_change) / brick_size)
                
                for _ in range(num_bricks):
                    if price_change > 0:
                        # 上升砖块
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
                        # 下降砖块
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