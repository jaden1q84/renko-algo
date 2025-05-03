import pandas as pd
import numpy as np

class RenkoGenerator:
    def __init__(self):
        """
        初始化砖型图生成器
        """
        self.renko_data = pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'trend'])
        
    def generate_renko(self, data):
        """
        生成砖型图数据，基于日K线涨跌
        
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
                # 上升砖块
                renko_data.append({
                    'date': data.index[i],
                    'open': current_price,
                    'high': price,
                    'low': current_price,
                    'close': price,
                    'trend': 1
                })
            elif price_change < 0:
                # 下降砖块
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