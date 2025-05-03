import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

class DataFetcher:
    def __init__(self):
        self.data_cache = {}

    def get_historical_data(self, symbol, start_date, end_date=None, interval='1d'):
        """
        获取历史数据
        
        Args:
            symbol (str): 股票代码
            start_date (str): 开始日期，格式为'YYYY-MM-DD'
            end_date (str, optional): 结束日期，格式为'YYYY-MM-DD'，默认为今天
            interval (str, optional): 数据间隔，默认为'1d'
            
        Returns:
            pd.DataFrame: 包含OHLCV数据的数据框
        """
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
            
        cache_key = f"{symbol}_{start_date}_{end_date}_{interval}"
        if cache_key in self.data_cache:
            return self.data_cache[cache_key]
            
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(start=start_date, end=end_date, interval=interval)
            
            if df.empty:
                raise ValueError(f"无法获取{symbol}的数据")
                
            # 确保数据包含所有必要的列
            required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            if not all(col in df.columns for col in required_columns):
                raise ValueError(f"数据缺少必要的列: {required_columns}")
                
            self.data_cache[cache_key] = df
            return df
            
        except Exception as e:
            print(f"获取数据时发生错误: {str(e)}")
            return None 