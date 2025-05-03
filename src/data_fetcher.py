import tushare as ts
import pandas as pd
from datetime import datetime

class DataFetcher:
    def __init__(self, token=None):
        """
        初始化数据获取器
        
        Args:
            token (str, optional): tushare的token，如果不提供则使用默认token
        """
        self.data_cache = {}
        if token:
            ts.set_token(token)
        self.pro = ts.pro_api()
        
    def get_historical_data(self, symbol, start_date, end_date=None, interval='1d'):
        """
        获取历史数据
        
        Args:
            symbol (str): 股票代码（A股需要加.SZ或.SH后缀）
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
            # 转换日期格式
            start_date = start_date.replace('-', '')
            end_date = end_date.replace('-', '')
            
            # 获取数据
            df = self.pro.daily(ts_code=symbol, start_date=start_date, end_date=end_date)
            
            if df.empty:
                raise ValueError(f"无法获取{symbol}的数据")
                
            # 重命名列以匹配标准格式
            df = df.rename(columns={
                'trade_date': 'Date',
                'open': 'Open',
                'high': 'High',
                'low': 'Low',
                'close': 'Close',
                'vol': 'Volume'
            })
            
            # 转换日期格式
            df['Date'] = pd.to_datetime(df['Date'])
            df = df.set_index('Date')
            df = df.sort_index()
            
            self.data_cache[cache_key] = df
            return df
            
        except Exception as e:
            print(f"获取数据时发生错误: {str(e)}")
            return None 