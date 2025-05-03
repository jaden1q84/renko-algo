import tushare as ts
import pandas as pd
from datetime import datetime
import os
import json

class DataFetcher:
    def __init__(self, token=None, cache_dir='data'):
        """
        初始化数据获取器
        
        Args:
            token (str, optional): tushare的token，如果不提供则使用默认token
            cache_dir (str): 本地缓存目录
        """
        self.data_cache = {}
        if token:
            ts.set_token(token)
        self.pro = ts.pro_api()
        
        # 创建缓存目录
        self.cache_dir = cache_dir
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
            
    def _get_cache_filename(self, symbol, start_date, end_date, interval):
        """生成缓存文件名"""
        return os.path.join(self.cache_dir, f"{symbol}_{start_date}_{end_date}_{interval}.csv")
        
    def _load_from_cache(self, cache_file):
        """从缓存文件加载数据"""
        if os.path.exists(cache_file):
            try:
                df = pd.read_csv(cache_file, index_col=0, parse_dates=True)
                return df
            except Exception as e:
                print(f"读取缓存文件失败: {str(e)}")
        return None
        
    def _save_to_cache(self, df, cache_file):
        """保存数据到缓存文件"""
        try:
            df.to_csv(cache_file)
        except Exception as e:
            print(f"保存缓存文件失败: {str(e)}")
        
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
            
        # 检查内存缓存
        cache_key = f"{symbol}_{start_date}_{end_date}_{interval}"
        if cache_key in self.data_cache:
            return self.data_cache[cache_key]
            
        # 检查本地缓存
        cache_file = self._get_cache_filename(symbol, start_date, end_date, interval)
        cached_df = self._load_from_cache(cache_file)
        if cached_df is not None:
            self.data_cache[cache_key] = cached_df
            return cached_df
            
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
            
            # 保存到内存缓存
            self.data_cache[cache_key] = df
            
            # 保存到本地缓存
            self._save_to_cache(df, cache_file)
            
            return df
            
        except Exception as e:
            print(f"获取数据时发生错误: {str(e)}")
            return None 