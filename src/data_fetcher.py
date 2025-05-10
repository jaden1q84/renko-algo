import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os
import akshare as ak

class DataFetcher:
    def __init__(self, cache_dir='data'):
        """
        初始化数据获取器
        
        Args:
            cache_dir (str): 本地缓存目录
        """
        self.data_cache = {}
        self.symbol_info = None
        self.symbol_info_db = {}
        
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
            symbol (str): 股票代码
            start_date (str): 开始日期，格式为'YYYY-MM-DD'
            end_date (str, optional): 结束日期，格式为'YYYY-MM-DD'，默认为今天
            interval (str, optional): 数据间隔，可选值：'1d', '1wk', '1mo'（akshare不支持分钟级别）
        Returns:
            pd.DataFrame: 包含OHLCV数据的数据框
        """

        if not symbol.endswith('.HK') and not symbol.endswith('.SZ') and not symbol.endswith('.SS'):
            raise ValueError(f"A/H股仅支持港股代码，收到: {symbol}")
        
        # 如果symbol是数字，则认为是港股
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
            
        # interval到period的映射
        interval_map = {'1d': 'daily', '1wk': 'weekly', '1mo': 'monthly'}
        if interval not in interval_map:
            raise ValueError(f"A/H股仅支持interval为'1d', '1wk', '1mo'，收到: {interval}")
        period = interval_map[interval]  
              
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
            # 判断股票类型（A股/港股）
            if symbol.endswith('.HK'):
                df = ak.stock_hk_hist(symbol=symbol.split('.')[0], period=period, start_date=start_date.replace('-', ''), end_date=end_date.replace('-', ''), adjust="qfq")
            else:
                df = ak.stock_zh_a_hist(symbol=symbol.split('.')[0], period=period, start_date=start_date.replace('-', ''), end_date=end_date.replace('-', ''), adjust="qfq")
            # 字段兼容
            df.rename(columns={
                '日期': 'Date',
                '开盘': 'Open',
                '收盘': 'Close',
                '最高': 'High',
                '最低': 'Low',
                '成交量': 'Volume',
                '成交额': 'Turnover',
            }, inplace=True)
            df['Date'] = pd.to_datetime(df['Date'])
            df.set_index('Date', inplace=True)

            # 保存到内存缓存
            self.data_cache[cache_key] = df
            # 保存到本地缓存
            self._save_to_cache(df, cache_file)
            return df
        except Exception as e:
            print(f"获取数据时发生错误: {str(e)}")
            return None 

    def get_info(self, symbol):
        """
        获取股票的基本信息
        
        Args:
            symbol (str): 股票代码
            
        Returns:
            dict: 包含股票基本信息的字典，如果获取失败则返回None
        """
        try:
            # 如果symbol_info_db为空，则从CSV文件加载股票代码和名称
            if len(self.symbol_info_db) == 0:
                self._load_symbol_info_from_csv()

            symbol = symbol.split('.')[0]
            self.symbol_info = self.symbol_info_db[symbol]
            print(f"****************获取股票信息: {self.symbol_info}")
            return self.symbol_info
        except Exception as e:
            print(f"获取股票信息时发生错误: {str(e)}")
            return None 

    def _load_symbol_info_from_csv(self, csv_path='data/stock_info_a_code_name.csv'):
        """
        从CSV文件加载股票代码和名称到 symbol_info_db

        Args:
            csv_path (str): CSV文件路径
        """
        try:
            # 检查文件是否存在
            if not os.path.exists(csv_path):
                # 如果不存在，自动用 akshare 获取并保存
                df = ak.stock_info_a_code_name()
                os.makedirs(os.path.dirname(csv_path), exist_ok=True)
                df.to_csv(csv_path, index=False)
                print(f"已自动下载股票信息到 {csv_path}")

            df = pd.read_csv(csv_path, dtype=str)
            # 构建字典，key为code，value为name
            self.symbol_info_db = dict(zip(df['code'], df['name']))
            print(f"已加载{len(self.symbol_info_db)}只股票信息")
        except Exception as e:
            print(f"加载股票信息失败: {str(e)}") 