import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os
import akshare as ak
import json
from database import DataBase

class DataFetcher:
    def __init__(self, cache_dir='data'):
        """
        初始化数据获取器
        
        Args:
            cache_dir (str): 本地缓存目录
        """
        self.data_cache = {}
        self.symbol_info_db = {}

        # 创建缓存目录
        self.cache_dir = cache_dir
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
        
        self.FILE_STOCK_INFO_SH = os.path.join(self.cache_dir, "stock_info_sh.csv")
        self.FILE_STOCK_INFO_SH_KCB = os.path.join(self.cache_dir, "stock_info_sh_kcb.csv")
        self.FILE_STOCK_INFO_SZ = os.path.join(self.cache_dir, "stock_info_sz.csv")
        self.FILE_STOCK_INFO_HK = os.path.join(self.cache_dir, "stock_info_hk.csv")
        self.FILE_STOCK_INFO_AH_CODE_NAME = os.path.join(self.cache_dir, "stock_info_ah_code_name.csv")
        self.FILE_STOCK_AH_CODES_ALL = os.path.join(self.cache_dir, "stock_ah_codes_all.json")
        
        self.db = DataBase(os.path.join(self.cache_dir, 'hist_data.db'))
            
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

        if not symbol.endswith('.HK') and not symbol.isdigit():
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
            
        # 检查数据库
        df = self.db.fetch(symbol, start_date, end_date, interval)
        if df is not None:
            self.data_cache[cache_key] = df
            return df
        
        # 检查本地缓存
        cache_file = self._get_cache_filename(symbol, start_date, end_date, interval)
        cached_df = self._load_from_cache(cache_file)
        if cached_df is not None:
           self.data_cache[cache_key] = cached_df
           self.db.insert(symbol, cached_df, interval)
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
            # 保存到数据库
            self.db.insert(symbol, df, interval)
            return df
        except Exception as e:
            print(f"获取数据时发生错误: {str(e)}")
            return None 

    def get_symbol_name(self, symbol):
        """
        获取股票的基本信息
        
        Args:
            symbol (str): 股票代码
            
        Returns:
            dict: 包含股票基本信息的字典，如果获取失败则返回None
        """
        try:
            symbol_name = self.symbol_info_db[symbol]
            return symbol_name
        except Exception as e:
            print(f"获取股票信息时发生错误: {str(e)}")
            return None 

    def init_stock_info(self):
        """
        获取A股、科创板、深市、港股的股票信息，保存为csv和json文件。
        """
        if os.path.exists(self.FILE_STOCK_INFO_AH_CODE_NAME) and len(self.symbol_info_db) == 0:
            df = pd.read_csv(self.FILE_STOCK_INFO_AH_CODE_NAME, dtype=str)
            self.symbol_info_db = dict(zip(df['code'], df['name']))
            print(f"已加载{len(self.symbol_info_db)}只股票信息")
            return

        if not os.path.exists(self.FILE_STOCK_INFO_SH):
            stock_info_sh_df = ak.stock_info_sh_name_code(symbol="主板A股")
            stock_info_sh_df["证券代码"] = stock_info_sh_df["证券代码"].astype(str)
            stock_info_sh_df.to_csv(self.FILE_STOCK_INFO_SH, index=False)
        else:
            stock_info_sh_df = pd.read_csv(self.FILE_STOCK_INFO_SH, dtype={"证券代码": str})

        if not os.path.exists(self.FILE_STOCK_INFO_SH_KCB):
            stock_info_sh_df_kcb = ak.stock_info_sh_name_code(symbol="科创板")
            stock_info_sh_df_kcb["证券代码"] = stock_info_sh_df_kcb["证券代码"].astype(str)
            stock_info_sh_df_kcb.to_csv(self.FILE_STOCK_INFO_SH_KCB, index=False)
        else:
            stock_info_sh_df_kcb = pd.read_csv(self.FILE_STOCK_INFO_SH_KCB, dtype={"证券代码": str})

        if not os.path.exists(self.FILE_STOCK_INFO_SZ):
            stock_info_sz_df = ak.stock_info_sz_name_code(symbol="A股列表")
            stock_info_sz_df["A股代码"] = stock_info_sz_df["A股代码"].astype(str)
            stock_info_sz_df.to_csv(self.FILE_STOCK_INFO_SZ, index=False)
        else:
            stock_info_sz_df = pd.read_csv(self.FILE_STOCK_INFO_SZ, dtype={"A股代码": str})

        if not os.path.exists(self.FILE_STOCK_INFO_HK):
            stock_info_hk_df = ak.stock_hk_spot_em()
            stock_info_hk_df['代码'] = stock_info_hk_df['代码'].astype(str)
            stock_info_hk_df.to_csv(self.FILE_STOCK_INFO_HK, index=False)
        else:
            stock_info_hk_df = pd.read_csv(self.FILE_STOCK_INFO_HK, dtype={"代码": str})

        # 统一字段名
        sh_df = stock_info_sh_df.rename(columns={"证券代码": "code", "证券简称": "name"})[["code", "name"]]
        kcb_df = stock_info_sh_df_kcb.rename(columns={"证券代码": "code", "证券简称": "name"})[["code", "name"]]
        sz_df = stock_info_sz_df.rename(columns={"A股代码": "code", "A股简称": "name"})[["code", "name"]]
        hk_df = stock_info_hk_df.rename(columns={"代码": "code", "名称": "name"})[["code", "name"]]
        hk_df['code'] = hk_df['code'].apply(lambda x: f"{x}.HK")

        # 合并
        all_df = pd.concat([sh_df, kcb_df, sz_df, hk_df], ignore_index=True)
        all_df.to_csv(self.FILE_STOCK_INFO_AH_CODE_NAME, index=False)

        # code单独保存为json
        with open(self.FILE_STOCK_AH_CODES_ALL, 'w', encoding='utf-8') as f:
            json.dump(all_df['code'].tolist(), f, ensure_ascii=False, indent=2) 