import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os
import akshare as ak
import json
from database import DataBase
import logging
class DataFetcher:
    def __init__(self, cache_dir='data', use_db_cache=True, use_csv_cache=True, query_method='akshare'):
        """
        初始化数据获取器
        
        Args:
            cache_dir (str): 本地缓存目录
            use_db_cache (bool): 是否使用数据库缓存
            use_csv_cache (bool): 是否使用本地csv缓存
            query_method (str): 数据源方式
        """
        self.data_cache = {}
        self.symbol_info_db = {}
        self.use_db_cache = use_db_cache
        self.use_csv_cache = use_csv_cache
        self.query_method = query_method
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
        # 配置日志
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')
        self.logger = logging.getLogger(__name__)
            
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
                self.logger.error(f"读取缓存文件失败: {str(e)}")
        return None
        
    def _save_to_cache(self, df, cache_file):
        """保存数据到缓存文件"""
        try:
            df.to_csv(cache_file)
        except Exception as e:
            self.logger.error(f"保存缓存文件失败: {str(e)}")
        
    def get_historical_data(self, symbol, start_date, end_date=None, interval='1d', query_method=None):
        """
        获取历史数据
        
        Args:
            symbol (str): 股票代码
            start_date (str): 开始日期，格式为'YYYY-MM-DD'
            end_date (str, optional): 结束日期，格式为'YYYY-MM-DD'，默认为今天
            interval (str, optional): 数据间隔，可选值：'1d', '1wk', '1mo'（akshare不支持分钟级别）
            query_method (str, optional): 数据源方式
        Returns:
            pd.DataFrame: 包含OHLCV数据的数据框
        """
        if query_method is None:
            query_method = self.query_method
        partial_data = False
        db_end_date = None
        db_df = pd.DataFrame()

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
        if self.use_db_cache:
            db_df = self.db.fetch(symbol, start_date, end_date, interval)
            if db_df is not None and not db_df.empty:
                # 检查是否包含end_date
                db_end_date = pd.to_datetime(db_df.index[-1]).strftime('%Y-%m-%d')
                if db_end_date == end_date:
                    self.data_cache[cache_key] = db_df
                    return db_df
                else:
                    self.logger.info(f"数据库中没有{db_end_date} - {end_date}的数据，需要从网上获取")
                    partial_data = True # 数据库中没有end_date的数据，需要从akshare获取
        
        # 检查本地缓存
        cache_file = self._get_cache_filename(symbol, start_date, end_date, interval)
        if self.use_csv_cache:
            cached_df = self._load_from_cache(cache_file)
            if cached_df is not None:
                self.data_cache[cache_key] = cached_df
                if self.use_db_cache:
                    self.db.insert(symbol, cached_df, interval)
                return cached_df
        
        try:
            # 判断股票类型（A股/港股）
            all_df = pd.DataFrame()
            query_df = pd.DataFrame()
            query_start_date = start_date if not partial_data else (pd.to_datetime(db_end_date) + timedelta(days=1)).strftime('%Y-%m-%d')
            query_end_date = (pd.to_datetime(end_date) + timedelta(days=1)).strftime('%Y-%m-%d')
            self.logger.info(f"使用{query_method}获取{symbol}从{query_start_date}到{query_end_date}的数据")

            if query_method == 'akshare':
                if symbol.endswith('.HK'):
                    query_df = ak.stock_hk_hist(symbol=symbol.split('.')[0], period=period, 
                                        start_date=query_start_date.replace('-', ''), end_date=query_end_date.replace('-', ''), adjust="qfq")
                else:
                    query_df = ak.stock_zh_a_hist(symbol=symbol.split('.')[0], period=period, 
                                            start_date=query_start_date.replace('-', ''), end_date=query_end_date.replace('-', ''), adjust="qfq")
                # 字段兼容
                query_df.rename(columns={
                    '日期': 'Date',
                    '开盘': 'Open',
                    '收盘': 'Close',
                    '最高': 'High',
                    '最低': 'Low',
                    '成交量': 'Volume',
                    '成交额': 'Turnover',
                }, inplace=True)

            elif query_method == 'yfinance':
                # 如果symbol不是.HK，则认为是A股，6开头需要补.SS结尾，否则补.SZ结尾
                yf_symbol = symbol
                if not symbol.endswith('.HK'):
                    if symbol.startswith('6'):
                        yf_symbol = f"{symbol}.SS"
                    else:
                        yf_symbol = f"{symbol}.SZ"

                ticker = yf.Ticker(yf_symbol)
                query_df = ticker.history(start=query_start_date, end=query_end_date, interval=interval)
            else:
                raise ValueError(f"不支持的query_method: {query_method}")

            if not query_df.empty:
                if 'Date' not in query_df.columns:
                    query_df = query_df.reset_index()

                # 保持DB兼容性，添加 Turnover 列，默认值为 -1
                if 'Turnover' not in query_df.columns:
                    query_df['Turnover'] = -1

                # Date列去掉时区，只保留日期
                query_df['Date'] = query_df['Date'].dt.date
                query_df['Date'] = pd.to_datetime(query_df['Date'])
                query_df.set_index('Date', inplace=True)

                self.logger.info(f"query_df前3条数据:")
                self.logger.info(query_df.head(3))

            if partial_data:
                if not query_df.empty:
                    all_df = pd.concat([db_df, query_df])
                else:
                    all_df = db_df
            else:
                all_df = query_df

            # 保存到内存缓存
            self.data_cache[cache_key] = all_df 

            # 保存到本地缓存
            if self.use_csv_cache:
                self._save_to_cache(all_df, cache_file)

            # 保存到数据库
            if self.use_db_cache:
                self.db.insert(symbol, query_df, interval)

            return all_df
        except Exception as e:
            self.logger.error(f"获取数据时发生错误: {str(e)}")
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
            self.logger.error(f"获取股票信息时发生错误: {str(e)}")
            return None 

    def init_stock_info(self):
        """
        获取A股、科创板、深市、港股的股票信息，保存为csv和json文件。
        """
        if os.path.exists(self.FILE_STOCK_INFO_AH_CODE_NAME) and len(self.symbol_info_db) == 0:
            df = pd.read_csv(self.FILE_STOCK_INFO_AH_CODE_NAME, dtype=str)
            self.symbol_info_db = dict(zip(df['code'], df['name']))
            self.logger.info(f"已加载{len(self.symbol_info_db)}只股票信息")
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