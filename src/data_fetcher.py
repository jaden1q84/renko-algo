import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os
import akshare as ak
import json
import logging
from src.database import DataBase

class DataFetcher:
    # 配置常量
    SUPPORTED_INTERVALS = ['1d', '1wk', '1mo']
    INTERVAL_TO_PERIOD_MAP = {'1d': 'daily', '1wk': 'weekly', '1mo': 'monthly'}
    DEFAULT_CACHE_DIR = 'data'
    DEFAULT_QUERY_METHOD = 'yfinance'
    
    def __init__(self, cache_dir=DEFAULT_CACHE_DIR, use_db_cache=True, use_csv_cache=False, query_method=DEFAULT_QUERY_METHOD):
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
        
        # 初始化文件路径
        self._init_file_paths()
        
        # 初始化数据库和日志
        self.db = DataBase(os.path.join(self.cache_dir, 'stock_hist_data.db'))
        self.logger = logging.getLogger(__name__)
            
    def _init_file_paths(self):
        """初始化所有文件路径"""
        self.FILE_STOCK_INFO_SH = os.path.join(self.cache_dir, "stock_info_sh.csv")
        self.FILE_STOCK_INFO_SH_KCB = os.path.join(self.cache_dir, "stock_info_sh_kcb.csv")
        self.FILE_STOCK_INFO_SZ = os.path.join(self.cache_dir, "stock_info_sz.csv")
        self.FILE_STOCK_INFO_HK = os.path.join(self.cache_dir, "stock_info_hk.csv")
        self.FILE_STOCK_INFO_AH_CODE_NAME = os.path.join(self.cache_dir, "stock_info_ah_symbol_name.csv")
        self.FILE_STOCK_AH_SYMBOLS_ALL = os.path.join(self.cache_dir, "stock_ah_symbols_all.json")
        
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

    def _get_cache_key(self, symbol, start_date, end_date, interval):
        """生成缓存键"""
        return f"{symbol}_{start_date}_{end_date}_{interval}"

    def _get_from_memory_cache(self, cache_key):
        """从内存缓存获取数据"""
        return self.data_cache.get(cache_key)

    def _save_to_memory_cache(self, cache_key, df):
        """保存数据到内存缓存"""
        self.data_cache[cache_key] = df

    def _get_from_db_cache(self, symbol, start_date, end_date, interval):
        """从数据库缓存获取数据"""
        if not self.use_db_cache:
            return None
        return self.db.fetch(symbol, start_date, end_date, interval)

    def _get_from_file_cache(self, symbol, start_date, end_date, interval):
        """从文件缓存获取数据"""
        if not self.use_csv_cache:
            return None
        cache_file = self._get_cache_filename(symbol, start_date, end_date, interval)
        return self._load_from_cache(cache_file)

    def _process_query_result(self, query_df, symbol, start_date, end_date, interval):
        """处理查询结果，统一数据格式"""
        if query_df.empty:
            self.logger.warning(f"警告: {symbol} 没有获取到数据")
            return None

        # 重置索引，确保Date列存在
        if 'Date' not in query_df.columns:
            query_df = query_df.reset_index()

        # 保持DB兼容性，添加 Turnover 列，默认值为 -1
        if 'Turnover' not in query_df.columns:
            query_df['Turnover'] = -1

        # Date列去掉时区，只保留日期
        query_df['Date'] = query_df['Date'].dt.date
        query_df['Date'] = pd.to_datetime(query_df['Date'])
        query_df.set_index('Date', inplace=True)

        # 添加入库时间戳
        query_df['Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        self.logger.info(f"[DONE]获取了 {symbol}@{query_df.index.min()} -> {query_df.index.max()} 的 {len(query_df)} 条数据")
        return query_df

    def _save_query_result(self, query_df, symbol, start_date, end_date, interval):
        """保存查询结果到各种缓存"""
        if query_df is None or query_df.empty:
            return

        # 保存到文件缓存
        if self.use_csv_cache:
            cache_file = self._get_cache_filename(symbol, start_date, end_date, interval)
            self._save_to_cache(query_df, cache_file)

        # 保存到内存缓存
        cache_key = self._get_cache_key(symbol, start_date, end_date, interval)
        self._save_to_memory_cache(cache_key, query_df)

        # 保存到数据库
        if self.use_db_cache:
            self.db.insert(symbol, query_df, interval)

    def _get_db_first_date(self, symbol, interval='1d'):
        """
        获取第一条数据日期
        """
        db_first_date = self.db.get_first_date(symbol, interval)
        if db_first_date is None:
            return None
        else:
            return pd.to_datetime(db_first_date)

    def _get_db_last_date(self, symbol, interval='1d'):
        """
        检查最后一条数据日期
        """
        db_last_date = self.db.get_last_date(symbol, interval)
        if db_last_date is None:
            return None
        else:
            return pd.to_datetime(db_last_date)
        
    def _prepare_params(self, symbol, start_date, end_date, interval):
        """
        入参检查和处理
        """
         # 入参检查和处理
        adj_symbol = symbol
        adj_start_date = start_date
        adj_end_date = end_date

        if symbol.endswith('.SS') or symbol.endswith('.SH') or symbol.endswith('.SZ'):
            adj_symbol = symbol.split('.')[0]
        elif symbol.endswith('.HK'):
            pass
        elif not symbol.isdigit():
            raise ValueError(f"不支持的股票代码: {symbol}")

        if end_date is None:
            adj_end_date = datetime.now().strftime('%Y-%m-%d')

        # 如果start_date不是工作日，则调整为最近一个工作日
        if not self._is_workday(start_date):
            adj_start_date = self._get_nearest_workday_forward(start_date)
            self.logger.info(f"start_date不是工作日，调整为最近一个工作日: {start_date} -> {adj_start_date}")

        # 如果end_date不是工作日，则调整为最近一个工作日
        if not self._is_workday(end_date):
            adj_end_date = self._get_nearest_workday_backward(end_date)
            self.logger.info(f"end_date不是工作日，调整为最近一个工作日: {end_date} -> {adj_end_date}")
        
        # interval到period的映射，用于兼容akshare接口
        interval_valid = ['1d', '1wk', '1mo']
        if interval not in interval_valid:
            raise ValueError(f"A/H股仅支持interval为'1d', '1wk', '1mo'，收到: {interval}")

        return adj_symbol, adj_start_date, adj_end_date, interval

    def prepare_db_data(self, symbol, start_date, end_date=None, interval='1d'):
        """
        根据参数准备数据库的数据，对比数据库和网络最新数据，并更新到数据库
        返回True表示数据库数据就绪，False标识失败。
        """
        # 入参检查和处理
        adj_symbol, adj_start_date, adj_end_date, interval = self._prepare_params(symbol, start_date, end_date, interval)
        
        if not self.use_db_cache:
            # 如果不用数据库，则直接返回
            self.logger.info(f"不使用数据库缓存，直接返回False")
            return False
        need_update_data = False

        # 数据库中有数据不是最终收盘数据，检查记录的入库时间戳，标注要执行update动作。
        db_df = pd.DataFrame()
        db_df = self.db.fetch(adj_symbol, adj_start_date, adj_end_date, interval)
        if not db_df.empty:
            for index, row in db_df.iterrows():
                if row['Timestamp'] and pd.to_datetime(row['Timestamp']) < pd.to_datetime(f"{index} 16:15:00"): # 入库时间戳较收盘时间早，标记需要特殊处理，考虑港股要取16:15:00
                    self.logger.info(f"[CHECK]股票{adj_symbol}数据库中存在{index}的盘中数据，入库时间戳为{row['Timestamp']}，较收盘时间早，标记需要特殊处理")
                    need_update_data = True

        # 开始检查数据库数据情况
        db_start_date = self._get_db_first_date(adj_symbol, interval)
        db_end_date = self._get_db_last_date(adj_symbol, interval)

        # 检查数据库中是否包含start_date到end_date的数据
        if not need_update_data and db_start_date is not None and db_end_date is not None:
            self.logger.info(f"[CHECK]股票{adj_symbol}最新历史数据范围: {db_start_date.strftime('%Y-%m-%d')} -> {db_end_date.strftime('%Y-%m-%d')}")
            if db_start_date > pd.to_datetime(adj_end_date):
                adj_end_date = (db_start_date - timedelta(days=1)).strftime('%Y-%m-%d') # ok，准备的数据比数据库中数据段更早，则调整adj_end_date为数据库中数据段的前1天
            elif db_end_date < pd.to_datetime(adj_start_date):
                adj_start_date = (db_end_date + timedelta(days=1)).strftime('%Y-%m-%d') # ok，准备的数据比数据库中数据段更晚，则调整adj_start_date为数据库中数据段的后1天
            elif pd.to_datetime(adj_start_date) < db_start_date and db_start_date < pd.to_datetime(adj_end_date):
                adj_end_date = (db_start_date - timedelta(days=1)).strftime('%Y-%m-%d') # ok，准备的数据比数据库中数据段更早，但有部分重叠，则调整adj_end_date为数据库中数据段的前1天
            elif pd.to_datetime(adj_start_date) < db_end_date and db_end_date < pd.to_datetime(adj_end_date):
                adj_start_date = (db_end_date + timedelta(days=1)).strftime('%Y-%m-%d') # ok，准备的数据比数据库中数据段更晚，但有部分重叠，则调整adj_start_date为数据库中数据段的后1天
            elif pd.to_datetime(adj_start_date) >= db_start_date and db_end_date >= pd.to_datetime(adj_end_date):
                self.logger.info(f"[DONE]股票{adj_symbol}数据库中已包含 {adj_start_date} -> {adj_end_date} 的数据，数据就绪")
                return False
    
        # 如果数据库中没有数据，则从网上获取数据
        self.logger.info(f"[TODO]: 数据库缺少股票{adj_symbol}@{adj_start_date} -> {adj_end_date}的数据")
        query_df = pd.DataFrame()
        query_df = self._query_stock_data_from_net(adj_symbol, adj_start_date, adj_end_date, interval)
        if isinstance(query_df, bool) and query_df is False:
            return False
        
        query_df = self._process_query_result(query_df, adj_symbol, adj_start_date, adj_end_date, interval)
        if query_df is None:
            return False

        # 保存到数据库
        if need_update_data:
            self.db.update(adj_symbol, query_df, interval)
        else:
            self.db.insert(adj_symbol, query_df, interval)
        self.logger.info(f"[DONE]股票{adj_symbol}@{adj_start_date} -> {adj_end_date}的数据已保存到数据库")

        # 保存到内存缓存，下次匹配直接获取
        cache_key = f"{adj_symbol}_{adj_start_date}_{adj_end_date}_{interval}"
        self.data_cache[cache_key] = query_df

        return True

    def get_historical_data(self, symbol, start_date, end_date=None, interval='1d'):
        """
        获取历史数据
        
        Args:
            symbol (str): 股票代码
            start_date (str): 开始日期，格式为'YYYY-MM-DD'
            end_date (str, optional): 结束日期，格式为'YYYY-MM-DD'，默认为今天
            interval (str, optional): 数据间隔，可选值：'1d', '1wk', '1mo'
        Returns:
            pd.DataFrame: 包含OHLCV数据的数据框
        """
        # 参数预处理
        adj_symbol, adj_start_date, adj_end_date, interval = self._prepare_params(symbol, start_date, end_date, interval)
        
        # 生成缓存键
        cache_key = self._get_cache_key(adj_symbol, adj_start_date, adj_end_date, interval)
        
        # 1. 检查内存缓存
        if cached_df := self._get_from_memory_cache(cache_key):
            return cached_df
            
        # 2. 检查数据库缓存
        if db_df := self._get_from_db_cache(adj_symbol, adj_start_date, adj_end_date, interval):
            if not db_df.empty:
                self._save_to_memory_cache(cache_key, db_df)
                return db_df
            self.logger.warning(f"数据库中没有{adj_symbol}@{adj_start_date} - {adj_end_date}的数据")
        
        # 3. 检查文件缓存
        if csv_df := self._get_from_file_cache(adj_symbol, adj_start_date, adj_end_date, interval):
            if csv_df is not None:
                self._save_to_memory_cache(cache_key, csv_df)
                return csv_df
            self.logger.warning(f"本地缓存中没有{adj_symbol}@{adj_start_date} - {adj_end_date}的数据")
        
        # 4. 从网络获取数据
        self.logger.info(f"[TODO]从网络获取股票{adj_symbol}@{adj_start_date} -> {adj_end_date}的数据")
        query_df = self._query_stock_data_from_net(adj_symbol, adj_start_date, adj_end_date, interval)
        
        # 5. 处理并保存查询结果
        if query_df is not None:
            processed_df = self._process_query_result(query_df, adj_symbol, adj_start_date, adj_end_date, interval)
            if processed_df is not None:
                self._save_query_result(processed_df, adj_symbol, adj_start_date, adj_end_date, interval)
                return processed_df
                
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
            if not symbol.endswith('.HK'):
                symbol = symbol.split('.')[0]
            symbol_name = self.symbol_info_db[symbol]
            return symbol_name
        except Exception as e:
            self.logger.error(f"获取股票信息时发生错误: {str(e)}")
            return None 

    def init_stock_info(self):
        """
        获取A股、科创板、深市、港股的股票信息，保存为csv和json文件。
        """

        self.logger.info("初始化股票信息")

        # 从数据库获取股票信息
        stock_info_db = pd.DataFrame(self.db.get_all_stock_info(), columns=['symbol', 'name'])
        if not stock_info_db.empty:
            self.symbol_info_db = dict(zip(stock_info_db['symbol'], stock_info_db['name']))
            self.logger.info(f"已从数据库加载stock_info_db = {len(self.symbol_info_db)}只股票信息")
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
        sh_df = stock_info_sh_df.rename(columns={"证券代码": "symbol", "证券简称": "name"})[["symbol", "name"]]
        kcb_df = stock_info_sh_df_kcb.rename(columns={"证券代码": "symbol", "证券简称": "name"})[["symbol", "name"]]
        sz_df = stock_info_sz_df.rename(columns={"A股代码": "symbol", "A股简称": "name"})[["symbol", "name"]]
        hk_df = stock_info_hk_df.rename(columns={"代码": "symbol", "名称": "name"})[["symbol", "name"]]
        hk_df['symbol'] = hk_df['symbol'].apply(lambda x: f"{x}.HK")

        # 合并
        all_df = pd.concat([sh_df, kcb_df, sz_df, hk_df], ignore_index=True)
        all_df.to_csv(self.FILE_STOCK_INFO_AH_CODE_NAME, index=False)

        # 保存到数据库
        self.db.update_stock_info(all_df)

        # code单独保存为json
        with open(self.FILE_STOCK_AH_SYMBOLS_ALL, 'w', encoding='utf-8') as f:
            json.dump(all_df['symbol'].tolist(), f, ensure_ascii=False, indent=2) 

    def _query_stock_data_from_net(self, adj_symbol, adj_start_date, adj_end_date, interval):
        """
        从网络获取股票数据
        
        Args:
            adj_symbol (str): 处理后的股票代码
            adj_start_date (str): 开始日期
            adj_end_date (str): 结束日期
            interval (str): 数据间隔
            
        Returns:
            pd.DataFrame: 查询结果，如果失败返回None
        """
        # 调整结束日期，加1天以包含结束日期
        adj_end_date = (pd.to_datetime(adj_end_date) + timedelta(days=1)).strftime('%Y-%m-%d')
        
        try:
            if self.query_method == 'akshare':
                query_df = self._fetch_data_akshare(adj_symbol, adj_start_date, adj_end_date, interval)
            elif self.query_method == 'yfinance':
                query_df = self._fetch_data_yfinance(adj_symbol, adj_start_date, adj_end_date, interval)
            else:
                raise ValueError(f"不支持的query_method: {self.query_method}")
                
            if isinstance(query_df, bool) and query_df is False:
                return None
                
            return query_df
            
        except Exception as e:
            self.logger.error(f"获取股票数据失败: {str(e)}")
            return None

    def _fetch_data_akshare(self, symbol, start_date, end_date, period):
        """
        使用akshare获取A股或港股数据
        
        Args:
            symbol (str): 股票代码
            start_date (str): 开始日期
            end_date (str): 结束日期
            period (str): 数据周期
            
        Returns:
            pd.DataFrame: 查询结果，如果失败返回False
        """
        if period not in self.INTERVAL_TO_PERIOD_MAP:
            raise ValueError(f"A/H股仅支持interval为{self.SUPPORTED_INTERVALS}，收到: {period}")
            
        akshare_period = self.INTERVAL_TO_PERIOD_MAP[period]
        
        try:
            if symbol.endswith('.HK'):
                query_df = ak.stock_hk_hist(
                    symbol=symbol, 
                    period=akshare_period,
                    start_date=start_date.replace('-', ''), 
                    end_date=end_date.replace('-', ''), 
                    adjust="qfq"
                )
            else:
                query_df = ak.stock_zh_a_hist(
                    symbol=symbol, 
                    period=akshare_period,
                    start_date=start_date.replace('-', ''), 
                    end_date=end_date.replace('-', ''), 
                    adjust="qfq"
                )
                
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
            
            return query_df
            
        except Exception as e:
            self.logger.error(f"使用akshare获取{symbol}数据失败: {str(e)}")
            return False

    def _fetch_data_yfinance(self, symbol, start_date, end_date, interval):
        """
        使用yfinance获取A股或港股数据
        
        Args:
            symbol (str): 股票代码
            start_date (str): 开始日期
            end_date (str): 结束日期
            interval (str): 数据间隔
            
        Returns:
            pd.DataFrame: 查询结果，如果失败返回False
        """
        try:
            yf_symbol = self._convert_to_yfinance_symbol(symbol)
            ticker = yf.Ticker(yf_symbol)
            return ticker.history(start=start_date, end=end_date, interval=interval)
            
        except Exception as e:
            self.logger.error(f"使用yfinance获取{symbol}数据失败: {str(e)}")
            return False
            
    def _convert_to_yfinance_symbol(self, symbol):
        """
        将股票代码转换为yfinance格式
        
        Args:
            symbol (str): 原始股票代码
            
        Returns:
            str: yfinance格式的股票代码
        """
        if symbol.endswith('.HK'):
            return symbol[1:]  # 港股减掉第1个数字
        elif symbol.startswith('6'):
            return f"{symbol}.SS"
        else:
            return f"{symbol}.SZ"

    @staticmethod
    def _is_workday(date_str):
        date = pd.to_datetime(date_str)
        if date.weekday() >= 5:
            return False
        return True

    @staticmethod
    def _get_nearest_workday_backward(date_str):
        date = pd.to_datetime(date_str)
        while not DataFetcher._is_workday(date.strftime('%Y-%m-%d')):
            date -= timedelta(days=1)
        return date.strftime('%Y-%m-%d')

    @staticmethod
    def _get_nearest_workday_forward(date_str):
        date = pd.to_datetime(date_str)
        while not DataFetcher._is_workday(date.strftime('%Y-%m-%d')):
            date += timedelta(days=1)
        return date.strftime('%Y-%m-%d')