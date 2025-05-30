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
from typing import Optional, Dict, List, Union, Tuple, Any

class DataFetcher:
    # 配置常量
    SUPPORTED_INTERVALS = ['1d', '1wk', '1mo']
    INTERVAL_TO_PERIOD_MAP = {'1d': 'daily', '1wk': 'weekly', '1mo': 'monthly'}
    DEFAULT_CACHE_DIR = 'data'
    DEFAULT_QUERY_METHOD = 'yfinance'
    
    def __init__(self, cache_dir: str = DEFAULT_CACHE_DIR, use_db_cache: bool = True, 
                 use_csv_cache: bool = False, query_method: str = DEFAULT_QUERY_METHOD) -> None:
        """
        初始化数据获取器
        
        Args:
            cache_dir (str): 本地缓存目录
            use_db_cache (bool): 是否使用数据库缓存
            use_csv_cache (bool): 是否使用本地csv缓存
            query_method (str): 数据源方式
        """
        self.data_cache: Dict[str, pd.DataFrame] = {}
        self.symbol_info_db: Dict[str, str] = {}
        self.use_db_cache: bool = use_db_cache
        self.use_csv_cache: bool = use_csv_cache
        self.query_method: str = query_method
        
        # 创建缓存目录
        self.cache_dir: str = cache_dir
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
        
        # 初始化文件路径
        self._init_file_paths()
        
        # 初始化数据库和日志
        self.db: DataBase = DataBase(os.path.join(self.cache_dir, 'stock_hist_data.db'))
        self.logger: logging.Logger = logging.getLogger(__name__)
            
    def _init_file_paths(self) -> None:
        """初始化所有文件路径"""
        self.FILE_STOCK_INFO_SH: str = os.path.join(self.cache_dir, "stock_info_sh.csv")
        self.FILE_STOCK_INFO_SH_KCB: str = os.path.join(self.cache_dir, "stock_info_sh_kcb.csv")
        self.FILE_STOCK_INFO_SZ: str = os.path.join(self.cache_dir, "stock_info_sz.csv")
        self.FILE_STOCK_INFO_HK: str = os.path.join(self.cache_dir, "stock_info_hk.csv")
        self.FILE_STOCK_INFO_AH_CODE_NAME: str = os.path.join(self.cache_dir, "stock_info_ah_symbol_name.csv")
        self.FILE_STOCK_AH_SYMBOLS_ALL: str = os.path.join(self.cache_dir, "stock_ah_symbols_all.json")
        
    def _get_cache_filename(self, symbol: str, start_date: str, end_date: str, interval: str) -> str:
        """生成缓存文件名"""
        return os.path.join(self.cache_dir, f"{symbol}_{start_date}_{end_date}_{interval}.csv")
        
    def _load_from_cache(self, cache_file: str) -> Optional[pd.DataFrame]:
        """从缓存文件加载数据"""
        if os.path.exists(cache_file):
            try:
                df = pd.read_csv(cache_file, index_col=0, parse_dates=True)
                return df
            except Exception as e:
                self.logger.error(f"读取缓存文件失败: {str(e)}")
        return None
        
    def _save_to_cache(self, df: pd.DataFrame, cache_file: str) -> None:
        """保存数据到缓存文件"""
        try:
            df.to_csv(cache_file)
        except Exception as e:
            self.logger.error(f"保存缓存文件失败: {str(e)}")

    def _get_cache_key(self, symbol: str, start_date: str, end_date: str, interval: str) -> str:
        """生成缓存键"""
        return f"{symbol}_{start_date}_{end_date}_{interval}"

    def _get_from_memory_cache(self, cache_key: str) -> Optional[pd.DataFrame]:
        """从内存缓存获取数据"""
        return self.data_cache.get(cache_key)

    def _save_to_memory_cache(self, cache_key: str, df: pd.DataFrame) -> None:
        """保存数据到内存缓存"""
        self.data_cache[cache_key] = df

    def _get_from_db_cache(self, symbol: str, start_date: str, end_date: str, interval: str) -> Optional[pd.DataFrame]:
        """从数据库缓存获取数据"""
        if not self.use_db_cache:
            return None
        return self.db.fetch(symbol, start_date, end_date, interval)
    
    def _save_to_db_cache(self, symbol: str, df: pd.DataFrame, interval: str, update: bool = False) -> None:
        """保存数据到数据库缓存"""
        if not self.use_db_cache:
            return
        if update:
            self.db.update(symbol, df, interval)
        else:
            self.db.insert(symbol, df, interval)

    def _get_from_file_cache(self, symbol: str, start_date: str, end_date: str, interval: str) -> Optional[pd.DataFrame]:
        """从文件缓存获取数据"""
        if not self.use_csv_cache:
            return None
        cache_file = self._get_cache_filename(symbol, start_date, end_date, interval)
        return self._load_from_cache(cache_file)

    def _process_query_result(self, query_df: pd.DataFrame, symbol: str, start_date: str, 
                            end_date: str, interval: str) -> Optional[pd.DataFrame]:
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

    def _save_query_result(self, query_df: pd.DataFrame, symbol: str, start_date: str, 
                          end_date: str, interval: str) -> None:
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
            self._save_to_db_cache(symbol, query_df, interval)

    def _get_db_first_date(self, symbol: str, interval: str = '1d') -> Optional[datetime]:
        """
        获取第一条数据日期
        """
        db_first_date = self.db.get_first_date(symbol, interval)
        if db_first_date is None:
            return None
        else:
            return pd.to_datetime(db_first_date)

    def _get_db_last_date(self, symbol: str, interval: str = '1d') -> Optional[datetime]:
        """
        检查最后一条数据日期
        """
        db_last_date = self.db.get_last_date(symbol, interval)
        if db_last_date is None:
            return None
        else:
            return pd.to_datetime(db_last_date)
        
    def _prepare_params(self, symbol: str, start_date: str, end_date: Optional[str], 
                       interval: str) -> Tuple[str, str, str, str]:
        """
        入参检查和处理
        """
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

    def prepare_db_data(self, symbol: str, start_date: str, end_date: Optional[str] = None, 
                       interval: str = '1d') -> bool:
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
        db_df = self._get_from_db_cache(adj_symbol, adj_start_date, adj_end_date, interval)
        need_update_data = self._check_db_record_timestamp(db_df, adj_symbol)

        # 开始检查数据库数据情况
        db_start_date = self._get_db_first_date(adj_symbol, interval)
        db_end_date = self._get_db_last_date(adj_symbol, interval)

        # 检查数据库中是否包含start_date到end_date的数据
        if not need_update_data and db_start_date is not None and db_end_date is not None:
            self.logger.info(f"[CHECK]股票{adj_symbol}最新历史数据范围: {db_start_date.strftime('%Y-%m-%d')} -> {db_end_date.strftime('%Y-%m-%d')}")
            if pd.to_datetime(adj_start_date) >= db_start_date and db_end_date >= pd.to_datetime(adj_end_date):
                self.logger.info(f"[DONE]股票{adj_symbol}数据库中已包含 {adj_start_date} -> {adj_end_date} 的数据，数据就绪")
                return False
            adj_start_date, adj_end_date = self._adjust_date_range(adj_symbol, adj_start_date, adj_end_date, db_start_date, db_end_date)
    
        # 如果数据库中没有数据，则从网上获取数据
        self.logger.info(f"[TODO]: 数据库缺少股票{adj_symbol}@{adj_start_date} -> {adj_end_date}的数据")
        query_df = pd.DataFrame()
        query_df = self._query_stock_data_from_net(adj_symbol, adj_start_date, adj_end_date, interval)
        if isinstance(query_df, bool) and query_df is False:
            return False
        
        # 处理查询结果兼容性
        query_df = self._process_query_result(query_df, adj_symbol, adj_start_date, adj_end_date, interval)
        if query_df is None:
            return False

        # 保存到数据库
        self._save_to_db_cache(adj_symbol, query_df, interval, update=need_update_data)
        self.logger.info(f"[DONE]股票{adj_symbol}@{adj_start_date} -> {adj_end_date}的数据已保存到数据库")

        # 保存到内存缓存，下次匹配直接获取
        cache_key = self._get_cache_key(adj_symbol, adj_start_date, adj_end_date, interval)
        self._save_to_memory_cache(cache_key, query_df)

        return True

    def get_historical_data(self, symbol: str, start_date: str, end_date: Optional[str] = None, 
                           interval: str = '1d') -> Optional[pd.DataFrame]:
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
        cached_df = self._get_from_memory_cache(cache_key)
        if cached_df is not None and not cached_df.empty:
            return cached_df
        self.logger.info(f"内存中没有{adj_symbol}@{adj_start_date} - {adj_end_date}的数据")

        # 2. 检查数据库缓存
        db_df = self._get_from_db_cache(adj_symbol, adj_start_date, adj_end_date, interval)
        if db_df is not None and not db_df.empty:
            self._save_to_memory_cache(cache_key, db_df)
            return db_df
        self.logger.info(f"数据库中没有{adj_symbol}@{adj_start_date} - {adj_end_date}的数据") if self.use_db_cache else None
        
        # 3. 检查文件缓存
        csv_df = self._get_from_file_cache(adj_symbol, adj_start_date, adj_end_date, interval)
        if csv_df is not None and not csv_df.empty:
            self._save_to_memory_cache(cache_key, csv_df)
            return csv_df
        self.logger.info(f"本地缓存中没有{adj_symbol}@{adj_start_date} - {adj_end_date}的数据") if self.use_csv_cache else None
        
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

    def get_symbol_name(self, symbol: str) -> Optional[str]:
        """
        获取股票的基本信息
        
        Args:
            symbol (str): 股票代码
            
        Returns:
            str: 股票名称，如果获取失败则返回None
        """
        try:
            if not symbol.endswith('.HK'):
                symbol = symbol.split('.')[0]
            symbol_name = self.symbol_info_db[symbol]
            return symbol_name
        except Exception as e:
            self.logger.error(f"获取股票信息时发生错误: {str(e)}")
            return None 

    def init_stock_info(self) -> dict:
        """
        从数据库获取A股、科创板、深市、港股的股票信息，保存为csv和json文件。
        """
        self.logger.info("初始化股票信息")

        # 从数据库获取股票信息
        stock_info_db = pd.DataFrame(self.db.get_all_stock_info(), columns=['symbol', 'name'])
        if not stock_info_db.empty:
            self.symbol_info_db = dict(zip(stock_info_db['symbol'], stock_info_db['name']))
            self.logger.info(f"已从数据库加载stock_info_db = {len(self.symbol_info_db)}只股票信息")
        else:
            self.logger.error("数据库中没有股票信息")
        
        return self.symbol_info_db

    def _query_stock_data_from_net(self, adj_symbol: str, adj_start_date: str, adj_end_date: str, 
                                  interval: str) -> Optional[pd.DataFrame]:
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

    def _fetch_data_akshare(self, symbol: str, start_date: str, end_date: str, 
                           period: str) -> Union[pd.DataFrame, bool]:
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

    def _fetch_data_yfinance(self, symbol: str, start_date: str, end_date: str, 
                            interval: str) -> Union[pd.DataFrame, bool]:
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
            
    def _convert_to_yfinance_symbol(self, symbol: str) -> str:
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
    def _is_workday(date_str: str) -> bool:
        date = pd.to_datetime(date_str)
        if date.weekday() >= 5:
            return False
        return True

    @staticmethod
    def _get_nearest_workday_backward(date_str: str) -> str:
        date = pd.to_datetime(date_str)
        while not DataFetcher._is_workday(date.strftime('%Y-%m-%d')):
            date -= timedelta(days=1)
        return date.strftime('%Y-%m-%d')

    @staticmethod
    def _get_nearest_workday_forward(date_str: str) -> str:
        date = pd.to_datetime(date_str)
        while not DataFetcher._is_workday(date.strftime('%Y-%m-%d')):
            date += timedelta(days=1)
        return date.strftime('%Y-%m-%d')

    def _adjust_date_range(self, adj_symbol: str, adj_start_date: str, adj_end_date: str, 
                           db_start_date: datetime, db_end_date: datetime) -> Tuple[str, str]:
        """
        根据数据库中的日期范围调整请求的日期范围
        
        Args:
            adj_symbol: 股票代码
            adj_start_date: 请求的开始日期
            adj_end_date: 请求的结束日期
            db_start_date: 数据库中的最早日期
            db_end_date: 数据库中的最晚日期
            
        Returns:
            tuple[str, str]: 调整后的开始日期和结束日期
        """
        if db_start_date > pd.to_datetime(adj_end_date):
            adj_end_date = (db_start_date - timedelta(days=1)).strftime('%Y-%m-%d')
        elif db_end_date < pd.to_datetime(adj_start_date):
            adj_start_date = (db_end_date + timedelta(days=1)).strftime('%Y-%m-%d')
        elif pd.to_datetime(adj_start_date) < db_start_date and db_start_date < pd.to_datetime(adj_end_date):
            adj_end_date = (db_start_date - timedelta(days=1)).strftime('%Y-%m-%d')
        elif pd.to_datetime(adj_start_date) < db_end_date and db_end_date < pd.to_datetime(adj_end_date):
            adj_start_date = (db_end_date + timedelta(days=1)).strftime('%Y-%m-%d')
            
        return adj_start_date, adj_end_date

    def _check_db_record_timestamp(self, db_df: pd.DataFrame, adj_symbol: str) -> bool:
        """
        检查数据库中的记录时间戳，判断是否需要更新数据
        
        Args:
            db_df: 数据库查询结果DataFrame
            adj_symbol: 股票代码
            
        Returns:
            bool: 是否需要更新数据
        """
        need_update_data = False
        if not db_df.empty:
            for index, row in db_df.iterrows():
                if row['Timestamp'] and pd.to_datetime(row['Timestamp']) < pd.to_datetime(f"{index} 16:15:00"): # 入库时间戳较收盘时间早，标记需要特殊处理，考虑港股要取16:15:00
                    self.logger.info(f"[CHECK]股票{adj_symbol}数据库中存在{index}的盘中数据，入库时间戳为{row['Timestamp']}，较收盘时间早，标记需要特殊处理")
                    need_update_data = True
        return need_update_data