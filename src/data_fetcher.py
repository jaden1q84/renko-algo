import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os
import akshare as ak
import json
from database import DataBase
import logging

class DataFetcher:
    def __init__(self, cache_dir='data', use_db_cache=True, use_csv_cache=True, query_method='yfinance'):
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
        self.FILE_STOCK_INFO_AH_CODE_NAME = os.path.join(self.cache_dir, "stock_info_ah_symbol_name.csv")
        self.FILE_STOCK_AH_SYMBOLS_ALL = os.path.join(self.cache_dir, "stock_ah_symbols_all.json")
        
        self.db = DataBase(os.path.join(self.cache_dir, 'stock_hist_data.db'))
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
        
    def _prepare_params(self, symbol, start_date, end_date, interval, query_method):
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
        
        if query_method is None or (query_method != "yfinance" and query_method != "akshare"):
            raise ValueError(f"query_method不能为空，并且只支持yfinance和akshare")

        return adj_symbol, adj_start_date, adj_end_date, interval, query_method

    def prepare_db_data(self, symbol, start_date, end_date=None, interval='1d', query_method="yfinance"):
        """
        根据参数准备数据库的数据，对比数据库和网络最新数据，并更新到数据库
        返回True表示数据库数据就绪，False标识失败。
        """
        # 入参检查和处理
        adj_symbol, adj_start_date, adj_end_date, interval, query_method = self._prepare_params(symbol, start_date, end_date, interval, query_method)
        
        if not self.use_db_cache:
            # 如果不用数据库，则直接返回
            self.logger.info(f"不使用数据库缓存，直接返回False")
            return False
        need_update_data = False

        # interval到period的映射，用于兼容akshare接口
        interval_map = {'1d': 'daily', '1wk': 'weekly', '1mo': 'monthly'}
        if interval not in interval_map:
            raise ValueError(f"A/H股仅支持interval为'1d', '1wk', '1mo'，收到: {interval}")
        akshare_period = interval_map[interval]
        
        # 数据库中有数据不是最终收盘数据，检查记录的入库时间戳，标注要执行update动作。
        db_df = pd.DataFrame()
        db_df = self.db.fetch(adj_symbol, adj_start_date, adj_end_date, interval)
        if not db_df.empty:
            for index, row in db_df.iterrows():
                if row['Timestamp'] and pd.to_datetime(row['Timestamp']) < pd.to_datetime(f"{index} 16:15:00"): # 入库时间戳较收盘时间早，标记需要特殊处理，考虑港股要取16:15:00
                    self.logger.info(f"股票{adj_symbol}数据库中存在{index}的盘中数据，入库时间戳为{row['Timestamp']}，较收盘时间早，标记需要特殊处理")
                    need_update_data = True

        # 开始检查数据库数据情况
        db_start_date = self._get_db_first_date(adj_symbol, interval)
        db_end_date = self._get_db_last_date(adj_symbol, interval)

        # 检查数据库中是否包含start_date到end_date的数据
        if not need_update_data and db_start_date is not None and db_end_date is not None:
            self.logger.info(f"股票{adj_symbol}最新历史数据范围: {db_start_date.strftime('%Y-%m-%d')} -> {db_end_date.strftime('%Y-%m-%d')}")
            if db_start_date > pd.to_datetime(adj_end_date):
                adj_end_date = (db_start_date - timedelta(days=1)).strftime('%Y-%m-%d') # ok，准备的数据比数据库中数据段更早，则调整adj_end_date为数据库中数据段的前1天
            elif db_end_date < pd.to_datetime(adj_start_date):
                adj_start_date = (db_end_date + timedelta(days=1)).strftime('%Y-%m-%d') # ok，准备的数据比数据库中数据段更晚，则调整adj_start_date为数据库中数据段的后1天
            elif pd.to_datetime(adj_start_date) < db_start_date and db_start_date < pd.to_datetime(adj_end_date):
                adj_end_date = (db_start_date - timedelta(days=1)).strftime('%Y-%m-%d') # ok，准备的数据比数据库中数据段更早，但有部分重叠，则调整adj_end_date为数据库中数据段的前1天
            elif pd.to_datetime(adj_start_date) < db_end_date and db_end_date < pd.to_datetime(adj_end_date):
                adj_start_date = (db_end_date + timedelta(days=1)).strftime('%Y-%m-%d') # ok，准备的数据比数据库中数据段更晚，但有部分重叠，则调整adj_start_date为数据库中数据段的后1天
            elif pd.to_datetime(adj_start_date) >= db_start_date and db_end_date >= pd.to_datetime(adj_end_date):
                self.logger.info(f"股票{adj_symbol}数据库中已包含 {adj_start_date} -> {adj_end_date} 的数据，数据就绪")
                return True
    
        # 如果数据库中没有数据，则从网上获取数据
        self.logger.info(f"[TODO]: 数据库缺少股票{adj_symbol}@{adj_start_date} -> {adj_end_date}的数据，使用{query_method}获取")
        query_df = pd.DataFrame()
        query_df = self._query_stock_data_from_net(adj_symbol, adj_start_date, adj_end_date, interval, query_method)

        # 保存到数据库
        if need_update_data:
            self.db.update(adj_symbol, query_df, interval)
        else:
            self.db.insert(adj_symbol, query_df, interval)

        # 保存到内存缓存，下次匹配直接获取
        cache_key = f"{adj_symbol}_{adj_start_date}_{adj_end_date}_{interval}"
        self.data_cache[cache_key] = query_df

        return True

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
        # 入参检查和处理
        adj_symbol, adj_start_date, adj_end_date, interval, query_method = self._prepare_params(symbol, start_date, end_date, interval, query_method)

        # interval到period的映射，用于兼容akshare接口
        interval_map = {'1d': 'daily', '1wk': 'weekly', '1mo': 'monthly'}
        if interval not in interval_map:
            raise ValueError(f"A/H股仅支持interval为'1d', '1wk', '1mo'，收到: {interval}")
        akshare_period = interval_map[interval]

        if query_method is None:
            raise ValueError(f"query_method不能为空")

        # 先检查内存缓存
        cache_key = f"{adj_symbol}_{adj_start_date}_{adj_end_date}_{interval}"
        if cache_key in self.data_cache:
            return self.data_cache[cache_key]

        # 检查数据库，从数据库返回
        if self.use_db_cache:
            self.logger.info(f"查询数据库中{adj_symbol}@{adj_start_date} - {adj_end_date}的数据")
            db_df = pd.DataFrame()
            db_df = self.db.fetch(adj_symbol, adj_start_date, adj_end_date, interval)
            if not db_df.empty:
                self.data_cache[cache_key] = db_df
                return db_df
            else:
                self.logger.error(f"数据库中没有{adj_symbol}@{adj_start_date} - {adj_end_date}的数据")
                return None
        
        # 检查本地缓存，从本地缓存返回
        csv_file = self._get_cache_filename(adj_symbol, adj_start_date, adj_end_date, interval)
        if self.use_csv_cache:
            csv_df = self._load_from_cache(csv_file)
            if csv_df is not None:
                self.data_cache[cache_key] = csv_df
                return csv_df
            else:
                self.logger.error(f"本地缓存中没有{adj_symbol}@{adj_start_date} - {adj_end_date}的数据")

        # 如果前面都没有获取到数据，则从网上获取数据
        self.logger.info(f"[TODO]: 数据库和本地缓存中没有股票{adj_symbol}@{adj_start_date} -> {adj_end_date}的数据，使用{query_method}获取")
        query_df = pd.DataFrame()
        query_df = self._query_stock_data_from_net(adj_symbol, adj_start_date, adj_end_date, interval, query_method)

        # 保存到本地缓存和内存
        if not query_df.empty:
            self._save_to_cache(query_df, csv_file)
            self.data_cache[cache_key] = query_df
            
        return query_df

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

    def _fetch_data_akshare(self, symbol, start_date, end_date, period):
        """
        使用akshare获取A股或港股数据
        """
        try:
            import akshare as ak
            if symbol.endswith('.HK'):
                query_df = ak.stock_hk_hist(symbol=symbol, period=period, 
                                    start_date=start_date.replace('-', ''), end_date=end_date.replace('-', ''), adjust="qfq")
            else:
                query_df = ak.stock_zh_a_hist(symbol=symbol, period=period, 
                                        start_date=start_date.replace('-', ''), end_date=end_date.replace('-', ''), adjust="qfq")
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
        """
        try:
            import yfinance as yf
            yf_symbol = symbol
            if not symbol.endswith('.HK'):
                if symbol.startswith('6'):
                    yf_symbol = f"{symbol}.SS"
                else:
                    yf_symbol = f"{symbol}.SZ"
            else:
                # 港股减掉第1个数字
                yf_symbol = symbol[1:]
            ticker = yf.Ticker(yf_symbol)
            query_df = ticker.history(start=start_date, end=end_date, interval=interval)
            return query_df
        except Exception as e:
            self.logger.error(f"使用yfinance获取{symbol}数据失败: {str(e)}")
            return False
        
    def _query_stock_data_from_net(self, adj_symbol, adj_start_date, adj_end_date, interval, query_method):
        """
        查询股票数据
        """
        # 从网上获取数据
        query_df = pd.DataFrame()
        if query_method == 'akshare':
            # 查询前adj_end_date加1天，用于兼容akshare接口
            adj_end_date = (pd.to_datetime(adj_end_date) + timedelta(days=1)).strftime('%Y-%m-%d')
            query_df = self._fetch_data_akshare(adj_symbol, adj_start_date, adj_end_date, akshare_period)
            if isinstance(query_df, bool) and query_df is False:
                return None
        elif query_method == 'yfinance':
            query_df = self._fetch_data_yfinance(adj_symbol, adj_start_date, adj_end_date, interval)
            if isinstance(query_df, bool) and query_df is False:
                return None
        else:
            raise ValueError(f"不支持的query_method: {query_method}")

        # 数据兼容性处理，保证入库数据格式一致性        
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

            # query_df增加一列入库时间戳，精确到秒
            query_df['Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            self.logger.info(f"获取了 {adj_symbol}@{query_df['Date'].min()} -> {query_df['Date'].max()} 的 {len(query_df)} 条数据")
        else:
            self.logger.warning(f"警告: {adj_symbol} 没有获取到数据")

        return query_df

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