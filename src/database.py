import sqlite3
import pandas as pd
import threading
import logging
from typing import Optional, List, Tuple, Dict, Any
from dataclasses import dataclass
from datetime import datetime

@dataclass
class TableSchema:
    """数据库表结构定义"""
    HIST_DATA_TABLE_PREFIX = "stock_hist_data_"
    STOCK_INFO_TABLE = "stock_info_ah_symbol_name"
    
    HIST_DATA_COLUMNS = [
        "symbol", "date", "open", "high", "low", "close",
        "volume", "turnover", "interval", "timestamp"
    ]
    
    STOCK_INFO_COLUMNS = ["symbol", "name"]

    @classmethod
    def get_hist_data_table(cls, interval: str) -> str:
        """获取历史数据表名"""
        return f"{cls.HIST_DATA_TABLE_PREFIX}{interval}"

class DataConverter:
    """数据转换工具类"""
    @staticmethod
    def df_to_db_records(symbol: str, df: pd.DataFrame, interval: str) -> List[Tuple]:
        """将DataFrame转换为数据库记录格式"""
        return [
            (symbol, idx.strftime('%Y-%m-%d'), row.Open, row.High, row.Low,
             row.Close, row.Volume, row.Turnover, interval, row.Timestamp)
            for idx, row in df.iterrows()
        ]
    
    @staticmethod
    def db_rows_to_df(rows: List[Tuple]) -> pd.DataFrame:
        """将数据库查询结果转换为DataFrame"""
        df = pd.DataFrame(rows, columns=["Date", "Open", "High", "Low", "Close", "Volume", "Turnover", "Timestamp"])
        df["Date"] = pd.to_datetime(df["Date"])
        df.set_index("Date", inplace=True)
        return df

class DataBase:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.lock = threading.Lock()
        self.logger = logging.getLogger(__name__)
        self._create_tables()

    def _create_tables(self) -> None:
        """创建必要的数据库表"""
        with self.lock:
            self._create_hist_data_table()
            self._create_stock_info_table()
            self._ensure_timestamp_column()

    def _create_hist_data_table(self) -> None:
        """创建历史数据表"""
        table_name = TableSchema.get_hist_data_table("1d")  # 默认创建1d表
        sql = f'''CREATE TABLE IF NOT EXISTS {table_name} (
            symbol TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            turnover REAL,
            interval TEXT,
            timestamp TEXT,
            PRIMARY KEY(symbol, date)
        )'''
        self.conn.execute(sql)
        self.conn.commit()

    def _create_stock_info_table(self) -> None:
        """创建股票信息表"""
        sql = f'''CREATE TABLE IF NOT EXISTS {TableSchema.STOCK_INFO_TABLE} (
            symbol TEXT NOT NULL,
            name TEXT NOT NULL,
            PRIMARY KEY(symbol)
        )'''
        self.conn.execute(sql)
        self.conn.commit()

    def _ensure_timestamp_column(self) -> None:
        """确保timestamp列存在"""
        cur = self.conn.execute("PRAGMA table_info(stock_hist_data_1d)")
        columns = [row[1] for row in cur.fetchall()]
        if "timestamp" not in columns:
            self.conn.execute("ALTER TABLE stock_hist_data_1d ADD COLUMN timestamp TEXT")
            self.conn.commit()

    def _log_data_operation(self, operation: str, df: pd.DataFrame) -> None:
        """记录数据操作日志"""
        if not df.empty:
            self.logger.debug(f"DB {operation}数据第1条: Date: {df.index[0]} {df.iloc[0].to_dict()}")
            self.logger.debug(f"DB {operation}数据最后1条: Date: {df.index[-1]} {df.iloc[-1].to_dict()}")

    def fetch(self, symbol: str, start_date: str, end_date: str, interval: str) -> Optional[pd.DataFrame]:
        """获取历史数据"""
        with self.lock:
            table_name = TableSchema.get_hist_data_table(interval)
            sql = f'''SELECT date, open, high, low, close, volume, turnover, timestamp 
                     FROM {table_name}
                     WHERE symbol=? AND date>=? AND date<=? 
                     ORDER BY date ASC'''
            
            cur = self.conn.execute(sql, (symbol, start_date, end_date))
            rows = cur.fetchall()
            
            if not rows:
                self.logger.warning(f"未查询到数据: symbol={symbol}, start_date={start_date}, end_date={end_date}, interval={interval}")
                return None
                
            df = DataConverter.db_rows_to_df(rows)
            self._log_data_operation("fetch", df)
            return df

    def insert(self, symbol: str, df: pd.DataFrame, interval: str) -> None:
        """插入历史数据"""
        if df.empty:
            self.logger.warning(f"插入数据为空: symbol={symbol}, interval={interval}")
            return
        
        with self.lock:
            table_name = TableSchema.get_hist_data_table(interval)
            sql = f'''INSERT OR IGNORE INTO {table_name}
                     (symbol, date, open, high, low, close, volume, turnover, interval, timestamp)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
            
            data = DataConverter.df_to_db_records(symbol, df, interval)
            self._log_data_operation("insert", df)
            self.conn.executemany(sql, data)
            self.conn.commit()

    def update(self, symbol: str, df: pd.DataFrame, interval: str) -> None:
        """更新历史数据"""
        with self.lock:
            table_name = TableSchema.get_hist_data_table(interval)
            sql = f'''INSERT OR REPLACE INTO {table_name}
                     (symbol, date, open, high, low, close, volume, turnover, interval, timestamp)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
            
            data = DataConverter.df_to_db_records(symbol, df, interval)
            self._log_data_operation("update", df)
            self.conn.executemany(sql, data)
            self.conn.commit()

    def get_last_date(self, symbol: str, interval: str) -> Optional[str]:
        """获取最后一条数据的日期"""
        with self.lock:
            table_name = TableSchema.get_hist_data_table(interval)
            sql = f'''SELECT date FROM {table_name}
                     WHERE symbol=? ORDER BY date DESC LIMIT 1'''
            
            cur = self.conn.execute(sql, (symbol,))
            result = cur.fetchone()
            
            if result is None:
                self.logger.warning(f"未找到股票数据: symbol={symbol}, interval={interval}")
                return None
                
            return result[0]

    def get_first_date(self, symbol: str, interval: str) -> Optional[str]:
        """获取第一条数据的日期"""
        with self.lock:
            table_name = TableSchema.get_hist_data_table(interval)
            sql = f'''SELECT date FROM {table_name}
                     WHERE symbol=? ORDER BY date ASC LIMIT 1'''
            
            cur = self.conn.execute(sql, (symbol,))
            result = cur.fetchone()
            
            if result is None:
                self.logger.warning(f"未找到股票数据: symbol={symbol}, interval={interval}")
                return None
                
            return result[0]

    def get_stock_info(self, symbol: str) -> Optional[str]:
        """获取股票信息"""
        with self.lock:
            sql = f'''SELECT symbol, name FROM {TableSchema.STOCK_INFO_TABLE} WHERE symbol=?'''
            cur = self.conn.execute(sql, (symbol,))
            result = cur.fetchone()
            
            if result is None:
                self.logger.warning(f"未找到股票数据: symbol={symbol}")
                return None
                
            return result[0]

    def get_all_stock_info(self) -> List[Tuple[str, str]]:
        """获取所有股票信息"""
        with self.lock:
            sql = f'''SELECT symbol, name FROM {TableSchema.STOCK_INFO_TABLE}'''
            cur = self.conn.execute(sql)
            return cur.fetchall()

    def update_stock_info(self, df: pd.DataFrame) -> None:
        """更新股票信息"""
        with self.lock:
            sql = f'''INSERT OR REPLACE INTO {TableSchema.STOCK_INFO_TABLE} 
                     (symbol, name) VALUES (?, ?)'''
            self.conn.executemany(sql, df.values)
            self.conn.commit()
