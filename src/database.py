import sqlite3
import pandas as pd
import threading
import logging

class DataBase:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.lock = threading.Lock()
        self._create_table()
        # 配置日志
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')
        self.logger = logging.getLogger(__name__)

    def _create_table(self):
        with self.lock:
            sql = '''CREATE TABLE IF NOT EXISTS stock_hist_data_1d (
                symbol TEXT NOT NULL,
                date TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                turnover REAL,
                interval TEXT,
                PRIMARY KEY(symbol, date)
            )'''
            sql_stock_info = '''CREATE TABLE IF NOT EXISTS stock_info_ah_symbol_name (
                symbol TEXT NOT NULL,
                name TEXT NOT NULL,
                PRIMARY KEY(symbol)
            )'''
            self.conn.execute(sql)
            self.conn.execute(sql_stock_info)
            self.conn.commit()

    def fetch(self, symbol, start_date, end_date, interval):
        with self.lock:
            table_name = f"stock_hist_data_{interval}"
            sql = f'''SELECT date, open, high, low, close, volume, turnover FROM {table_name}
                     WHERE symbol=? AND date>=? AND date<=? ORDER BY date ASC'''
            cur = self.conn.execute(sql, (symbol, start_date, end_date))
            rows = cur.fetchall()
            if not rows:
                self.logger.info(f"未查询到数据: symbol={symbol}, start_date={start_date}, end_date={end_date}, interval={interval}")
                return None
            df = pd.DataFrame(rows, columns=["Date", "Open", "High", "Low", "Close", "Volume", "Turnover"])
            df["Date"] = pd.to_datetime(df["Date"])
            df.set_index("Date", inplace=True)
            # 打印第1条记录和最后1条记录
            self.logger.info(f"DB fetch结果第1条: {f"Date: {df.index[0]}；{df.iloc[0].to_dict()}"}")
            self.logger.info(f"DB fetch结果最后1条: {f"Date: {df.index[-1]}；{df.iloc[-1].to_dict()}"}")
            return df

    def insert(self, symbol, df, interval):
        if df.empty:
            self.logger.error(f"插入数据为空: symbol={symbol}, interval={interval}")
            return
        
        with self.lock:
            # 避免重复插入，采用INSERT OR IGNORE
            table_name = f"stock_hist_data_{interval}"
            sql = f'''INSERT OR IGNORE INTO {table_name}
                     (symbol, date, open, high, low, close, volume, turnover, interval)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'''
            data = [
                (symbol, idx.strftime('%Y-%m-%d'), row.Open, row.High, row.Low, row.Close, row.Volume, row.Turnover, interval)
                for idx, row in df.iterrows()
            ]
            # 打印插入第1条记录和最后1条记录
            self.logger.info(f"DB insert数据第1条: {f"Date: {df.index[0]}；{df.iloc[0].to_dict()}"}")
            self.logger.info(f"DB insert数据最后1条: {f"Date: {df.index[-1]}；{df.iloc[-1].to_dict()}"}")
            self.conn.executemany(sql, data)
            self.conn.commit() 

    def get_last_date(self, symbol, interval):
        with self.lock:
            table_name = f"stock_hist_data_{interval}"
            sql = f'''SELECT date FROM {table_name}
                     WHERE symbol=? ORDER BY date DESC LIMIT 1'''
            cur = self.conn.execute(sql, (symbol,))
            result = cur.fetchone()
            if result is None:
                self.logger.info(f"未找到股票数据: symbol={symbol}, interval={interval}")
                return None
            return result[0]

    def get_first_date(self, symbol, interval):
        with self.lock:
            table_name = f"stock_hist_data_{interval}"
            sql = f'''SELECT date FROM {table_name}
                     WHERE symbol=? ORDER BY date ASC LIMIT 1'''
            cur = self.conn.execute(sql, (symbol,))
            result = cur.fetchone()
            if result is None:
                self.logger.info(f"未找到股票数据: symbol={symbol}, interval={interval}")
                return None
            return result[0]

    def get_stock_info(self, symbol):
        with self.lock:
            sql = '''SELECT symbol, name FROM stock_info_ah_symbol_name WHERE symbol=?'''
            cur = self.conn.execute(sql, (symbol,))
            result = cur.fetchone()
            if result is None:
                self.logger.info(f"未找到股票数据: symbol={symbol}")
                return None
            return result[0]
        
    def get_all_stock_info(self):
        with self.lock:
            sql = '''SELECT symbol, name FROM stock_info_ah_symbol_name'''
            cur = self.conn.execute(sql)
            return cur.fetchall()
        
    def insert_stock_info(self, df):
        with self.lock:
            sql = '''INSERT OR IGNORE INTO stock_info_ah_symbol_name (symbol, name) VALUES (?, ?)'''
            self.conn.executemany(sql, df.values)
            self.conn.commit()
