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
            self.conn.execute(sql)
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
            # 打印前3条记录
            self.logger.info(f"DB fetch结果前3条: {df.head(3).to_dict(orient='records')}")
            return df

    def insert(self, symbol, df, interval):
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
            # 打印前3条记录
            self.logger.info(f"DB insert数据前3条: {df.head(3).to_dict(orient='records')}")
            self.conn.executemany(sql, data)
            self.conn.commit() 