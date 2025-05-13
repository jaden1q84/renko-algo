import pytest
import pandas as pd
from src.database import DataBase
import datetime

@pytest.fixture
def db():
    # 使用内存数据库，测试不会产生文件
    return DataBase(":memory:")

def make_test_df():
    # 构造测试用的DataFrame
    data = {
        'Open': [10.0, 11.0],
        'High': [10.5, 11.5],
        'Low': [9.5, 10.5],
        'Close': [10.2, 11.2],
        'Volume': [1000, 1100],
        'Turnover': [10000, 12000],
        'Timestamp': ["2024-01-01 09:30:00", "2024-01-02 09:30:00"]
    }
    idx = pd.to_datetime(["2024-01-01", "2024-01-02"])
    df = pd.DataFrame(data, index=idx)
    return df

def test_insert_and_fetch(db):
    symbol = "000001.SZ"
    interval = "1d"
    df = make_test_df()
    db.insert(symbol, df, interval)
    result = db.fetch(symbol, "2024-01-01", "2024-01-02", interval)
    assert result is not None
    assert len(result) == 2
    assert result.iloc[0]["Open"] == 10.0
    assert result.iloc[1]["Close"] == 11.2

def test_update_and_get_last_first_date(db):
    symbol = "000002.SZ"
    interval = "1d"
    df = make_test_df()
    db.update(symbol, df, interval)
    last = db.get_last_date(symbol, interval)
    first = db.get_first_date(symbol, interval)
    assert last == "2024-01-02"
    assert first == "2024-01-01"

def test_stock_info(db):
    # 测试股票信息表的插入和查询
    info_df = pd.DataFrame({"symbol": ["000001.SZ"], "name": ["平安银行"]})
    db.update_stock_info(info_df)
    all_info = db.get_all_stock_info()
    assert ("000001.SZ", "平安银行") in all_info
    symbol = db.get_stock_info("000001.SZ")
    assert symbol == "000001.SZ" 