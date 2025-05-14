import os
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from src.data_fetcher import DataFetcher

@pytest.fixture
def mock_db(tmp_path):
    # mock DataBase
    with patch('src.data_fetcher.DataBase') as MockDB:
        mock_db_instance = MockDB.return_value
        # mock fetch, insert, update, get_first_date, get_last_date, get_all_stock_info
        mock_db_instance.fetch.return_value = pd.DataFrame()
        mock_db_instance.insert.return_value = None
        mock_db_instance.update.return_value = None
        mock_db_instance.get_first_date.return_value = None
        mock_db_instance.get_last_date.return_value = None
        mock_db_instance.get_all_stock_info.return_value = [('000001', '平安银行')]
        yield mock_db_instance

@pytest.fixture
def fetcher(tmp_path, mock_db):
    # 使用临时目录，避免污染真实数据
    return DataFetcher(cache_dir=str(tmp_path), use_db_cache=True, use_csv_cache=False, query_method='yfinance')

def test_init(fetcher):
    assert isinstance(fetcher, DataFetcher)
    assert os.path.exists(fetcher.cache_dir)

def test_get_symbol_name(fetcher):
    fetcher.symbol_info_db = {'000001': '平安银行'}
    assert fetcher.get_symbol_name('000001') == '平安银行'
    assert fetcher.get_symbol_name('000002') is None

def test_get_historical_data_memory_cache(fetcher):
    # 直接测试内存缓存
    key = fetcher._get_cache_key('000001', '2024-01-01', '2024-01-10', '1d')
    df = pd.DataFrame({'Open': [1], 'High': [2], 'Low': [0.5], 'Close': [1.5], 'Volume': [100], 'Turnover': [150], 'Timestamp': ['2024-01-01 16:00:00']}, index=pd.to_datetime(['2024-01-01']))
    fetcher._save_to_memory_cache(key, df)
    result = fetcher.get_historical_data('000001', '2024-01-01', '2024-01-10', '1d')
    assert result is not None
    assert isinstance(result, pd.DataFrame)
    assert result.iloc[0]['Open'] == 1

@patch('src.data_fetcher.ak')
def test_init_stock_info(mock_ak, fetcher, tmp_path):
    # mock akshare 返回
    mock_ak.stock_info_sh_name_code.return_value = pd.DataFrame({'证券代码': ['000001'], '证券简称': ['平安银行']})
    mock_ak.stock_info_sh_name_code.side_effect = None
    mock_ak.stock_info_sz_name_code.return_value = pd.DataFrame({'A股代码': ['000002'], 'A股简称': ['万科A']})
    mock_ak.stock_info_hk_spot_em.return_value = pd.DataFrame({'代码': ['00003'], '名称': ['中银香港']})
    mock_ak.stock_info_sh_name_code.side_effect = None
    mock_ak.stock_info_sh_name_code.return_value = pd.DataFrame({'证券代码': ['000001'], '证券简称': ['平安银行']})
    fetcher.init_stock_info()
    # 检查文件是否生成
    assert os.path.exists(os.path.join(fetcher.cache_dir, 'stock_info_ah_symbol_name.csv'))
    assert os.path.exists(os.path.join(fetcher.cache_dir, 'stock_ah_symbols_all.json')) 