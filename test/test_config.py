import os
import tempfile
import yaml
import pytest
from src.config import RenkoConfig

def test_default_config_load_and_save():
    # 使用临时文件避免污染真实配置
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = os.path.join(tmpdir, 'test_config.yaml')
        config = RenkoConfig(config_path)
        # 检查默认值
        assert config.use_db_cache is True
        assert config.use_csv_cache is True
        assert config.query_method == 'akshare'
        assert config.max_iterations == 10000
        assert config.initial_capital == 1000000
        assert config.atr_periods == [3, 5, 10, 15]
        # 保存后文件应存在
        config.save_config()
        assert os.path.exists(config_path)
        # 检查保存内容
        with open(config_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        assert data['use_db_cache'] is True
        assert data['backtest_config']['max_iterations'] == 10000

def test_custom_config_load():
    # 构造自定义配置
    custom = {
        'use_db_cache': False,
        'use_csv_cache': False,
        'query_method': 'custom',
        'backtest_config': {
            'max_iterations': 5000,
            'max_threads': 2,
            'initial_capital': 500000,
            'recent_signal_days': 2,
            'target_return': 20
        },
        'optimization_parameters': {
            'atr_periods': [1, 2],
            'atr_multipliers': [0.1],
            'trend_lengths': [1]
        }
    }
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = os.path.join(tmpdir, 'custom.yaml')
        with open(config_path, 'w', encoding='utf-8') as f:
            yaml.dump(custom, f, allow_unicode=True)
        config = RenkoConfig(config_path)
        assert config.use_db_cache is False
        assert config.query_method == 'custom'
        assert config.max_iterations == 5000
        assert config.atr_periods == [1, 2]
        assert config.atr_multipliers == [0.1]
        assert config.trend_lengths == [1]

def test_recent_signal_days_limit():
    # recent_signal_days 超过5时应被限制为5
    custom = {
        'backtest_config': {
            'recent_signal_days': 10
        }
    }
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = os.path.join(tmpdir, 'limit.yaml')
        with open(config_path, 'w', encoding='utf-8') as f:
            yaml.dump(custom, f, allow_unicode=True)
        config = RenkoConfig(config_path)
        assert config.recent_signal_days == 5 