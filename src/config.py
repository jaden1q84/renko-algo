from typing import List, Dict, Any
import yaml
import os

class RenkoConfig:
    DEFAULT_CONFIG: Dict[str, Any] = {
        'use_db_cache': True,
        'use_csv_cache': True,
        'query_method': 'akshare',
        'backtest_config': {
            'max_iterations': 10000,
            'max_threads': 1,
            'initial_capital': 1000000,
            'recent_signal_days': 3,
            'target_return': 15
        },
        'optimization_parameters': {
            'atr_periods': [3, 5, 10, 15],
            'atr_multipliers': [0.3, 0.5, 1.0, 2.0],
            'trend_lengths': [2, 3, 5]
        }
    }

    def __init__(self, config_path: str = "config/config.yaml") -> None:
        """
        初始化回测配置
        Args:
            config_path (str): 配置文件路径
        """
        self.config_path = config_path
        self._load_config()

    def _load_config(self) -> None:
        """从YAML文件加载配置，如不存在则使用默认配置"""
        if not os.path.exists(self.config_path):
            self._apply_config(self.DEFAULT_CONFIG)
            self._ensure_config_dir()
            self.save_config()
        else:
            try:
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f) or {}
                self._apply_config(config)
            except Exception as e:
                print(f"加载配置文件失败，使用默认配置: {e}")
                self._apply_config(self.DEFAULT_CONFIG)

    def _ensure_config_dir(self) -> None:
        """确保配置文件目录存在"""
        config_dir = os.path.dirname(self.config_path)
        if config_dir:
            os.makedirs(config_dir, exist_ok=True)

    def _apply_config(self, config: Dict[str, Any]) -> None:
        """应用配置参数到实例属性"""
        self.use_db_cache: bool = config.get('use_db_cache', self.DEFAULT_CONFIG['use_db_cache'])
        self.use_csv_cache: bool = config.get('use_csv_cache', self.DEFAULT_CONFIG['use_csv_cache'])
        self.query_method: str = config.get('query_method', self.DEFAULT_CONFIG['query_method'])

        backtest = config.get('backtest_config', {})
        default_backtest = self.DEFAULT_CONFIG['backtest_config']
        self.max_iterations: int = backtest.get('max_iterations', default_backtest['max_iterations'])
        self.max_threads: int = backtest.get('max_threads', default_backtest['max_threads'])
        self.initial_capital: int = backtest.get('initial_capital', default_backtest['initial_capital'])
        self.recent_signal_days: int = min(5, int(backtest.get('recent_signal_days', default_backtest['recent_signal_days'])))
        self.target_return: float = float(backtest.get('target_return', default_backtest['target_return']))

        opt = config.get('optimization_parameters', {})
        default_opt = self.DEFAULT_CONFIG['optimization_parameters']
        self.atr_periods: List[int] = opt.get('atr_periods', default_opt['atr_periods'])
        self.atr_multipliers: List[float] = opt.get('atr_multipliers', default_opt['atr_multipliers'])
        self.trend_lengths: List[int] = opt.get('trend_lengths', default_opt['trend_lengths'])

    def save_config(self) -> None:
        """保存当前配置到文件"""
        config = {
            'use_db_cache': self.use_db_cache,
            'use_csv_cache': self.use_csv_cache,
            'query_method': self.query_method,
            'backtest_config': {
                'max_iterations': self.max_iterations,
                'max_threads': self.max_threads,
                'initial_capital': self.initial_capital,
                'recent_signal_days': self.recent_signal_days,
                'target_return': self.target_return
            },
            'optimization_parameters': {
                'atr_periods': self.atr_periods,
                'atr_multipliers': self.atr_multipliers,
                'trend_lengths': self.trend_lengths
            }
        }
        self._ensure_config_dir()
        with open(self.config_path, 'w', encoding='utf-8') as f:
            yaml.dump(config, f, allow_unicode=True, default_flow_style=False) 