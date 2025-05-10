from typing import List, Dict
import yaml
import os

class RenkoConfig:
    def __init__(self, config_path: str = "config/config.yaml"):
        """
        初始化回测配置
        
        Args:
            config_path (str): 配置文件路径
        """
        self.config_path = config_path
        self.load_config()
    
    def load_config(self):
        """从YAML文件加载配置"""
        if not os.path.exists(self.config_path):
            self.use_default_config()
        else:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                self._set_config(config)
    
    def use_default_config(self):
        """使用默认配置"""
        default_config = {
            'backtest_config': {
                'max_iterations': 10000,
                'max_workers': 1,
                'initial_capital': 1000000
            },
            'optimization_parameters': {
                'atr_periods': [3, 5, 10, 15],
                'atr_multipliers': [0.3, 0.5, 1.0, 2.0],
                'trend_lengths': [2, 3, 5]
            }
        }
        self._set_config(default_config)
        
        # 确保配置目录存在
        os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
        
        # 保存默认配置到文件
        with open(self.config_path, 'w', encoding='utf-8') as f:
            yaml.dump(default_config, f, allow_unicode=True, default_flow_style=False)
    
    def _set_config(self, config: Dict):
        """设置配置参数"""
        backtest_config = config.get('backtest_config', {})
        optimization_parameters = config.get('optimization_parameters', {})
        
        # 优化参数
        self.max_iterations = backtest_config.get('max_iterations', 10000)
        self.max_workers = backtest_config.get('max_workers', 1)
        self.initial_capital = backtest_config.get('initial_capital', 1000000)
        self.recent_signal_days = min(5, int(backtest_config.get('recent_signal_days', 3)))
        self.target_return = float(backtest_config.get('target_return', 15))
        
        # 策略参数
        self.atr_periods = optimization_parameters.get('atr_periods', [3, 5, 10, 15])
        self.atr_multipliers = optimization_parameters.get('atr_multipliers', [0.3, 0.5, 1.0, 2.0])
        self.trend_lengths = optimization_parameters.get('trend_lengths', [2, 3, 5])
    
    def save_config(self):
        """保存当前配置到文件"""
        config = {
            'backtest_config': {
                'max_iterations': self.max_iterations,
                'max_workers': self.max_workers,
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
        
        with open(self.config_path, 'w', encoding='utf-8') as f:
            yaml.dump(config, f, allow_unicode=True, default_flow_style=False) 