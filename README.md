# Renko回测系统

这是一个基于砖型图(Renko)的量化交易回测系统。

## 项目结构

- `data/`: 数据存储目录
- `src/`: 源代码目录
  - `data_fetcher.py`: 数据获取模块
  - `renko_generator.py`: 砖型图生成模块
  - `strategy.py`: 交易策略模块
  - `backtest.py`: 回测引擎模块
  - `analysis.py`: 结果分析模块

## 安装依赖

```bash
pip install -r requirements.txt
```

## 使用方法

1. 配置数据源和参数
2. 运行回测
3. 分析结果

## 注意事项

- 需要安装TA-Lib库
- 建议使用Python 3.8+ 