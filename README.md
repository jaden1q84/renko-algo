# Renko策略回测系统

这是一个基于Renko图表的量化交易策略回测系统。系统支持两种Renko生成模式：基于ATR和基于日线，并提供参数优化功能。

## 项目结构

```
src/
├── main.py                # 主程序入口
├── renko_backtester.py    # Renko回测核心类
├── data_fetcher.py        # 数据获取模块
├── renko_generator.py     # Renko图表生成器
├── strategy.py            # 交易策略实现
├── backtest_optimizer.py  # 参数优化器
└── renko_plotter.py       # Renko图表可视化
```

## 功能特点

- 支持两种Renko生成模式：
  - ATR模式：基于ATR指标动态调整砖块大小
  - 日线模式：基于日线数据生成固定大小的砖块
- 参数优化功能：自动寻找最优策略参数
- 可视化回测结果：包括Renko图表和投资组合价值曲线
- 支持批处理模式：可用于批量回测多个标的

## 使用方法

1. 推荐使用conda安装依赖：
```bash
conda env create -f environment.yml
conda activate renko-algo
```

2. 运行回测：
```bash
python src/main.py --symbol 688041 --start_date 2023-01-01 --end_date 2023-12-31
```

3. 参数说明：
- `--symbol`: 股票代码（必需，与--symbol_list二选一），例如：688041
- `--symbol_list`: 股票代码列表配置文件（JSON数组），如config/symbol_list.json（必需，与--symbol二选一）
- `--start_date`: 开始日期（可选，默认取过去180天），格式：YYYY-MM-DD
- `--end_date`: 结束日期（可选，默认取今天日期），格式：YYYY-MM-DD
- `--renko_mode`: Renko生成模式（可选，默认atr），可选值：atr（基于ATR）或daily（基于日线）
- `--atr_period`: ATR周期（可选，默认10，仅当renko_mode=atr时有效）
- `--atr_multiplier`: ATR乘数（可选，默认0.5，仅当renko_mode=atr时有效）
- `--brick_size`: 砖块颗粒度（可选）
- `--buy_trend_length`: 买入信号所需的趋势长度（可选，默认3）
- `--sell_trend_length`: 卖出信号所需的趋势长度（可选，默认3）
- `--optimize`: 是否进行参数优化（可选）
- `--max_iterations`: 最大优化迭代次数（可选）
- `--threads`: 每个进程的多线程数量（可选）
- `--workers`: 多进程数量（可选，默认1）
- `--save_data`: 是否保存中间Renko、portfolio等中间数据文件（可选，默认不保存）

## 示例

1. 自动参数优化回测（推荐）：
```bash
# A股直接代码
python src/main.py --symbol 688041 --optimize

# 港股代码需要加 .HK
python src/main.py --symbol 00700.HK --optimize
```

2. 默认回测（自动取过去180天，默认周期和趋势）：
```bash
python src/main.py --symbol 688041
```

3. 指定周期、趋势标准回测：
```bash
python src/main.py --symbol 688041 --start_date 2025-01-01 --end_date 2025-05-01 --renko_mode atr --atr_period 5 --atr_multiplier 0.5 --buy_trend_length 2 --sell_trend_length 2
```

4. 批处理参数优化模式：
```bash
python src/main.py --symbol-list config/symbol_list.json --optimize --start_date 2025-01-01 --end_date 2025-05-01
```

## tools说明

- `get_stockinfo.py`：
  - 功能：自动获取A股（主板、科创板）、深市、港股的股票基础信息，并保存为csv和json文件，同时写入本地数据库。便于后续批量回测和数据同步。
  - 用法：
    ```bash
    python tools/get_stockinfo.py
    ```
    运行后会在 data 目录下生成/更新股票信息相关文件。

- `sync_stock_hist_data.py`：
  - 功能：批量同步A股和港股的历史行情数据，支持多线程、断点续传，数据可选来源（akshare/yfinance），并写入本地数据库。适合大规模数据准备。
  - 用法示例：
    ```bash
    # 批量同步（推荐，symbol_list为股票代码json文件）
    python tools/sync_stock_hist_data.py --symbol_list config/symbol_list.json --start_date 2024-01-01 --threads 4
    
    # 单只股票同步
    python tools/sync_stock_hist_data.py --symbol 688041 --start_date 2024-01-01
    ```
  - 主要参数：
    - `--symbol_list`：股票代码列表json文件路径
    - `--symbol`：单只股票代码
    - `--start_date`/`--end_date`：同步数据的起止日期，不提供end_date会自动获取今日日期
    - `--interval`：数据频率（1d/1wk/1mo）
    - `--query_method`：数据源（akshare/yfinance），默认yfinance
    - `--threads`：线程数（批量时建议>1）

## 输出说明

- 回测结果将保存在 `results` 目录下
- 图片文件名格式：`[Buy|Sell|NA]{symbol}_{start_date}_{end_date}.png`
- 控制台输出包括：
  - 数据获取信息
  - 回测参数
  - 收益统计
  - 优化结果（如果启用优化）

示例输出图表：
![回测结果示例](example_figure.png)

## 注意事项

1. 数据获取可能需要一定时间，请耐心等待
2. 参数优化可能需要较长时间，建议先在小数据集上测试
3. 批处理模式下不会显示图形界面 