# Renko策略回测系统

这是一个基于Renko图表的量化交易策略回测系统。系统支持两种Renko生成模式：基于ATR和基于日线，并提供参数优化功能。

## 项目结构

```
src/
├── main.py              # 主程序入口
├── renko_backtest.py    # Renko回测核心类
├── data_fetcher.py      # 数据获取模块
├── renko_generator.py   # Renko图表生成器
├── strategy.py          # 交易策略实现
└── backtest_optimizer.py # 参数优化器
```

## 功能特点

- 支持两种Renko生成模式：
  - ATR模式：基于ATR指标动态调整砖块大小
  - 日线模式：基于日线数据生成固定大小的砖块
- 参数优化功能：自动寻找最优策略参数
- 可视化回测结果：包括Renko图表和投资组合价值曲线
- 支持批处理模式：可用于批量回测多个标的

## 使用方法

1. 安装依赖：
```bash
pip install -r requirements.txt
```

2. 运行回测：
```bash
python src/main.py --token YOUR_API_TOKEN --symbol 688041.SH --start_date 2023-01-01 --end_date 2023-12-31
```

3. 参数说明：
- `--token`: API访问令牌（必需）
- `--symbol`: 股票代码（必需）
- `--start_date`: 开始日期（必需）
- `--end_date`: 结束日期（必需）
- `--renko_mode`: Renko生成模式（可选，默认daily）
- `--atr_period`: ATR周期（可选，默认10）
- `--atr_multiplier`: ATR乘数（可选，默认0.5）
- `--buy_trend_length`: 买入信号所需的趋势长度（可选，默认3）
- `--sell_trend_length`: 卖出信号所需的趋势长度（可选，默认3）
- `--optimize`: 是否进行参数优化（可选）
- `--max_iterations`: 最大优化迭代次数（可选，默认500）
- `--batch`: 是否以批处理模式运行（可选）

## 示例

1. 标准回测：
```bash
python src/main.py --token YOUR_API_TOKEN --symbol 688041.SH --start_date 2023-01-01 --end_date 2023-12-31 --renko_mode atr --atr_period 14 --atr_multiplier 1.0 --buy_trend_length 3 --sell_trend_length 3
```

2. 参数优化回测：
```bash
python src/main.py --token YOUR_API_TOKEN --symbol 688041.SH --start_date 2023-01-01 --end_date 2023-12-31 --optimize --max_iterations 500
```

3. 批处理模式：
```bash
python src/main.py --token YOUR_API_TOKEN --symbol 688041.SH --start_date 2023-01-01 --end_date 2023-12-31 --batch
```

## 输出说明

- 回测结果将保存在 `data` 目录下
- 图片文件名格式：`{symbol}_{start_date}_{end_date}.png`
- 控制台输出包括：
  - 数据获取信息
  - 回测参数
  - 收益统计
  - 优化结果（如果启用优化）

示例输出图表：
![回测结果示例](example_figure.png)

## 注意事项

1. 确保API令牌有效
2. 数据获取可能需要一定时间，请耐心等待
3. 参数优化可能需要较长时间，建议先在小数据集上测试
4. 批处理模式下不会显示图形界面 