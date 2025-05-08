#!/bin/bash

# 定义要测试的股票列表
SYMBOLS=(
    "688041.SS"
    "688981.SS"
    "603986.SS"
    "601127.SS"
    "300454.SZ"
    "688111.SS"
    "002456.SZ"
    "600809.SS"
    "600702.SS"
    "0700.HK"
    "2331.HK"
    "688692.SS"
    "002594.SZ"
    "600570.SS"
    "000776.SZ"
    "1810.HK"
    "601888.SS"
    "688318.SS"
    "600036.SS"
    "300454.SZ"
    "688578.SS"
    "002410.SZ"
    "688347.SS"
    "688017.SS"
    "688256.SS"
    "9992.HK"
    "600415.SS"
)

# 设置默认参数值
START_DATE="2025-01-01"
END_DATE="2025-05-01"

# 处理命令行参数
while getopts "t:s:e:" opt; do
  case $opt in
    s) START_DATE="$OPTARG" ;;
    e) END_DATE="$OPTARG" ;;
    \?) echo "无效的选项: -$OPTARG" >&2; exit 1 ;;
    :) echo "选项 -$OPTARG 需要一个参数" >&2; exit 1 ;;
  esac
done

# 创建日志目录
mkdir -p logs

# 并发运行回测
for SYMBOL in "${SYMBOLS[@]}"; do
    # 使用nohup在后台运行，并将输出重定向到日志文件
    nohup python src/main.py \
        --symbol "$SYMBOL" \
        --start_date "$START_DATE" \
        --end_date "$END_DATE" \
        --optimize > "logs/${SYMBOL}.log" \
        --batch 2>&1 &
done

echo "所有回测任务已启动，请查看logs目录下的日志文件" 