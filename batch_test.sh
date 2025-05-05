#!/bin/bash

# 定义要测试的股票列表
SYMBOLS=(
    "688041.SH"
    "688981.SH"
    "603986.SH"
    "601127.SH"
    "300454.SZ"
    "688111.SH"
    "600570.SH"
    "600809.SH"
    "600702.SH"
    "00700.HK"
    "02331.HK"
)

# 设置默认参数值
TOKEN=""
START_DATE="2025-01-01"
END_DATE="2025-05-01"

# 处理命令行参数
while getopts "t:s:e:" opt; do
  case $opt in
    t) TOKEN="$OPTARG" ;;
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
        --token "$TOKEN" \
        --symbol "$SYMBOL" \
        --start_date "$START_DATE" \
        --end_date "$END_DATE" \
        --optimize > "logs/${SYMBOL}.log" \
        --batch 2>&1 &
done

echo "所有回测任务已启动，请查看logs目录下的日志文件" 