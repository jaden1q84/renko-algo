#!/bin/bash

SYMBOLS=($(<list_for_batch.txt))

# 设置默认参数值，END_DATE 为当前日期，START_DATE 为当前日期前180天
if [[ "$OSTYPE" == "darwin"* ]]; then
    START_DATE=$(date -v-180d +%Y-%m-%d) # macOS 的 date 命令
else
    START_DATE=$(date -d "180 days ago" +%Y-%m-%d) # Linux 的 date 命令
fi
END_DATE=$(date +%Y-%m-%d)

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