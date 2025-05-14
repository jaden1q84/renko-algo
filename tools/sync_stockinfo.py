import sys
import os
import argparse
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import akshare as ak
import json
import pandas as pd
from src.database import DataBase
import logging

# 配置日志
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

# STOCK_HIST_DATA_DB = DataBase('data/stock_hist_data.db')
STOCK_HIST_DATA_DB = None

# 路径和文件名变量
DATA_DIR = "data"
FILE_STOCK_INFO_SH = os.path.join(DATA_DIR, "stock_info_sh.csv")
FILE_STOCK_INFO_SH_KCB = os.path.join(DATA_DIR, "stock_info_sh_kcb.csv")
FILE_STOCK_INFO_SZ = os.path.join(DATA_DIR, "stock_info_sz.csv")
FILE_STOCK_INFO_HK = os.path.join(DATA_DIR, "stock_info_hk.csv")
FILE_STOCK_INFO_AH_CODE_NAME = os.path.join(DATA_DIR, "stock_info_ah_symbol_name.csv")
FILE_STOCK_AH_SYMBOLS_ALL = os.path.join(DATA_DIR, "stock_ah_symbols_all.json")

def fetch_and_save_stock_info(force=False):
    """
    获取A股、科创板、深市、港股的股票信息，保存为csv和json文件。
    force: 是否强制更新所有数据
    """
    if force or not os.path.exists(FILE_STOCK_INFO_SH):
        stock_info_sh_df = ak.stock_info_sh_name_code(symbol="主板A股")
        stock_info_sh_df["证券代码"] = stock_info_sh_df["证券代码"].astype(str)
        stock_info_sh_df.to_csv(FILE_STOCK_INFO_SH, index=False)
    else:
        stock_info_sh_df = pd.read_csv(FILE_STOCK_INFO_SH, dtype={"证券代码": str})

    if force or not os.path.exists(FILE_STOCK_INFO_SH_KCB):
        stock_info_sh_df_kcb = ak.stock_info_sh_name_code(symbol="科创板")
        stock_info_sh_df_kcb["证券代码"] = stock_info_sh_df_kcb["证券代码"].astype(str)
        stock_info_sh_df_kcb.to_csv(FILE_STOCK_INFO_SH_KCB, index=False)
    else:
        stock_info_sh_df_kcb = pd.read_csv(FILE_STOCK_INFO_SH_KCB, dtype={"证券代码": str})

    if force or not os.path.exists(FILE_STOCK_INFO_SZ):
        stock_info_sz_df = ak.stock_info_sz_name_code(symbol="A股列表")
        stock_info_sz_df["A股代码"] = stock_info_sz_df["A股代码"].astype(str)
        stock_info_sz_df.to_csv(FILE_STOCK_INFO_SZ, index=False)
    else:
        stock_info_sz_df = pd.read_csv(FILE_STOCK_INFO_SZ, dtype={"A股代码": str})

    if force or not os.path.exists(FILE_STOCK_INFO_HK):
        stock_info_hk_df = ak.stock_hk_spot_em()
        stock_info_hk_df['代码'] = stock_info_hk_df['代码'].astype(str)
        stock_info_hk_df.to_csv(FILE_STOCK_INFO_HK, index=False)
    else:
        stock_info_hk_df = pd.read_csv(FILE_STOCK_INFO_HK, dtype={"代码": str})

    # 统一字段名
    sh_df = stock_info_sh_df.rename(columns={"证券代码": "symbol", "证券简称": "name"})[["symbol", "name"]]
    kcb_df = stock_info_sh_df_kcb.rename(columns={"证券代码": "symbol", "证券简称": "name"})[["symbol", "name"]]
    sz_df = stock_info_sz_df.rename(columns={"A股代码": "symbol", "A股简称": "name"})[["symbol", "name"]]
    hk_df = stock_info_hk_df.rename(columns={"代码": "symbol", "名称": "name"})[["symbol", "name"]]
    hk_df['symbol'] = hk_df['symbol'].apply(lambda x: f"{x}.HK")

    # 合并
    all_df = pd.concat([sh_df, kcb_df, sz_df, hk_df], ignore_index=True)
    all_df.to_csv(FILE_STOCK_INFO_AH_CODE_NAME, index=False)

    logger.info(f"保存 {len(all_df)} 条股票信息到数据库及csv文件：{FILE_STOCK_INFO_AH_CODE_NAME}")

    # 保存到数据库
    STOCK_HIST_DATA_DB.update_stock_info(all_df)

    # code单独保存为json
    with open(FILE_STOCK_AH_SYMBOLS_ALL, 'w', encoding='utf-8') as f:
        json.dump(all_df['symbol'].tolist(), f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="获取并保存股票信息")
    parser.add_argument('--force', action='store_true', help='强制更新所有数据')
    parser.add_argument('--db-path', type=str, default='data/stock_hist_data.db', help='指定数据库文件路径，默认为data/stock_hist_data.db')
    args = parser.parse_args()

    STOCK_HIST_DATA_DB = DataBase(args.db_path)
    fetch_and_save_stock_info(force=args.force)

