import akshare as ak
import json
import pandas as pd
import os

# 路径和文件名变量
DATA_DIR = "data"
FILE_STOCK_INFO_SH = os.path.join(DATA_DIR, "stock_info_sh.csv")
FILE_STOCK_INFO_SH_KCB = os.path.join(DATA_DIR, "stock_info_sh_kcb.csv")
FILE_STOCK_INFO_SZ = os.path.join(DATA_DIR, "stock_info_sz.csv")
FILE_STOCK_INFO_HK = os.path.join(DATA_DIR, "stock_info_hk.csv")
FILE_STOCK_INFO_AH_CODE_NAME = os.path.join(DATA_DIR, "stock_info_ah_code_name.csv")
FILE_STOCK_AH_CODES_ALL = os.path.join(DATA_DIR, "stock_ah_codes_all.json")

def fetch_and_save_stock_info():
    """
    获取A股、科创板、深市、港股的股票信息，保存为csv和json文件。
    """
    if not os.path.exists(FILE_STOCK_INFO_SH):
        stock_info_sh_df = ak.stock_info_sh_name_code(symbol="主板A股")
        stock_info_sh_df["证券代码"] = stock_info_sh_df["证券代码"].astype(str)
        stock_info_sh_df.to_csv(FILE_STOCK_INFO_SH, index=False)
    else:
        stock_info_sh_df = pd.read_csv(FILE_STOCK_INFO_SH, dtype={"证券代码": str})

    if not os.path.exists(FILE_STOCK_INFO_SH_KCB):
        stock_info_sh_df_kcb = ak.stock_info_sh_name_code(symbol="科创板")
        stock_info_sh_df_kcb["证券代码"] = stock_info_sh_df_kcb["证券代码"].astype(str)
        stock_info_sh_df_kcb.to_csv(FILE_STOCK_INFO_SH_KCB, index=False)
    else:
        stock_info_sh_df_kcb = pd.read_csv(FILE_STOCK_INFO_SH_KCB, dtype={"证券代码": str})

    if not os.path.exists(FILE_STOCK_INFO_SZ):
        stock_info_sz_df = ak.stock_info_sz_name_code(symbol="A股列表")
        stock_info_sz_df["A股代码"] = stock_info_sz_df["A股代码"].astype(str)
        stock_info_sz_df.to_csv(FILE_STOCK_INFO_SZ, index=False)
    else:
        stock_info_sz_df = pd.read_csv(FILE_STOCK_INFO_SZ, dtype={"A股代码": str})

    if not os.path.exists(FILE_STOCK_INFO_HK):
        stock_info_hk_df = ak.stock_hk_spot_em()
        stock_info_hk_df['代码'] = stock_info_hk_df['代码'].astype(str)
        stock_info_hk_df.to_csv(FILE_STOCK_INFO_HK, index=False)
    else:
        stock_info_hk_df = pd.read_csv(FILE_STOCK_INFO_HK, dtype={"代码": str})

    # 统一字段名
    sh_df = stock_info_sh_df.rename(columns={"证券代码": "code", "证券简称": "name"})[["code", "name"]]
    kcb_df = stock_info_sh_df_kcb.rename(columns={"证券代码": "code", "证券简称": "name"})[["code", "name"]]
    sz_df = stock_info_sz_df.rename(columns={"A股代码": "code", "A股简称": "name"})[["code", "name"]]
    hk_df = stock_info_hk_df.rename(columns={"代码": "code", "名称": "name"})[["code", "name"]]
    hk_df['code'] = hk_df['code'].apply(lambda x: f"{x}.HK")

    # 合并
    all_df = pd.concat([sh_df, kcb_df, sz_df, hk_df], ignore_index=True)
    all_df.to_csv(FILE_STOCK_INFO_AH_CODE_NAME, index=False)

    # code单独保存为json
    with open(FILE_STOCK_AH_CODES_ALL, 'w', encoding='utf-8') as f:
        json.dump(all_df['code'].tolist(), f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    fetch_and_save_stock_info()

