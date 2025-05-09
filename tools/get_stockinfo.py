import akshare as ak
import json

# stock_info_a_code_name_df = ak.stock_info_a_code_name()
# stock_info_a_code_name_df.to_csv("data/stock_info_a_code_name.csv", index=False)

stock_info_sh_df = ak.stock_info_sh_name_code(symbol="主板A股")
stock_info_sh_df.to_csv("data/stock_info_sh.csv", index=False)

stock_info_sh_df_kcb = ak.stock_info_sh_name_code(symbol="科创板")
stock_info_sh_df_kcb.to_csv("data/stock_info_sh_kcb.csv", index=False)

stock_info_sz_df = ak.stock_info_sz_name_code(symbol="A股列表")
stock_info_sz_df.to_csv("data/stock_info_sz.csv", index=False)

# 提取证券代码并加后缀
sh_codes = stock_info_sh_df['证券代码'].astype(str) + '.SS'
kcb_codes = stock_info_sh_df_kcb['证券代码'].astype(str) + '.SS'
sz_codes = stock_info_sz_df['A股代码'].astype(str) + '.SZ'

# 保存为json
with open('data/stock_codes_sh.json', 'w', encoding='utf-8') as f:
    json.dump(sh_codes.tolist(), f, ensure_ascii=False, indent=2)
with open('data/stock_codes_sh_kcb.json', 'w', encoding='utf-8') as f:
    json.dump(kcb_codes.tolist(), f, ensure_ascii=False, indent=2)
with open('data/stock_codes_sz.json', 'w', encoding='utf-8') as f:
    json.dump(sz_codes.tolist(), f, ensure_ascii=False, indent=2)

