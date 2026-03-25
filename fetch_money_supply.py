#!/usr/bin/env python3
"""
中国M1/M2货币供应量月度数据抓取脚本
使用 akshare 库获取央行口径的货币供应量数据，支持增量更新
"""

import os
import pandas as pd
from datetime import datetime
import akshare as ak

DATA_FILE = 'money_supply.csv'

def fetch_m1_m2():
    """获取 M1 和 M2 月度数据，返回 DataFrame 包含 date, m1, m2"""
    try:
        # akshare 的 macro_china_money_supply 返回包含 M0, M1, M2 的月度数据
        df = ak.macro_china_money_supply()
        # 根据 akshare 返回的列名处理（常见为 'month', 'm0', 'm1', 'm2'）
        if 'month' in df.columns:
            df = df[['month', 'm1', 'm2']]
            df.rename(columns={'month': 'date'}, inplace=True)
        else:
            # 兼容不同版本，假设列名为 '日期', 'M1', 'M2'
            df = df[['日期', 'M1', 'M2']]
            df.rename(columns={'日期': 'date', 'M1': 'm1', 'M2': 'm2'}, inplace=True)
        # 确保日期格式为 YYYYMM 字符串
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y%m')
        # 按日期排序
        df = df.sort_values('date')
        return df
    except Exception as e:
        print(f"抓取失败: {e}")
        return None

def load_existing_data():
    if os.path.exists(DATA_FILE):
        df = pd.read_csv(DATA_FILE)
        df['date'] = df['date'].astype(str)
        return df
    else:
        return pd.DataFrame(columns=['date', 'm1', 'm2'])

def save_data(df):
    df.to_csv(DATA_FILE, index=False)
    print(f"数据已保存至 {DATA_FILE}，共 {len(df)} 条记录")

def main():
    print("=" * 60)
    print("中国M1/M2货币供应量数据抓取工具 (akshare)")
    print("=" * 60)
    
    # 加载已有数据
    existing_df = load_existing_data()
    print(f"已有数据: {len(existing_df)} 条")
    
    # 抓取最新数据
    new_df = fetch_m1_m2()
    if new_df is None or new_df.empty:
        print("抓取失败，请检查网络或数据源")
        return
    
    print(f"抓取到 {len(new_df)} 条数据")
    print(f"最新数据时间范围: {new_df['date'].min()} 至 {new_df['date'].max()}")
    
    # 合并数据（去重，以新数据为准）
    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    combined_df = combined_df.drop_duplicates(subset=['date'], keep='last')
    combined_df = combined_df.sort_values('date')
    
    # 只保留过去10年（从当前年份往前推10年）
    current_year = datetime.now().year
    start_year = current_year - 10
    combined_df = combined_df[combined_df['date'].astype(str) >= f"{start_year}01"]
    
    save_data(combined_df)
    print(f"数据已合并，共 {len(combined_df)} 条记录（过去10年）")

if __name__ == "__main__":
    main()
