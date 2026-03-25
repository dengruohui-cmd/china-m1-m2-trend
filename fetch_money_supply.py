#!/usr/bin/env python3
"""
中国M1/M2及居民存款月度数据抓取脚本
使用 akshare 库获取央行口径数据，支持增量更新
"""

import os
import pandas as pd
from datetime import datetime
import akshare as ak

DATA_FILE = 'money_supply.csv'

def fetch_m1_m2():
    """获取 M1 和 M2 月度数据，返回 DataFrame 包含 date, m1, m2"""
    try:
        df = ak.macro_china_money_supply()
        if 'month' in df.columns:
            df = df[['month', 'm1', 'm2']]
            df.rename(columns={'month': 'date'}, inplace=True)
        else:
            df = df[['日期', 'M1', 'M2']]
            df.rename(columns={'日期': 'date', 'M1': 'm1', 'M2': 'm2'}, inplace=True)
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y%m')
        df = df.sort_values('date')
        return df
    except Exception as e:
        print(f"抓取M1/M2失败: {e}")
        return None

def fetch_household_deposit():
    """获取住户存款月度数据，返回 DataFrame 包含 date, household_deposit"""
    try:
        # 金融机构本外币信贷收支表 - 住户存款
        df = ak.macro_china_depository_institutions_liabilities()
        # 根据 akshare 实际返回的列名处理（通常包含 'item', 'value' 或类似）
        # 我们需要筛选出“住户存款”项，并按日期展开
        # 常见格式：columns = ['日期', '项目', '数值']
        # 我们按日期 pivot 出住户存款
        if '项目' in df.columns:
            # 筛选住户存款
            household = df[df['项目'].str.contains('住户存款')].copy()
            household = household[['日期', '数值']]
            household.rename(columns={'日期': 'date', '数值': 'household_deposit'}, inplace=True)
            household['date'] = pd.to_datetime(household['date']).dt.strftime('%Y%m')
            household = household.sort_values('date')
            return household
        else:
            # 兼容不同版本
            print("警告：无法识别 akshare 返回的列名，请检查 akshare 版本")
            return None
    except Exception as e:
        print(f"抓取住户存款失败: {e}")
        return None

def load_existing_data():
    if os.path.exists(DATA_FILE):
        df = pd.read_csv(DATA_FILE)
        df['date'] = df['date'].astype(str)
        # 确保所有列存在（兼容旧数据）
        for col in ['m1', 'm2', 'household_deposit']:
            if col not in df.columns:
                df[col] = None
        return df
    else:
        return pd.DataFrame(columns=['date', 'm1', 'm2', 'household_deposit'])

def save_data(df):
    df.to_csv(DATA_FILE, index=False)
    print(f"数据已保存至 {DATA_FILE}，共 {len(df)} 条记录")

def main():
    print("=" * 60)
    print("中国M1/M2及居民存款数据抓取工具")
    print("=" * 60)
    
    # 加载已有数据
    existing_df = load_existing_data()
    print(f"已有数据: {len(existing_df)} 条")
    
    # 抓取 M1/M2
    m1m2_df = fetch_m1_m2()
    if m1m2_df is None or m1m2_df.empty:
        print("M1/M2 抓取失败，终止")
        return
    print(f"M1/M2 抓取到 {len(m1m2_df)} 条数据")
    
    # 抓取住户存款
    deposit_df = fetch_household_deposit()
    if deposit_df is None or deposit_df.empty:
        print("住户存款抓取失败，继续使用已有数据")
        deposit_df = pd.DataFrame(columns=['date', 'household_deposit'])
    
    # 合并 M1/M2 和住户存款
    combined = pd.merge(m1m2_df, deposit_df, on='date', how='left')
    
    # 与已有数据合并（去重）
    all_df = pd.concat([existing_df, combined], ignore_index=True)
    all_df = all_df.drop_duplicates(subset=['date'], keep='last')
    all_df = all_df.sort_values('date')
    
    # 只保留过去10年（从当前年份往前推10年）
    current_year = datetime.now().year
    start_year = current_year - 10
    all_df = all_df[all_df['date'].astype(str) >= f"{start_year}01"]
    
    # 确保数值类型
    for col in ['m1', 'm2', 'household_deposit']:
        all_df[col] = pd.to_numeric(all_df[col], errors='coerce')
    
    save_data(all_df)
    print(f"数据已合并，共 {len(all_df)} 条记录（过去10年）")

if __name__ == "__main__":
    main()
