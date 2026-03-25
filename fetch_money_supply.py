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
        df = ak.macro_china_depository_institutions_liabilities()
        print("住户存款原始数据列名:", df.columns.tolist())
        
        # 自动识别列名
        date_col = None
        item_col = None
        value_col = None
        if '日期' in df.columns:
            date_col = '日期'
            item_col = '项目'
            value_col = '数值'
        elif 'date' in df.columns:
            date_col = 'date'
            item_col = 'item'
            value_col = 'value'
        else:
            # 其他格式：假设第一列是日期，第二列是指标，第三列是数值
            if df.shape[1] >= 3:
                date_col = df.columns[0]
                item_col = df.columns[1]
                value_col = df.columns[2]
            else:
                print("无法识别列结构，返回空")
                return None
        
        # 筛选包含“住户存款”的行
        household = df[df[item_col].astype(str).str.contains('住户存款')].copy()
        if household.empty:
            print("未找到住户存款项")
            return None
        
        # 提取需要的列并重命名
        household = household[[date_col, value_col]]
        household.rename(columns={date_col: 'date', value_col: 'household_deposit'}, inplace=True)
        household['date'] = pd.to_datetime(household['date']).dt.strftime('%Y%m')
        household = household.sort_values('date')
        return household
    except Exception as e:
        print(f"抓取住户存款失败: {e}")
        return None

def load_existing_data():
    if os.path.exists(DATA_FILE):
        df = pd.read_csv(DATA_FILE)
        df['date'] = df['date'].astype(str)
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
        print("住户存款抓取失败，将创建空列")
        # 创建一个包含所有日期的空 DataFrame
        deposit_df = pd.DataFrame({'date': m1m2_df['date'], 'household_deposit': None})
    else:
        print(f"住户存款抓取到 {len(deposit_df)} 条数据")
    
    # 合并 M1/M2 和住户存款
    combined = pd.merge(m1m2_df, deposit_df, on='date', how='left')
    
    # 与已有数据合并（去重）
    all_df = pd.concat([existing_df, combined], ignore_index=True)
    all_df = all_df.drop_duplicates(subset=['date'], keep='last')
    all_df = all_df.sort_values('date')
    
    # 只保留过去10年
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
