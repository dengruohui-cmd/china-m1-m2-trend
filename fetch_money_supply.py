#!/usr/bin/env python3
"""
中国M1/M2货币供应量月度数据抓取脚本
使用 cn-stats 库获取国家统计局数据，支持增量更新
"""

import os
import pandas as pd
from datetime import datetime
from cnstats.stats import stats
import time

# ========== 配置区 ==========
M2_CODE = 'A0D0101'      # 货币和准货币(M2)供应量_期末值
M1_CODE = 'A0D0103'      # 货币(M1)供应量_期末值
DBCODE = 'hgyd'          # 宏观月度数据库
DATA_FILE = 'money_supply.csv'
# ===========================

def fetch_single_indicator(zbcode, date_str):
    try:
        result = stats(
            zbcode=zbcode,
            datestr=date_str,
            dbcode=DBCODE,
            as_df=True
        )
        if result is not None and len(result) > 0:
            value_row = result[result['指标代码'] == zbcode]
            if len(value_row) > 0:
                return float(value_row.iloc[0]['数值'])
        return None
    except Exception as e:
        print(f"  抓取失败: {e}")
        return None

def fetch_monthly_data(year_start=2016, year_end=None):
    if year_end is None:
        year_end = datetime.now().year
    all_data = []
    for year in range(year_start, year_end + 1):
        for month in range(1, 13):
            date_str = f"{year}{month:02d}"
            if year == year_end and month > datetime.now().month:
                continue
            print(f"正在获取 {date_str}...")
            m2_value = fetch_single_indicator(M2_CODE, date_str)
            time.sleep(0.3)
            m1_value = fetch_single_indicator(M1_CODE, date_str)
            time.sleep(0.3)
            if m2_value is not None or m1_value is not None:
                all_data.append({
                    'date': date_str,
                    'm1': m1_value,
                    'm2': m2_value
                })
                print(f"  ✓ M1: {m1_value:.2f} 亿元, M2: {m2_value:.2f} 亿元")
    return pd.DataFrame(all_data)

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
    print("中国M1/M2货币供应量数据抓取工具")
    print(f"M2指标代码: {M2_CODE} | M1指标代码: {M1_CODE}")
    print("=" * 60)
    existing_df = load_existing_data()
    print(f"已有数据: {len(existing_df)} 条")
    if len(existing_df) > 0:
        latest_date = existing_df['date'].max()
        latest_year = int(latest_date[:4])
        latest_month = int(latest_date[4:6])
        print(f"上次更新至: {latest_date}")
        start_year = latest_year
        start_month = latest_month + 1
        if start_month > 12:
            start_year += 1
            start_month = 1
    else:
        start_year = 2016
        start_month = 1
        print("无历史数据，开始全量抓取...")
    new_data = []
    current_year = start_year
    current_month = start_month
    while current_year <= datetime.now().year:
        date_str = f"{current_year}{current_month:02d}"
        if current_year == datetime.now().year and current_month > datetime.now().month:
            break
        print(f"正在获取 {date_str}...")
        m2_value = fetch_single_indicator(M2_CODE, date_str)
        time.sleep(0.3)
        m1_value = fetch_single_indicator(M1_CODE, date_str)
        time.sleep(0.3)
        if m2_value is not None or m1_value is not None:
            new_data.append({
                'date': date_str,
                'm1': m1_value,
                'm2': m2_value
            })
            print(f"  ✓ M1: {m1_value:.2f} 亿元, M2: {m2_value:.2f} 亿元")
        current_month += 1
        if current_month > 12:
            current_month = 1
            current_year += 1
    if new_data:
        new_df = pd.DataFrame(new_data)
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        combined_df = combined_df.drop_duplicates(subset=['date'], keep='last')
        combined_df = combined_df.sort_values('date')
        save_data(combined_df)
        print(f"本次新增 {len(new_data)} 条记录")
    else:
        print("没有新数据需要更新")
    return combined_df

if __name__ == "__main__":
    main()
