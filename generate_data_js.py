#!/usr/bin/env python3
"""
将 money_supply.csv 转换为 JavaScript 数据文件，供 ECharts 使用
"""

import pandas as pd
import json
import os

CSV_FILE = 'money_supply.csv'
JS_FILE = 'docs/data.js'

def main():
    if not os.path.exists(CSV_FILE):
        print(f"错误：{CSV_FILE} 不存在，请先运行 fetch_money_supply.py")
        return

    df = pd.read_csv(CSV_FILE)
    df = df.sort_values('date')
    df['date_str'] = pd.to_datetime(df['date'], format='%Y%m').dt.strftime('%Y-%m')

    xAxis_data = df['date_str'].tolist()
    m1_data = df['m1'].tolist()
    m2_data = df['m2'].tolist()
    
    # 确保 household_deposit 列存在，如果不存在则创建空列表
    if 'household_deposit' in df.columns:
        deposit_data = df['household_deposit'].tolist()
    else:
        deposit_data = [None] * len(df)
        print("警告：CSV 中缺少 household_deposit 列，将使用空数据")

    js_content = f"""// 自动生成的数据文件，请勿手动修改
var chartData = {{
    xAxis: {json.dumps(xAxis_data, ensure_ascii=False)},
    m1: {json.dumps(m1_data)},
    m2: {json.dumps(m2_data)},
    householdDeposit: {json.dumps(deposit_data)}
}};
"""

    os.makedirs('docs', exist_ok=True)
    with open(JS_FILE, 'w', encoding='utf-8') as f:
        f.write(js_content)
    print(f"已生成 {JS_FILE}，共 {len(xAxis_data)} 条数据")
    print(f"数据时间范围：{xAxis_data[0]} 至 {xAxis_data[-1]}")

if __name__ == '__main__':
    main()
