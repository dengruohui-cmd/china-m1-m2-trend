#!/usr/bin/env python3
"""
生成中国M1/M2货币供应量月度趋势图
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import os

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

DATA_FILE = 'money_supply.csv'
CHART_FILE = 'm1_m2_trend.png'


def load_data():
    """加载数据"""
    if not os.path.exists(DATA_FILE):
        print(f"错误: {DATA_FILE} 不存在，请先运行 fetch_money_supply.py")
        return None
    
    df = pd.read_csv(DATA_FILE)
    df['date_dt'] = pd.to_datetime(df['date'], format='%Y%m')
    df = df.sort_values('date_dt')
    return df


def generate_chart(df):
    """生成M1和M2双线趋势图"""
    fig, ax = plt.subplots(figsize=(14, 7))
    
    # 绘制M2折线（广义货币）
    ax.plot(df['date_dt'], df['m2'] / 10000,  # 转换为万亿元
            linewidth=2.5, color='#2E86AB', marker='o', 
            markersize=2, label='M2 (广义货币)')
    
    # 绘制M1折线（狭义货币）
    ax.plot(df['date_dt'], df['m1'] / 10000,
            linewidth=2, color='#E76F51', marker='s', 
            markersize=2, label='M1 (狭义货币)')
    
    # 设置标题和标签
    ax.set_title('中国M1/M2货币供应量月度趋势（2016-2026）', 
                 fontsize=16, fontweight='bold', pad=20)
    ax.set_xlabel('日期', fontsize=12)
    ax.set_ylabel('货币供应量（万亿元）', fontsize=12)
    
    # 添加图例
    ax.legend(loc='upper left', fontsize=11)
    
    # 设置网格
    ax.grid(True, alpha=0.3, linestyle='--')
    
    # 格式化x轴日期显示
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    ax.xaxis.set_major_locator(mdates.YearLocator(1))
    ax.xaxis.set_minor_locator(mdates.MonthLocator(6))
    plt.xticks(rotation=45, ha='right')
    
    # 设置y轴格式
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:.0f}'))
    
    # 添加最新数据标注
    latest = df.iloc[-1]
    ax.annotate(f'M2: {latest["m2"]/10000:.1f}万亿',
                xy=(latest['date_dt'], latest['m2']/10000),
                xytext=(5, 10), textcoords='offset points',
                fontsize=9, color='#2E86AB')
    ax.annotate(f'M1: {latest["m1"]/10000:.1f}万亿',
                xy=(latest['date_dt'], latest['m1']/10000),
                xytext=(5, -15), textcoords='offset points',
                fontsize=9, color='#E76F51')
    
    # 添加数据来源说明
    ax.text(0.02, -0.08, '数据来源：国家统计局（中国人民银行口径）', 
            transform=ax.transAxes, fontsize=10, color='gray')
    ax.text(0.98, -0.08, f'更新日期：{datetime.now().strftime("%Y-%m-%d")}',
            transform=ax.transAxes, fontsize=10, color='gray', ha='right')
    
    plt.tight_layout()
    plt.savefig(CHART_FILE, dpi=150, bbox_inches='tight')
    plt.close()
    
    print(f"趋势图已保存至 {CHART_FILE}")


def main():
    df = load_data()
    if df is not None and len(df) > 0:
        print(f"数据时间范围: {df['date_dt'].min().strftime('%Y-%m')} 至 {df['date_dt'].max().strftime('%Y-%m')}")
        print(f"数据条数: {len(df)}")
        generate_chart(df)
    else:
        print("没有可用数据")


if __name__ == "__main__":
    main()
