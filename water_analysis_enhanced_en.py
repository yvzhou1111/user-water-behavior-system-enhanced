# 导入必要的库
import pandas as pd
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, time
import traceback
import sys
import seaborn as sns
from matplotlib.ticker import MaxNLocator
from matplotlib.patches import Patch
from matplotlib.lines import Line2D

try:
    print("Starting enhanced water meter data analysis script...")
    
    # 设置美观风格
    plt.rcParams['font.family'] = ['SimHei']  # 使用通用字体
    plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号
    sns.set_style("whitegrid")  # 设置seaborn美观风格
    
    # 读取CSV数据文件
    print("Reading CSV data file...")
    wm_data = pd.read_csv("1757125983314设备历史数据数据.csv")
    wm_data = wm_data.fillna(0)
    print(f"Data loaded successfully, {len(wm_data)} rows")
    
    # 将上报时间转换为日期时间格式
    print("Processing date time format...")
    wm_data['上报时间'] = pd.to_datetime(wm_data['上报时间'])
    wm_data['日期计算'] = wm_data['上报时间'].dt.date.astype('datetime64[ns]')
    wm_data['时间计算'] = wm_data['上报时间'].dt.time  # 只保存time对象
    print("Date time processing completed")
    
    # 定义函数selectdate，目的是提取有用的关键数据点
    def selectdate(date):
        print(f"selectdate: Processing date {date}")
        wm_data1 = wm_data[wm_data['日期计算'] == pd.to_datetime(date)]
        print(f"Data points for this date: {len(wm_data1)}")
        if len(wm_data1) == 0:
            print(f"Warning: No data for date {date}")
            return pd.DataFrame(), pd.DataFrame()
            
        wm_data1 = wm_data1.loc[:, ['时间计算', '累计流量', '瞬时流量', '温度', '电池电压', '信号值']]
        # 将瞬时流量转换为L/s (假设原始数据单位是m³/h)
        wm_data1['数据L/s'] = wm_data1['瞬时流量'] / 3.6
        
        # 按时间从晚到早排序
        wm_data1 = wm_data1.sort_values(by='时间计算', ascending=False)
        wm_data1['错位时间'] = wm_data1["时间计算"].shift(1)
        
        # 计算时间差（秒）
        def time_diff_seconds(t1, t2):
            if pd.isna(t1) or pd.isna(t2):
                return 0
            
            # 转换为总秒数
            t1_seconds = t1.hour * 3600 + t1.minute * 60 + t1.second
            t2_seconds = t2.hour * 3600 + t2.minute * 60 + t2.second
            
            # 计算差值（考虑可能跨天的情况）
            diff = t1_seconds - t2_seconds
            if diff < 0:  # 如果是负数，可能是跨天了
                diff += 24 * 3600
            return diff
        
        wm_data1['时间差秒'] = wm_data1.apply(lambda row: time_diff_seconds(row['错位时间'], row['时间计算']), axis=1)
        
        # 筛选前后两个差360秒的数据
        wm_data2 = wm_data1[wm_data1['时间差秒'] > 360].copy()
        wm_data2 = pd.concat([wm_data2, wm_data1.head(1)], ignore_index=True)
        wm_data2 = wm_data2.sort_values(by='时间计算', ascending=False)
        wm_data2 = wm_data2.reset_index(drop=True)
        print(f"Key data points after filtering: {len(wm_data2)}")
        
        return wm_data1, wm_data2
    
    # 定义函数flow_calc，计算关键数据点之间的流量差，并赋用水行为
    def flow_calc(wm_data2):
        if len(wm_data2) == 0:
            print("Warning: No data points for flow calculation")
            return wm_data2
            
        wm_data2['错位流量'] = wm_data2["累计流量"].shift(-1)
        wm_data2['区间流量'] = 1000 * (wm_data2["累计流量"] - wm_data2["错位流量"])
        wm_data2['用水行为'] = ""
        wm_data2.loc[wm_data2["区间流量"] > 25, "用水行为"] = "Flushing"  # 冲洗用水
        wm_data2.loc[(wm_data2["区间流量"] > 6.5) & (wm_data2["区间流量"] <= 25), "用水行为"] = "Bucket"  # 桶箱用水
        wm_data2.loc[wm_data2["区间流量"] <= 6.5, "用水行为"] = "Small Use"  # 零星用水
        
        # 使用更美观的颜色方案
        wm_data2["颜色"] = ""
        wm_data2.loc[wm_data2["用水行为"] == "Flushing", "颜色"] = "#FF9999"  # 浅红色
        wm_data2.loc[wm_data2["用水行为"] == "Bucket", "颜色"] = "#66B2FF"  # 浅蓝色
        wm_data2.loc[wm_data2["用水行为"] == "Small Use", "颜色"] = "#99CC99"  # 浅绿色
        
        # 删除最后一行
        if len(wm_data2) > 0:
            wm_data2.drop(wm_data2.index[-1], inplace=True)
        
        return wm_data2
    
    # 定义函数plotting，用于画图
    def plotting(wm_data1, wm_data2, givendate):
        if len(wm_data1) == 0:
            print(f"Warning: No data to plot for date {givendate}")
            return
            
        print(f"plotting: Drawing enhanced chart for date {givendate}")
        
        # 将time对象转换为datetime以便于绘图
        base_date = pd.Timestamp('1900-01-01')
        
        def time_to_datetime(t):
            try:
                # 检查t是否有hour, minute, second属性
                return base_date.replace(hour=t.hour, minute=t.minute, second=t.second)
            except AttributeError:
                print(f"Warning: Time object {t} type {type(t)} has no hour/minute/second attributes")
                return base_date
        
        time = np.array([time_to_datetime(t) for t in wm_data1["时间计算"].values])
        acc_flow = wm_data1["累计流量"].values
        flow_rate = wm_data1["数据L/s"].values
        temperature = wm_data1["温度"].values
        battery = wm_data1["电池电压"].values
        signal = wm_data1["信号值"].values
        
        if len(wm_data2) > 0:
            time2 = np.array([time_to_datetime(t) for t in wm_data2["时间计算"].values])
            acc_flow2 = wm_data2["累计流量"].values
            inter_flow = wm_data2["区间流量"].values
            activity = wm_data2["用水行为"].values
            color2 = wm_data2["颜色"].values
        else:
            time2 = []
            acc_flow2 = []
            inter_flow = []
            activity = []
            color2 = []
        
        # 创建一个更大的图形，包含多个子图
        fig = plt.figure(figsize=(16, 10), dpi=100)
        fig.patch.set_facecolor('#f8f9fa')
        
        # 设置网格布局
        gs = fig.add_gridspec(3, 1, height_ratios=[3, 1, 1], hspace=0.3)
        
        # 第一个子图：累计流量和瞬时流量
        ax1 = fig.add_subplot(gs[0, 0])
        ax1.set_facecolor('#f8f9fa')
        
        # 绘制累计流量
        ax1.plot(time, acc_flow, color='#1f77b4', linewidth=1.5, label="Cumulative Flow")
        ax1.set_ylabel('Cumulative Flow (m³)', fontsize=14, color='#1f77b4')
        ax1.tick_params(axis='y', labelcolor='#1f77b4', labelsize=12)
        ax1.grid(True, linestyle='--', alpha=0.7)
        
        # 添加关键数据点
        if len(wm_data2) > 0:
            for i, (t, f, act, col, vol) in enumerate(zip(time2, acc_flow2, activity, color2, inter_flow)):
                if pd.notna(vol) and abs(vol) > 0:
                    ax1.scatter(t, f, c=col, s=100 * abs(vol), marker="o", alpha=.8, edgecolor='black', linewidth=0.5)
                    
                    # 在重要点添加标签
                    if abs(vol) > 10:  # 只为较大的用水量添加标签
                        ax1.annotate(f"{abs(vol):.1f}L", 
                                    xy=(t, f), 
                                    xytext=(10, 10),
                                    textcoords='offset points', 
                                    fontsize=9,
                                    bbox=dict(boxstyle="round,pad=0.3", fc="white", alpha=0.7))
        
        # 添加小的数据点
        ax1.scatter(time, acc_flow, c='#1f77b4', s=10, marker="o", alpha=0.5)
        
        # 设置x轴格式
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        ax1.xaxis.set_major_locator(mdates.HourLocator(interval=2))
        plt.setp(ax1.get_xticklabels(), rotation=45, ha='right')
        
        # 设置y轴范围
        if len(wm_data2) > 0:
            y_min = wm_data2['累计流量'].min() - 0.05
            y_max = wm_data2['累计流量'].max() + 0.05
        else:
            y_min = min(acc_flow) - 0.05
            y_max = max(acc_flow) + 0.05
        ax1.set_ylim(y_min, y_max)
        
        # 添加瞬时流量到第二个y轴
        ax1_2 = ax1.twinx()
        ax1_2.plot(time, flow_rate, color='#ff7f0e', linestyle='--', linewidth=1, alpha=0.7)
        ax1_2.scatter(time, flow_rate, c='#ff7f0e', s=25, marker="x", linewidth=1)
        ax1_2.set_ylabel('Instantaneous Flow (L/s)', fontsize=14, color='#ff7f0e')
        ax1_2.tick_params(axis='y', labelcolor='#ff7f0e', labelsize=12)
        
        # 设置瞬时流量y轴范围
        max_flow_rate = max(flow_rate) if len(flow_rate) > 0 else 0.26
        ax1_2.set_ylim(-0.01, max(0.26, max_flow_rate * 1.2))
        
        # 添加图例
        legend_elements = [
            Line2D([0], [0], color='#1f77b4', lw=2, label='Cumulative Flow (m³)'),
            Line2D([0], [0], color='#ff7f0e', lw=2, linestyle='--', label='Instantaneous Flow (L/s)'),
            Patch(facecolor='#FF9999', edgecolor='black', label='Flushing'),
            Patch(facecolor='#66B2FF', edgecolor='black', label='Bucket'),
            Patch(facecolor='#99CC99', edgecolor='black', label='Small Use')
        ]
        ax1.legend(handles=legend_elements, loc='upper left', fontsize=10, framealpha=0.7)
        
        # 第二个子图：温度变化
        ax2 = fig.add_subplot(gs[1, 0])
        ax2.set_facecolor('#f8f9fa')
        ax2.plot(time, temperature, color='#d62728', linewidth=1.5)
        ax2.scatter(time, temperature, c='#d62728', s=15, marker="o", alpha=0.7)
        ax2.set_ylabel('Temperature (°C)', fontsize=12, color='#d62728')
        ax2.tick_params(axis='y', labelcolor='#d62728', labelsize=10)
        ax2.grid(True, linestyle='--', alpha=0.7)
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        ax2.xaxis.set_major_locator(mdates.HourLocator(interval=2))
        plt.setp(ax2.get_xticklabels(), rotation=45, ha='right')
        
        # 第三个子图：电池电压和信号强度
        ax3 = fig.add_subplot(gs[2, 0])
        ax3.set_facecolor('#f8f9fa')
        
        # 绘制电池电压
        ax3.plot(time, battery, color='#2ca02c', linewidth=1.5, label='Battery Voltage')
        ax3.scatter(time, battery, c='#2ca02c', s=15, marker="o", alpha=0.7)
        ax3.set_ylabel('Battery Voltage (V)', fontsize=12, color='#2ca02c')
        ax3.tick_params(axis='y', labelcolor='#2ca02c', labelsize=10)
        ax3.grid(True, linestyle='--', alpha=0.7)
        ax3.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        ax3.xaxis.set_major_locator(mdates.HourLocator(interval=2))
        plt.setp(ax3.get_xticklabels(), rotation=45, ha='right')
        
        # 添加信号强度到第二个y轴
        ax3_2 = ax3.twinx()
        ax3_2.plot(time, signal, color='#9467bd', linestyle='--', linewidth=1.5, alpha=0.7, label='Signal Strength')
        ax3_2.scatter(time, signal, c='#9467bd', s=15, marker="^", alpha=0.7)
        ax3_2.set_ylabel('Signal Strength (dBm)', fontsize=12, color='#9467bd')
        ax3_2.tick_params(axis='y', labelcolor='#9467bd', labelsize=10)
        
        # 添加图例
        lines1, labels1 = ax3.get_legend_handles_labels()
        lines2, labels2 = ax3_2.get_legend_handles_labels()
        ax3.legend(lines1 + lines2, labels1 + labels2, loc='upper right', fontsize=10)
        
        # 设置总标题
        fig.suptitle(f"Water Meter Data Analysis - {givendate}", fontsize=20, y=0.98)
        
        # 添加数据统计信息
        if len(wm_data2) > 0:
            total_usage = abs(wm_data2['区间流量'].sum())
            wash_usage = abs(wm_data2.loc[wm_data2["用水行为"] == "Flushing", "区间流量"].sum())
            bucket_usage = abs(wm_data2.loc[wm_data2["用水行为"] == "Bucket", "区间流量"].sum())
            small_usage = abs(wm_data2.loc[wm_data2["用水行为"] == "Small Use", "区间流量"].sum())
            
            stats_text = (
                f"Total Water Usage: {total_usage:.1f}L\n"
                f"Flushing: {wash_usage:.1f}L ({wash_usage/total_usage*100:.1f}%)\n"
                f"Bucket: {bucket_usage:.1f}L ({bucket_usage/total_usage*100:.1f}%)\n"
                f"Small Use: {small_usage:.1f}L ({small_usage/total_usage*100:.1f}%)"
            )
            
            fig.text(0.02, 0.02, stats_text, fontsize=10, 
                    bbox=dict(facecolor='white', alpha=0.8, boxstyle='round,pad=0.5'))
        
        # 保存图表
        plt.tight_layout()
        plt.subplots_adjust(top=0.92)
        plt.savefig(f"{givendate}_enhanced_en.png", dpi=150)
        print(f"Enhanced chart saved as {givendate}_enhanced_en.png")
        plt.close()
    
    # 从数据中提取可用的日期
    available_dates = wm_data['日期计算'].dt.strftime('%Y-%m-%d').unique()
    print(f"Available dates: {available_dates}")
    
    # 指定要画图的日期 - 使用数据中的前5个日期
    datenum = available_dates[:5]
    print(f"Will process the following dates: {datenum}")
    
    for date in datenum:
        try:
            print(f"\nProcessing date: {date}")
            wmdata, wmdata2 = selectdate(date)
            wmdata2 = flow_calc(wmdata2)
            plotting(wmdata, wmdata2, date)
        except Exception as e:
            print(f"Error processing date {date}: {e}")
            traceback.print_exc()

except Exception as e:
    print(f"Program error: {e}")
    traceback.print_exc()
    
print("Enhanced script execution completed") 