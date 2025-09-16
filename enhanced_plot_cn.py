# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Patch
from matplotlib.lines import Line2D
import matplotlib.gridspec as gridspec
import matplotlib.ticker as ticker
import os
from matplotlib import font_manager

# 中文字体：动态优先使用 Noto Sans（云端更稳定）
try:
    available = {f.name for f in font_manager.fontManager.ttflist}
    preferred = ['Noto Sans CJK SC','Noto Sans SC','SimHei','Microsoft YaHei','Arial Unicode MS','DejaVu Sans']
    fam = None
    for f in preferred:
        if f in available:
            fam = f
            break
    if fam is None:
        font_dir = os.path.join(os.path.dirname(__file__), 'fonts')
        os.makedirs(font_dir, exist_ok=True)
        font_path = os.path.join(font_dir, 'NotoSansSC-Regular.otf')
        if not os.path.exists(font_path):
            import urllib.request
            for u in [
                'https://github.com/googlefonts/noto-cjk/raw/main/Sans/OTF/SimplifiedChinese/NotoSansSC-Regular.otf',
                'https://fonts.gstatic.com/ea/notosanssc/v1/NotoSansSC-Regular.otf'
            ]:
                try:
                    urllib.request.urlretrieve(u, font_path)
                    break
                except Exception:
                    continue
        if os.path.exists(font_path):
            font_manager.fontManager.addfont(font_path)
            fam = 'Noto Sans SC'
    if fam:
        matplotlib.rcParams['font.sans-serif'] = [fam,'SimHei','Microsoft YaHei','Arial Unicode MS','DejaVu Sans']
except Exception:
    matplotlib.rcParams['font.sans-serif'] = ['SimHei','Microsoft YaHei','SimSun','DejaVu Sans']

matplotlib.rcParams['axes.unicode_minus'] = False


def _time_to_datetime(series_time):
    base = pd.Timestamp('1900-01-01')
    out = []
    for t in series_time:
        try:
            out.append(base.replace(hour=t.hour, minute=t.minute, second=t.second))
        except Exception:
            out.append(base)
    return np.array(out)


def _time_diff_seconds(t1, t2):
    if pd.isna(t1) or pd.isna(t2):
        return 0
    s1 = t1.hour*3600 + t1.minute*60 + t1.second
    s2 = t2.hour*3600 + t2.minute*60 + t2.second
    diff = s1 - s2
    if diff < 0:
        diff += 24*3600
    return diff


def create_enhanced_figure_cn(df_day: pd.DataFrame, date_str: str = None) -> plt.Figure:
    """
    输入：单日原始数据（包含：上报时间、累计流量、瞬时流量、温度、电池电压、信号值）
    逻辑：严格复刻 water_analysis_enhanced_en.py 的关键点筛选与行为分类，并绘制三分图（中文）。
    """
    df = df_day.copy()

    # 保障字段与类型
    for col in ['累计流量','瞬时流量','温度','电池电压','信号值']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    df['上报时间'] = pd.to_datetime(df['上报时间'], errors='coerce')
    df['时间计算'] = df['上报时间'].dt.time
    # 数据L/s (瞬时流量 m³/h -> L/s)
    if '瞬时流量' in df.columns:
        df['数据L/s'] = df['瞬时流量'] / 3.6
    else:
        df['数据L/s'] = 0

    # 与脚本一致：按时间从晚到早排序
    wm_data1 = df.loc[:, ['时间计算','累计流量','瞬时流量','温度','电池电压','信号值','数据L/s']].copy()
    wm_data1 = wm_data1.sort_values(by='时间计算', ascending=False)
    wm_data1['错位时间'] = wm_data1['时间计算'].shift(1)
    wm_data1['时间差秒'] = wm_data1.apply(lambda r: _time_diff_seconds(r['错位时间'], r['时间计算']), axis=1)

    # 关键点：前后两个差 > 360s
    wm_data2 = wm_data1[wm_data1['时间差秒'] > 360].copy()
    wm_data2 = pd.concat([wm_data2, wm_data1.head(1)], ignore_index=True)
    wm_data2 = wm_data2.sort_values(by='时间计算', ascending=False).reset_index(drop=True)

    # 行为分类（flow_calc）
    if len(wm_data2) > 0:
        wm_data2['错位流量'] = wm_data2['累计流量'].shift(-1)
        wm_data2['区间流量'] = 1000 * (wm_data2['累计流量'] - wm_data2['错位流量'])
        wm_data2['用水行为'] = ''
        wm_data2.loc[wm_data2['区间流量'] > 25, '用水行为'] = '冲洗用水'
        wm_data2.loc[(wm_data2['区间流量'] > 6.5) & (wm_data2['区间流量'] <= 25), '用水行为'] = '桶箱用水'
        wm_data2.loc[wm_data2['区间流量'] <= 6.5, '用水行为'] = '零星用水'
        wm_data2['颜色'] = ''
        wm_data2.loc[wm_data2['用水行为'] == '冲洗用水', '颜色'] = '#FF9999'
        wm_data2.loc[wm_data2['用水行为'] == '桶箱用水', '颜色'] = '#66B2FF'
        wm_data2.loc[wm_data2['用水行为'] == '零星用水', '颜色'] = '#99CC99'
        # 删除最后一行
        if len(wm_data2) > 0:
            wm_data2 = wm_data2.iloc[:-1]

    # 绘图
    time = _time_to_datetime(wm_data1['时间计算'].values)
    acc_flow = wm_data1['累计流量'].values
    flow_rate = wm_data1['数据L/s'].values
    temperature = wm_data1['温度'].values
    battery = wm_data1['电池电压'].values
    signal = wm_data1['信号值'].values

    if len(wm_data2) > 0:
        time2 = _time_to_datetime(wm_data2['时间计算'].values)
        acc_flow2 = wm_data2['累计流量'].values
        inter_flow = wm_data2['区间流量'].values
        activity = wm_data2['用水行为'].values
        color2 = wm_data2['颜色'].values
    else:
        time2 = []; acc_flow2=[]; inter_flow=[]; activity=[]; color2=[]

    # 创建更现代化、高对比度的图表
    fig = plt.figure(figsize=(16, 10), dpi=120)
    fig.patch.set_facecolor('#FFFFFF')  # 纯白背景
    
    # 使用GridSpec更精细地控制子图布局
    gs = gridspec.GridSpec(3, 1, height_ratios=[3, 1, 1], hspace=0.3)

    # 累计/瞬时
    ax1 = fig.add_subplot(gs[0, 0])
    ax1.set_facecolor('#F8F9FA')  # 浅灰色背景增加对比度
    
    # 使用更粗的线条和更鲜明的颜色
    ax1.plot(time, acc_flow, color='#3498DB', linewidth=2.0, label='累计流量')
    ax1.set_ylabel('累计流量 (m^3)', fontsize=13, color='#3498DB', fontweight='bold')
    ax1.grid(True, linestyle='--', alpha=0.7, color='#E0E0E0')

    if len(inter_flow) > 0:
        behavior_count = {'冲洗用水': 0, '桶箱用水': 0, '零星用水': 0}
        for t, f, act, col, vol in zip(time2, acc_flow2, activity, color2, inter_flow):
            if pd.notna(vol) and abs(vol) > 0:
                ax1.scatter(t, f, c=col, s=100*abs(vol), marker='o', alpha=.8, 
                          edgecolor='black', linewidth=0.8, zorder=10)
                behavior_count[act] += 1
                if abs(vol) > 10:
                    time_str = t.strftime('%H:%M')
                    ax1.annotate(
                        f"{abs(vol):.1f}L ({time_str})",
                        xy=(t, f), 
                        xytext=(10, 10 if behavior_count[act] % 2 == 0 else -20),
                        textcoords='offset points', 
                        fontsize=10,
                        bbox=dict(boxstyle='round,pad=0.3', fc='white', ec='gray', alpha=0.9),
                        arrowprops=dict(arrowstyle='->', color='gray', lw=1.2),
                        zorder=11
                    )

    # 增强散点对比度
    ax1.scatter(time, acc_flow, c='#3498DB', s=20, marker='o', alpha=0.7, zorder=5)
    
    # 添加更清晰的时间轴格式
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax1.xaxis.set_major_locator(mdates.HourLocator(interval=2))
    plt.setp(ax1.get_xticklabels(), rotation=45, ha='right', fontsize=10)

    # 瞬时流量轴优化
    ax12 = ax1.twinx()
    ax12.plot(time, flow_rate, color='#E74C3C', linestyle='-', linewidth=1.5, alpha=0.8)
    ax12.scatter(time, flow_rate, c='#E74C3C', s=25, marker='x', linewidth=1.5, alpha=0.8)
    ax12.set_ylabel('瞬时流量 (L/s)', fontsize=13, color='#E74C3C', fontweight='bold')
    
    # 改进图例设计
    legends = [
        Line2D([0],[0], color='#3498DB', lw=2, label='累计流量 (m^3)'),
        Line2D([0],[0], color='#E74C3C', lw=2, linestyle='-', label='瞬时流量 (L/s)'),
        Patch(facecolor='#FF9999', edgecolor='black', label='冲洗用水'),
        Patch(facecolor='#66B2FF', edgecolor='black', label='桶箱用水'),
        Patch(facecolor='#99CC99', edgecolor='black', label='零星用水')
    ]
    legend = ax1.legend(handles=legends, loc='upper left', fontsize=11, framealpha=0.9,
                      bbox_to_anchor=(0.01, 0.99), ncol=2, fancybox=True, shadow=True)
    # 加强图例边框
    legend.get_frame().set_linewidth(1.0)
    legend.get_frame().set_edgecolor('gray')

    # 子图2：温度优化
    ax2 = fig.add_subplot(gs[1, 0])
    ax2.set_facecolor('#F8F9FA')
    ax2.plot(time, temperature, color='#E67E22', linewidth=2.0)
    ax2.scatter(time, temperature, c='#E67E22', s=25, marker='o', alpha=0.8)
    ax2.set_ylabel('温度 (°C)', fontsize=13, color='#E67E22', fontweight='bold')
    ax2.grid(True, linestyle='--', alpha=0.7, color='#E0E0E0')
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax2.xaxis.set_major_locator(mdates.HourLocator(interval=2))
    plt.setp(ax2.get_xticklabels(), rotation=45, ha='right', fontsize=10)
    
    # 子图3：电池/信号强度
    ax3 = fig.add_subplot(gs[2, 0])
    ax3.set_facecolor('#F8F9FA')
    ax3.plot(time, battery, color='#2ECC71', linewidth=2.0, label='电池电压')
    ax3.scatter(time, battery, c='#2ECC71', s=25, marker='o', alpha=0.8)
    ax3.set_ylabel('电池电压 (V)', fontsize=13, color='#2ECC71', fontweight='bold')
    ax3.grid(True, linestyle='--', alpha=0.7, color='#E0E0E0')
    ax3.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax3.xaxis.set_major_locator(mdates.HourLocator(interval=2))
    plt.setp(ax3.get_xticklabels(), rotation=45, ha='right', fontsize=10)

    # 信号强度
    ax32 = ax3.twinx()
    ax32.plot(time, signal, color='#9B59B6', linestyle='-', linewidth=2.0, alpha=0.8, label='信号强度')
    ax32.scatter(time, signal, c='#9B59B6', s=25, marker='^', alpha=0.8)
    ax32.set_ylabel('信号强度 (dBm)', fontsize=13, color='#9B59B6', fontweight='bold')
    
    # 设置整体标题
    title = f"用水行为分析图" if date_str is None else f"用水行为分析图 - {date_str}"
    fig.suptitle(title, fontsize=16, y=0.98, fontweight='bold')

    # 统计摘要
    if len(wm_data2) > 0:
        total_usage = abs(pd.to_numeric(wm_data2['区间流量'], errors='coerce').sum())
        wash_usage = abs(wm_data2.loc[wm_data2['用水行为']=='冲洗用水','区间流量'].sum())
        bucket_usage = abs(wm_data2.loc[wm_data2['用水行为']=='桶箱用水','区间流量'].sum())
        small_usage = abs(wm_data2.loc[wm_data2['用水行为']=='零星用水','区间流量'].sum())
        if total_usage == 0:
            total_usage = 1
        stats_text = (
            f"总用水量: {total_usage:.1f}L\n"
            f"冲洗用水: {wash_usage:.1f}L ({wash_usage/total_usage*100:.1f}%)\n"
            f"桶箱用水: {bucket_usage:.1f}L ({bucket_usage/total_usage*100:.1f}%)\n"
            f"零星用水: {small_usage:.1f}L ({small_usage/total_usage*100:.1f}%)"
        )
        fig.text(0.02, 0.02, stats_text, fontsize=11, 
                 bbox=dict(facecolor='white', alpha=0.9, boxstyle='round,pad=0.6', edgecolor='#3498DB', linewidth=1.2))

    fig.tight_layout()
    fig.subplots_adjust(top=0.92)
    return fig 