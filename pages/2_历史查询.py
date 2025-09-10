import streamlit as st
import pandas as pd
import os
import datetime
import plotly.graph_objects as go

from data_normalizer import load_and_normalize

st.set_page_config(page_title="历史查询", page_icon="📚", layout="wide")

st.markdown("# 历史查询")

data_file = st.sidebar.selectbox(
    "选择数据源",
    ["water_meter_data.csv", "1757125983314设备历史数据数据.csv", "watermeter data1.csv"],
    index=0,
    key="data_src_history"
)

if not os.path.exists(data_file):
    st.error(f"找不到数据文件: {data_file}")
    st.stop()

# 加载数据
df = load_and_normalize(data_file)
if '上报时间' not in df.columns:
    st.error('数据缺少 上报时间 列')
    st.stop()

df['上报时间'] = pd.to_datetime(df['上报时间'], errors='coerce')
if df['上报时间'].isna().all():
    st.error('无法解析上报时间为日期时间类型，请检查数据格式')
    st.stop()

# 时间范围选择
col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input('开始日期', df['上报时间'].dt.date.min(), key='start_date_history')
with col2:
    end_date = st.date_input('结束日期', df['上报时间'].dt.date.max(), key='end_date_history')

if start_date > end_date:
    st.error('开始日期不能晚于结束日期')
    st.stop()

filtered = df[(df['上报时间'].dt.date >= start_date) & (df['上报时间'].dt.date <= end_date)].copy()
if filtered.empty:
    st.info('所选日期范围无数据')
    st.stop()

# 每日用水统计
daily_usage = []
for d in pd.date_range(start=start_date, end=end_date):
    ddf = filtered[filtered['上报时间'].dt.date == d.date()].sort_values('上报时间')
    if len(ddf) >= 2:
        x = pd.to_numeric(ddf['累计流量'], errors='coerce')
        du = (x.iloc[0] - x.iloc[-1]) * 1000
        daily_usage.append((d.date(), du))

if daily_usage:
    st.markdown('## 每日用水量统计')
    x = [d.strftime('%Y-%m-%d') for d, _ in daily_usage]
    y = [v for _, v in daily_usage]
    fig_hist = go.Figure()
    fig_hist.add_trace(go.Bar(x=x, y=y, marker_color='#3366CC', name='每日用水量(L)'))
    fig_hist.update_layout(height=380, xaxis_title='日期', yaxis_title='用水量(L)')
    st.plotly_chart(fig_hist, width='stretch')

# 行为分布（粗略基于区间流量阈值）
filtered = filtered.sort_values('上报时间')
filtered['累计流量'] = pd.to_numeric(filtered['累计流量'], errors='coerce')
filtered['区间流量'] = filtered['累计流量'].diff(-1) * -1000
filtered['用水行为'] = '零星用水'
filtered.loc[filtered['区间流量'] > 25, '用水行为'] = '冲洗用水'
filtered.loc[(filtered['区间流量'] > 6.5) & (filtered['区间流量'] <= 25), '用水行为'] = '桶箱用水'

valid = filtered[filtered['区间流量'] > 0]
if not valid.empty:
    st.markdown('## 用水行为分布')
    stats = valid.groupby('用水行为')['区间流量'].agg(['sum','count']).reset_index()
    stats['百分比'] = stats['sum'] / stats['sum'].sum() * 100
    fig_pie = go.Figure(data=[go.Pie(labels=stats['用水行为'], values=stats['sum'], hole=.3)])
    st.plotly_chart(fig_pie, width='stretch')

# 异常统计（示例规则）
large_flow_events = (valid['区间流量'] > 50).sum()
night_usage_events = valid[(valid['上报时间'].dt.hour >= 23) | (valid['上报时间'].dt.hour <= 5)].shape[0]
small_leak_days = (valid['区间流量'] < 1).sum() > 5

st.markdown('## 异常统计')
colx, coly, colz = st.columns(3)
with colx:
    st.metric('异常大流量事件', large_flow_events)
with coly:
    st.metric('夜间用水事件', night_usage_events)
with colz:
    st.metric('疑似漏水', '是' if small_leak_days else '否') 