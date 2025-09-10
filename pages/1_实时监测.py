import streamlit as st
import pandas as pd
import os
import datetime
import plotly.graph_objects as go

from data_normalizer import load_and_normalize
from enhanced_plot_cn import create_enhanced_figure_cn

st.set_page_config(page_title="实时监测", page_icon="📊", layout="wide")

st.markdown("# 实时监测")

# 数据源选择
data_file = st.sidebar.selectbox(
    "选择数据源",
    [ "1757125983314设备历史数据数据.csv", "watermeter data1.csv"],
    index=0,
    key="data_src_monitor"
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

# 指标卡片
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("累计流量(m³)", f"{pd.to_numeric(df['累计流量'], errors='coerce').iloc[0]:.3f}")
with col2:
    today = datetime.datetime.now().date()
    dtd = df[df['上报时间'].dt.date == today]
    usage = (pd.to_numeric(dtd['累计流量'], errors='coerce').iloc[0] - pd.to_numeric(dtd['累计流量'], errors='coerce').iloc[-1]) * 1000 if len(dtd) >= 2 else 0
    st.metric("今日用水量(L)", f"{usage:.1f}")
with col3:
    st.metric("平均瞬时流量(m³/h)", f"{pd.to_numeric(df['瞬时流量'], errors='coerce').mean():.4f}")
with col4:
    st.metric("最大瞬时流量(m³/h)", f"{pd.to_numeric(df['瞬时流量'], errors='coerce').max():.4f}")

# 日期选择
dates = sorted(df['上报时间'].dropna().dt.date.unique(), reverse=True)
sel_date = st.selectbox("选择日期", dates, format_func=lambda x: x.strftime('%Y-%m-%d'), key="sel_date_monitor")

# 当日数据与趋势
day = df[df['上报时间'].dt.date == sel_date].sort_values('上报时间')
if day.empty:
    st.info("所选日期无数据")
    st.stop()

st.markdown('## 用水趋势')
fig = go.Figure()
fig.add_trace(go.Scatter(x=day['上报时间'], y=pd.to_numeric(day['累计流量'], errors='coerce'), name='累计流量(m³)', mode='lines+markers', line=dict(color='#1f77b4')))
fig.add_trace(go.Scatter(x=day['上报时间'], y=pd.to_numeric(day['瞬时流量'], errors='coerce'), name='瞬时流量(m³/h)', mode='lines+markers', line=dict(color='#ff7f0e', dash='dot'), yaxis='y2'))
valid = day.assign(累计流量=pd.to_numeric(day['累计流量'], errors='coerce'))
valid['区间流量'] = valid['累计流量'].diff(-1) * -1000
valid = valid[valid['区间流量'] > 0]
if not valid.empty:
    fig.add_trace(go.Bar(x=valid['上报时间'], y=valid['区间流量'], name='区间用水量(L)', marker_color='rgba(158, 202, 225, .7)', yaxis='y3'))
fig.update_layout(height=480, xaxis=dict(title='时间'), yaxis=dict(title='累计流量(m³)'), yaxis2=dict(title='瞬时流量(m³/h)', overlaying='y', side='right'), yaxis3=dict(overlaying='y', side='right', position=0.95, title='区间用水量(L)'))
st.plotly_chart(fig, width='stretch')

# 自动渲染中文增强图（与 water_analysis_enhanced_en 逻辑一致）
st.markdown('## 增强图（中文）')
fig_cn = create_enhanced_figure_cn(day, sel_date.strftime('%Y-%m-%d'))
st.pyplot(fig_cn) 