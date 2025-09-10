import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from data_normalizer import normalize_dataframe

st.set_page_config(page_title="数据上传分析", page_icon="🗂️", layout="wide")

st.markdown('# 数据上传分析')

uploaded = st.file_uploader('上传CSV或Excel进行分析', type=['csv','xlsx'])
if uploaded is not None:
    try:
        if uploaded.name.lower().endswith('.xlsx'):
            up_df = pd.read_excel(uploaded)
        else:
            try:
                up_df = pd.read_csv(uploaded)
            except Exception:
                uploaded.seek(0)
                up_df = pd.read_csv(uploaded, encoding='gbk')
        df = normalize_dataframe(up_df)
        if '上报时间' in df.columns:
            df['上报时间'] = pd.to_datetime(df['上报时间'], errors='coerce')
        if df.empty or df['上报时间'].isna().all():
            st.warning('上传数据无法识别或时间解析失败')
            st.stop()
        st.success('上传数据已解析')
        dates = sorted(df['上报时间'].dropna().dt.date.unique(), reverse=True)
        sel_date = st.selectbox('选择日期', dates, format_func=lambda x: x.strftime('%Y-%m-%d'))
        day = df[df['上报时间'].dt.date == sel_date].sort_values('上报时间')
        if day.empty:
            st.info('该日期无数据')
            st.stop()
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=day['上报时间'], y=pd.to_numeric(day['累计流量'], errors='coerce'), name='累计流量(m³)', mode='lines+markers', line=dict(color='#1f77b4')))
        fig.add_trace(go.Scatter(x=day['上报时间'], y=pd.to_numeric(day['瞬时流量'], errors='coerce'), name='瞬时流量(m³/h)', mode='lines+markers', line=dict(color='#ff7f0e', dash='dot'), yaxis='y2'))
        day['累计流量'] = pd.to_numeric(day['累计流量'], errors='coerce')
        day['区间流量'] = day['累计流量'].diff(-1) * -1000
        valid = day[day['区间流量'] > 0]
        if not valid.empty:
            fig.add_trace(go.Bar(x=valid['上报时间'], y=valid['区间流量'], name='区间用水量(L)', marker_color='rgba(158, 202, 225, .7)', yaxis='y3'))
        fig.update_layout(height=450, xaxis_title='时间', yaxis=dict(title='累计流量(m³)'), yaxis2=dict(title='瞬时流量(m³/h)', overlaying='y', side='right'), yaxis3=dict(overlaying='y', side='right', position=0.95, title='区间用水量(L)'))
        st.plotly_chart(fig, width='stretch')
        st.dataframe(day, use_container_width=True)
    except Exception as e:
        st.error(f'解析上传文件出错: {e}') 