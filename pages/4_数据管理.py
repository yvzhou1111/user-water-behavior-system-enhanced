import streamlit as st
import pandas as pd
import os
import socket
from data_normalizer import load_and_normalize

st.set_page_config(page_title="数据管理", page_icon="🧰", layout="wide")

st.markdown('# 数据管理')

# 侧边栏：接收服务状态与推送地址
def get_ips():
    ips = set()
    try:
        hostname = socket.gethostname()
        ips.add(socket.gethostbyname(hostname))
        for info in socket.getaddrinfo(socket.gethostname(), None):
            ip = info[4][0]
            if ':' not in ip and not ip.startswith('127.'):
                ips.add(ip)
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(('8.8.8.8', 80))
            ips.add(s.getsockname()[0])
        finally:
            s.close()
    except Exception:
        ips.add('127.0.0.1')
    return sorted(list(ips))

st.sidebar.markdown('### 接收服务')
st.sidebar.write('推送地址 (POST):')
for ip in get_ips():
    st.sidebar.code(f'http://{ip}:8000/api/data')
st.sidebar.write('健康检查:')
st.sidebar.code('http://localhost:8000/health')

# 主区：导出与删除

data_file = st.selectbox(
    "选择数据源",
    ["water_meter_data.csv", "1757125983314设备历史数据数据.csv", "watermeter data1.csv"],
    index=0
)

if not os.path.exists(data_file):
    st.error(f"找不到数据文件: {data_file}")
    st.stop()

df = load_and_normalize(data_file)
if '上报时间' in df.columns:
    df['上报时间'] = pd.to_datetime(df['上报时间'], errors='coerce')

st.markdown('## 导出数据')
if not df.empty:
    csv_bytes = df.to_csv(index=False).encode('utf-8-sig')
    st.download_button('下载当前数据CSV', data=csv_bytes, file_name=f'export_{os.path.basename(data_file)}', mime='text/csv')

st.markdown('## 删除数据（water_meter_data.csv）')
st.caption('危险操作：会直接覆盖保存')
if st.button('加载当前存储文件 water_meter_data.csv'):
    st.session_state['manage_df'] = pd.read_csv('water_meter_data.csv') if os.path.exists('water_meter_data.csv') else pd.DataFrame()

m_df = st.session_state.get('manage_df', pd.DataFrame())
if not m_df.empty:
    m_df['上报时间'] = pd.to_datetime(m_df['上报时间'], errors='coerce')
    col1, col2 = st.columns(2)
    with col1:
        s = st.date_input('删除开始日期')
    with col2:
        e = st.date_input('删除结束日期')
    if st.button('执行删除'):
        keep_df = m_df[~((m_df['上报时间'].dt.date >= s) & (m_df['上报时间'].dt.date <= e))]
        keep_df.to_csv('water_meter_data.csv', index=False, encoding='utf-8')
        st.success(f'删除完成，保留 {len(keep_df)} 条记录') 