import os
import requests
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, date
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Patch
from matplotlib.lines import Line2D
from dotenv import load_dotenv
import time
import socket
import enhanced_plot_cn as enhanced_cn
# 中文字体与负号
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

# Load environment variables
load_dotenv()

# Modern UI configuration
st.set_page_config(
    page_title="用户用水行为识别系统",
    page_icon="💧",
    layout="wide",
    initial_sidebar_state="expanded"
)
# 页面配置结束

# 添加现代UI的CSS样式
st.markdown("""
<style>
    /* 主题颜色 */
    :root {
        --primary-color: #3498db;
        --secondary-color: #2ecc71;
        --background-color: #f8f9fa;
        --text-color: #2c3e50;
        --accent-color: #e74c3c;
    }
    
    /* 整体样式 */
    .reportview-container {
        background-color: var(--background-color);
        color: var(--text-color);
    }
    
    /* 标题样式 */
    h1, h2, h3, h4 {
        color: var(--text-color);
        font-weight: 600;
    }
    h1 {
        font-size: 2.2rem;
        margin-bottom: 1.5rem;
        border-bottom: 2px solid var(--primary-color);
        padding-bottom: 0.5rem;
    }
    h2 {
        font-size: 1.8rem;
        margin-top: 1.5rem;
        margin-bottom: 1rem;
    }
    h3 {
        font-size: 1.4rem;
        margin-top: 1.2rem;
        margin-bottom: 0.8rem;
    }
    
    /* 区块样式 */
    .section-container {
        background-color: white;
        border-radius: 8px;
        padding: 1.5rem;
        box-shadow: 0 2px 6px rgba(0,0,0,0.05);
        margin-bottom: 2rem;
    }
    
    /* 指标样式 */
    .css-1wivap2 {
        background-color: white;
        border-radius: 8px;
        box-shadow: 0 2px 6px rgba(0,0,0,0.05);
        padding: 1rem;
    }
    
    /* 卡片标题 */
    .card-title {
        font-size: 1.1rem;
        font-weight: 600;
        color: var(--primary-color);
        margin-bottom: 0.8rem;
        padding-bottom: 0.4rem;
        border-bottom: 1px solid #eee;
    }
    
    /* 菜单样式 */
    .css-1d391kg {
        background-color: #2c3e50;
    }

    /* 隐藏Streamlit默认多页侧边导航，保留自定义菜单 */
    div[data-testid="stSidebarNav"], nav[aria-label="Main navigation"] { display: none !important; }
    ul[data-testid="stSidebarNavItems"] { display: none !important; }
    
    /* 按钮样式 */
    .stButton>button {
        background-color: var(--primary-color);
        color: white;
        border: none;
        border-radius: 4px;
        padding: 0.5rem 1rem;
        font-weight: 500;
    }
    .stButton>button:hover {
        background-color: #2980b9;
    }
    
    /* 次要按钮 */
    .secondary-button>button {
        background-color: #95a5a6;
    }
    .secondary-button>button:hover {
        background-color: #7f8c8d;
    }
    
    /* 侧边栏样式 */
    .css-1v3fvcr {
        background-color: #34495e;
        color: white;
        padding-top: 2rem;
    }
    .css-1v3fvcr .css-1avcm0n {
        background-color: #2c3e50;
    }
    
    /* 下拉框样式 */
    .stSelectbox label {
        color: var(--text-color);
        font-weight: 500;
    }
    
    /* 成功消息 */
    .element-container .stAlert.success {
        background-color: #d4edda;
        color: #155724;
    }
    
    /* 警告消息 */
    .element-container .stAlert.warning {
        background-color: #fff3cd;
        color: #856404;
    }
    
    /* 错误消息 */
    .element-container .stAlert.error {
        background-color: #f8d7da;
        color: #721c24;
    }
    
    /* 登录表单样式 */
    .login-container {
        max-width: 400px;
        margin: 2rem auto;
        padding: 2rem;
        background-color: white;
        border-radius: 8px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.1);
    }
    
    /* 仪表盘卡片 */
    .dashboard-tile {
        background-color: white;
        border-radius: 8px;
        box-shadow: 0 2px 6px rgba(0,0,0,0.05);
        padding: 1.2rem;
        height: 100%;
    }
    
    /* 仪表盘指标 */
    .metric-value {
        font-size: 2rem;
        font-weight: 700;
        color: var(--primary-color);
    }
    .metric-label {
        font-size: 0.9rem;
        color: #7f8c8d;
        margin-top: 0.3rem;
    }
</style>
""", unsafe_allow_html=True)

# 检测API服务器是否运行
def is_api_running(host='localhost', port=8000, timeout=1):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(timeout)
            s.connect((host, port))
            return True
    except:
        return False

# 获取API地址和端口（在云端优先使用 localhost，避免内网IP不可达）
API_HOST = os.getenv("API_HOST", "localhost")
API_PORT = os.getenv("API_PORT", "8000")

# 在常见云环境（如 Streamlit Cloud、Codespaces）强制回退到 localhost
_cih = os.getenv("CI"); _sc = os.getenv("STREAMLIT_RUNTIME"); _codespace = os.getenv("CODESPACES") or os.getenv("GITPOD_WORKSPACE_ID")
if API_HOST not in ("localhost", "127.0.0.1") and (_cih or _sc or _codespace):
    API_HOST = "localhost"
API_BASE = f"http://{API_HOST}:{API_PORT}"

# 检查API服务器状态
API_AVAILABLE = is_api_running(API_HOST, int(API_PORT))

# 若未运行且允许自动启动，则尝试启动一次
if not API_AVAILABLE and os.getenv("AUTO_START_API", "1") == "1":
    try:
        if "_api_started" not in st.session_state:
            st.session_state._api_started = True
            # 在云端优先以内嵌方式启动API，无法则回退到子进程
            try:
                if _cih or _sc or _codespace or os.getenv("RUN_IN_PROCESS_API", "1") == "1":
                    import api_server_local as _api_mod
                    import uvicorn, threading
                    config = uvicorn.Config(_api_mod.app, host="127.0.0.1", port=int(API_PORT), log_level="warning")
                    server = uvicorn.Server(config)
                    threading.Thread(target=server.run, daemon=True).start()
                else:
                    import subprocess, sys
                    subprocess.Popen([sys.executable, "api_server_local.py"], creationflags=0, env=os.environ.copy())
            except Exception:
                import subprocess, sys
                subprocess.Popen([sys.executable, "api_server_local.py"], creationflags=0, env=os.environ.copy())
            time.sleep(2.0)
            API_AVAILABLE = is_api_running(API_HOST, int(API_PORT))
            if API_AVAILABLE:
                st.toast("本地API已自动启动", icon="✅")
    except Exception:
        pass

# 显示API状态提示
if not API_AVAILABLE:
    st.warning(f"""
    ⚠️ API服务器（{API_BASE}）未运行或无法连接。某些功能可能不可用。
    
    本地运行：
    ```
    python run_app.py --api-port {API_PORT}
    ```
    或仅启动API：
    ```
    python api_server_local.py
    ```
    """)

# API工具函数
def api_get(path: str, timeout: int = 2):
    if not API_AVAILABLE:
        return None
    try:
        r = requests.get(API_BASE + path, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        if "timeout" in str(e).lower():
            st.warning(f"API请求超时: GET {path}")
        return None

def api_post(path: str, json_body: dict, timeout: int = 2):
    if not API_AVAILABLE:
        return None
    try:
        r = requests.post(API_BASE + path, json=json_body, timeout=timeout)
        if r.status_code >= 400:
            try:
                detail = r.json()
            except Exception:
                detail = r.text
            st.warning(f"API请求失败: POST {path} - {detail}")
            return None
        return r.json()
    except Exception as e:
        if "timeout" in str(e).lower():
            st.warning(f"API请求超时: POST {path}")
        return None

def load_csv_safely(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        try:
            return pd.read_csv(path, on_bad_lines='skip', engine='python')
        except Exception:
            return pd.DataFrame()

# -------- 登录 --------
if "auth" not in st.session_state:
    # 尝试从URL参数恢复登录状态（兼容新API）
    qp = st.query_params
    token_val = qp.get("token")
    if token_val:
        token = token_val[0] if isinstance(token_val, list) else token_val
        # 验证token
        if API_AVAILABLE:
            resp = api_get(f"/auth/verify?token={token}")
            if resp and resp.get("ok"):
                st.session_state.auth = True
                st.session_state.role = resp.get("role", "admin")
                st.session_state.username = resp.get("username")
                st.session_state.token = token
            else:
                st.session_state.auth = False
                st.session_state.role = None
                st.session_state.username = None
        else:
            # API不可用时，如果token以demo开头，提供演示模式登录
            if str(token).startswith("demo_"):
                st.session_state.auth = True
                st.session_state.role = "demo"
                st.session_state.username = "演示用户"
                st.session_state.token = token
            else:
                st.session_state.auth = False
                st.session_state.role = None
                st.session_state.username = None
    else:
        st.session_state.auth = False
        st.session_state.role = None
        st.session_state.username = None

def render_login():
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("<div class='login-container'>", unsafe_allow_html=True)
        st.markdown("<h2 style='text-align: center;'>用户用水行为识别系统</h2>", unsafe_allow_html=True)
        st.markdown("<div style='text-align: center; margin-bottom: 2rem;'>", unsafe_allow_html=True)
        st.markdown("💧", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)
        
        st.markdown("<h3 style='text-align: center;'>管理员登录</h3>", unsafe_allow_html=True)
        with st.form("login_form", clear_on_submit=False):
            u = st.text_input("用户名", key="login_user", placeholder="请输入用户名")
            p = st.text_input("密码", type="password", key="login_pass", placeholder="请输入密码")
            
            col1, col2 = st.columns(2)
            with col1:
                submitted = st.form_submit_button("登录", width="stretch")
            with col2:
                demo = st.form_submit_button("演示模式", width="stretch")
            
            if submitted:
                if not u or not p:
                    st.warning("请输入用户名和密码")
                    return
                if not API_AVAILABLE:
                    st.error("API服务器未运行，无法验证登录信息。请启动API服务或使用演示模式。")
                    return
                
                resp = api_post("/auth/login", {"username": u, "password": p}, timeout=8)
                if resp and resp.get("ok"):
                    st.session_state.auth = True
                    st.session_state.role = resp.get("role", "admin")
                    st.session_state.username = u
                    st.session_state.token = resp.get("token")
                    # 将token设置到URL参数中，以便刷新页面时恢复登录状态（新API）
                    st.query_params["token"] = st.session_state.token
                    st.success("登录成功")
                    st.rerun()
                else:
                    st.error("登录失败")
            
            if demo:
                st.session_state.auth = True
                st.session_state.role = "demo"
                st.session_state.username = "演示用户"
                # 演示模式也设置一个伪token
                demo_token = "demo_token_12345"
                st.session_state.token = demo_token
                st.query_params["token"] = demo_token
                st.success("已进入演示模式")
        
        st.markdown("</div>", unsafe_allow_html=True)

# -------- 公共分析函数 --------

def compute_intervals(day_df: pd.DataFrame) -> pd.DataFrame:
    """基于单日数据计算区间流量与用水行为分类。
    要求包含列：上报时间、累计流量。
    返回包含：区间流量(升)、用水行为、时间计算 等列。
    """
    if day_df.empty:
        return pd.DataFrame()
    df = day_df.copy().sort_values('上报时间')
    # 计算相邻点的差值（升）
    df['错位流量'] = df['累计流量'].shift(-1)
    df['区间流量'] = 1000.0 * (df['累计流量'] - df['错位流量'])
    # 过滤正向有效区间
    df = df[pd.notna(df['区间流量'])]
    df = df[df['区间流量'] > 0]
    # 时间列
    df['时间计算'] = df['上报时间'].dt.strftime('%H:%M:%S')
    # 行为分类
    df['用水行为'] = '零星用水'
    df.loc[df['区间流量'] > 25.0, '用水行为'] = '冲洗用水'
    df.loc[(df['区间流量'] > 6.5) & (df['区间流量'] <= 25.0), '用水行为'] = '桶箱用水'
    return df

def create_enhanced_figure_cn(day_df: pd.DataFrame):
    if day_df is None or day_df.empty or '上报时间' not in day_df.columns or '累计流量' not in day_df.columns:
        return None
    df = day_df.copy().sort_values('上报时间')
    # 确保数值列
    for c in ['累计流量','温度','电池电压','信号值','瞬时流量']:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')
    # L/s 由 m³/h 换算
    if '瞬时流量' in df.columns:
        df['数据L/s'] = df['瞬时流量'] / 3.6
    else:
        df['数据L/s'] = None
    # 关键点筛选：相邻时间差 > 360 秒
    df['prev_time'] = df['上报时间'].shift(1)
    df['时间差秒'] = (df['上报时间'] - df['prev_time']).dt.total_seconds()
    wm2 = df[df['时间差秒'] > 360].copy()
    if not df.empty:
        wm2 = pd.concat([wm2, df.iloc[[0]]], ignore_index=True)
        wm2 = wm2.sort_values('上报时间').reset_index(drop=True)
    # 区间流量
    wm2['错位流量'] = wm2['累计流量'].shift(-1)
    wm2['区间流量'] = 1000.0 * (wm2['累计流量'] - wm2['错位流量'])
    wm2 = wm2[pd.notna(wm2['区间流量'])]
    # 行为分类
    wm2['用水行为'] = '零星用水'
    wm2.loc[wm2['区间流量'] > 25.0, '用水行为'] = '冲洗用水'
    wm2.loc[(wm2['区间流量'] > 6.5) & (wm2['区间流量'] <= 25.0), '用水行为'] = '桶箱用水'
    # 时间轴（按日）
    base_date = pd.Timestamp('1900-01-01')
    def to_daytime(ts: pd.Timestamp):
        return base_date.replace(hour=ts.hour, minute=ts.minute, second=ts.second)
    t_all = df['上报时间'].apply(to_daytime)
    t_key = wm2['上报时间'].apply(to_daytime) if not wm2.empty else pd.Series([], dtype='datetime64[ns]')

    fig = plt.figure(figsize=(14, 9), dpi=100)
    gs = fig.add_gridspec(3, 1, height_ratios=[3, 1, 1], hspace=0.3)

    # 子图1：累计流量 + 瞬时流量
    ax1 = fig.add_subplot(gs[0, 0])
    ax1.plot(t_all, df['累计流量'], color='#1f77b4', linewidth=1.5, label='累计流量(m³)')
    ax1.scatter(t_all, df['累计流量'], c='#1f77b4', s=10, alpha=0.5)
    if not wm2.empty:
        sizes = wm2['区间流量'].abs().clip(upper=200) * 2
        colors = wm2['用水行为'].map({'冲洗用水':'#FF9999','桶箱用水':'#66B2FF','零星用水':'#99CC99'}).fillna('#999999')
        ax1.scatter(t_key, wm2['累计流量'], c=colors, s=sizes, edgecolors='black', linewidths=0.5, alpha=0.8)
    ax1.set_ylabel('累计流量 (m³)', fontsize=12, color='#1f77b4')
    ax1.tick_params(axis='y', labelcolor='#1f77b4')
    ax1.grid(True, linestyle='--', alpha=0.5)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    # 次轴：L/s
    ax1b = ax1.twinx()
    if '数据L/s' in df.columns:
        ax1b.plot(t_all, df['数据L/s'], color='#ff7f0e', linestyle='--', linewidth=1, alpha=0.8, label='瞬时流量(L/s)')
        ax1b.scatter(t_all, df['数据L/s'], c='#ff7f0e', s=20, marker='x', linewidth=1)
    ax1b.set_ylabel('瞬时流量 (L/s)', fontsize=12, color='#ff7f0e')
    ax1b.tick_params(axis='y', labelcolor='#ff7f0e')

    # 图例
    legend_elements = [
        Line2D([0], [0], color='#1f77b4', lw=2, label='累计流量(m³)'),
        Line2D([0], [0], color='#ff7f0e', lw=2, linestyle='--', label='瞬时流量(L/s)'),
        Patch(facecolor='#FF9999', edgecolor='black', label='冲洗用水'),
        Patch(facecolor='#66B2FF', edgecolor='black', label='桶箱用水'),
        Patch(facecolor='#99CC99', edgecolor='black', label='零星用水')
    ]
    ax1.legend(handles=legend_elements, loc='upper left', fontsize=10, framealpha=0.6)

    # 子图2：温度
    ax2 = fig.add_subplot(gs[1, 0])
    if '温度' in df.columns:
        ax2.plot(t_all, df['温度'], color='#d62728', linewidth=1.5)
        ax2.scatter(t_all, df['温度'], c='#d62728', s=15, alpha=0.7)
    ax2.set_ylabel('温度 (°C)', fontsize=12, color='#d62728')
    ax2.tick_params(axis='y', labelcolor='#d62728')
    ax2.grid(True, linestyle='--', alpha=0.5)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))

    # 子图3：电池电压 + 信号
    ax3 = fig.add_subplot(gs[2, 0])
    if '电池电压' in df.columns:
        ax3.plot(t_all, df['电池电压'], color='#2ca02c', linewidth=1.5, label='电池电压(V)')
        ax3.scatter(t_all, df['电池电压'], c='#2ca02c', s=15, alpha=0.7)
    ax3.set_ylabel('电池电压 (V)', fontsize=12, color='#2ca02c')
    ax3.tick_params(axis='y', labelcolor='#2ca02c')
    ax3.grid(True, linestyle='--', alpha=0.5)
    ax3.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax3b = ax3.twinx()
    if '信号值' in df.columns:
        ax3b.plot(t_all, df['信号值'], color='#9467bd', linestyle='--', linewidth=1.2, alpha=0.8, label='信号强度(dBm)')
        ax3b.scatter(t_all, df['信号值'], c='#9467bd', s=15, marker='^', alpha=0.7)
    ax3b.set_ylabel('信号强度 (dBm)', fontsize=12, color='#9467bd')
    ax3b.tick_params(axis='y', labelcolor='#9467bd')

    fig.suptitle('增强图（中文）', fontsize=16, y=0.98)
    plt.tight_layout()
    return fig

# -------- 实时监测 --------

def render_realtime():
    st.markdown("<h2>实时监测</h2>", unsafe_allow_html=True)
    st.markdown("<div class='section-container'>", unsafe_allow_html=True)
    # 数据源优先级：真实推送(device_push_data.csv) > 历史采集(water_meter_data.csv)
    sources = []
    has_real = os.path.exists("device_push_data.csv")
    has_hist = os.path.exists("water_meter_data.csv")
    if has_real:
        sources.append("device_push_data.csv")
    if has_hist:
        sources.append("water_meter_data.csv")
    if not sources:
        sources = ["water_meter_data.csv"]
    # 默认选择真实数据（若存在）
    default_index = 0
    ds = st.selectbox("选择数据源", sources, index=default_index, key="realtime_source", help="优先选择真实推送数据，可手动切换")

    df = load_csv_safely(ds)
    if df.empty:
        st.info("暂无数据")
        # 如果选的是历史数据但存在真实推送数据，自动切换
        if ds != "device_push_data.csv" and os.path.exists("device_push_data.csv"):
            st.session_state.realtime_source = "device_push_data.csv"
            st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)
        return

    if '上报时间' in df.columns:
        df['上报时间'] = pd.to_datetime(df['上报时间'], errors='coerce')
        df = df.dropna(subset=['上报时间']).sort_values('上报时间', ascending=True)
    for col in ['累计流量', '瞬时流量', '电池电压', '温度', '信号值']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    latest = df.iloc[-1]
    
    st.markdown("<div class='card-title'>实时指标</div>", unsafe_allow_html=True)
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        val = latest.get('累计流量')
        st.markdown(f"<div class='dashboard-tile'>", unsafe_allow_html=True)
        st.markdown(f"<div class='metric-value'>{val:.3f}</div>" if pd.notna(val) else "<div class='metric-value'>-</div>", unsafe_allow_html=True)
        st.markdown(f"<div class='metric-label'>累计流量(m³)</div>", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)
    with col2:
        today = date.today()
        # 优先使用 '日期计算' 过滤，避免时区/解析误差；回退到上报时间日期
        if '日期计算' in df.columns:
            today_str = today.strftime('%Y-%m-%d')
            dtd = df[df['日期计算'] == today_str].copy()
        else:
            dtd = df[df['上报时间'].dt.date == today].copy()
        # 保证排序与数值类型
        if '上报时间' in dtd.columns:
            dtd = dtd.sort_values('上报时间')
        if '累计流量' in dtd.columns:
            dtd['累计流量'] = pd.to_numeric(dtd['累计流量'], errors='coerce')
        # 计算用水量：优先使用当日最大-最小累计流量；若仍为0则用正向增量和
        if len(dtd) >= 1 and '累计流量' in dtd.columns:
            max_min_usage = (dtd['累计流量'].max() - dtd['累计流量'].min()) * 1000.0
            inc_sum_usage = dtd['累计流量'].diff().clip(lower=0).sum() * 1000.0
            usage = float(max(max_min_usage, inc_sum_usage)) if pd.notna(max_min_usage) else float(inc_sum_usage or 0.0)
        else:
            usage = 0.0
        st.markdown(f"<div class='dashboard-tile'>", unsafe_allow_html=True)
        st.markdown(f"<div class='metric-value'>{usage:.1f}</div>", unsafe_allow_html=True)
        st.markdown(f"<div class='metric-label'>今日用水量(L)</div>", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)
    with col3:
        avg_q = df['瞬时流量'].mean() if '瞬时流量' in df.columns else None
        st.markdown(f"<div class='dashboard-tile'>", unsafe_allow_html=True)
        st.markdown(f"<div class='metric-value'>{avg_q:.4f}</div>" if pd.notna(avg_q) else "<div class='metric-value'>-</div>", unsafe_allow_html=True)
        st.markdown(f"<div class='metric-label'>平均瞬时流量(m³/h)</div>", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)
    with col4:
        max_q = df['瞬时流量'].max() if '瞬时流量' in df.columns else None
        st.markdown(f"<div class='dashboard-tile'>", unsafe_allow_html=True)
        st.markdown(f"<div class='metric-value'>{max_q:.4f}</div>" if pd.notna(max_q) else "<div class='metric-value'>-</div>", unsafe_allow_html=True)
        st.markdown(f"<div class='metric-label'>最大瞬时流量(m³/h)</div>", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("<div class='card-title'>流量趋势</div>", unsafe_allow_html=True)

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.1, row_heights=[0.65, 0.35], subplot_titles=("累计与瞬时流量", "温度与电池"))
    if '累计流量' in df.columns:
        fig.add_trace(go.Scatter(x=df['上报时间'], y=df['累计流量'], mode='lines+markers', name='累计流量(m³)', line=dict(color='#3498db', width=2)), row=1, col=1)
    if '瞬时流量' in df.columns:
        fig.add_trace(go.Scatter(x=df['上报时间'], y=df['瞬时流量'], mode='lines+markers', name='瞬时流量(m³/h)', line=dict(color='#e74c3c', width=2, dash='dot')), row=1, col=1)
    if '温度' in df.columns:
        fig.add_trace(go.Scatter(x=df['上报时间'], y=df['温度'], mode='lines+markers', name='温度(°C)', line=dict(color='#f39c12', width=2)), row=2, col=1)
    if '电池电压' in df.columns:
        fig.add_trace(go.Scatter(x=df['上报时间'], y=df['电池电压'], mode='lines+markers', name='电池电压(V)', line=dict(color='#2ecc71', width=2)), row=2, col=1)
    
    fig.update_layout(
        height=560, 
        legend=dict(orientation='h', yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=40, r=40, t=60, b=40),
        plot_bgcolor='white',
        paper_bgcolor='white',
        font=dict(color='#2c3e50'),
        xaxis=dict(gridcolor='#f0f0f0', showgrid=True),
        yaxis=dict(gridcolor='#f0f0f0', showgrid=True),
        xaxis2=dict(gridcolor='#f0f0f0', showgrid=True),
        yaxis2=dict(gridcolor='#f0f0f0', showgrid=True)
    )
    st.plotly_chart(fig, width="stretch")

    # 中文增强图（与历史页一致风格）
    st.markdown("<div class='card-title'>增强图（中文）</div>", unsafe_allow_html=True)
    try:
        # 当天数据用于增强图
        day_df = df[df['上报时间'].dt.date == date.today()].copy()
        if not day_df.empty:
            fig_cn = enhanced_cn.create_enhanced_figure_cn(day_df, date_str=date.today().strftime('%Y-%m-%d'))
            st.pyplot(fig_cn, clear_figure=True)
        else:
            st.info("今日暂无数据用于绘制增强图")
    except Exception as e:
        st.warning(f"增强图绘制失败: {e}")
    
    # 添加自动刷新功能
    st.markdown("<div class='card-title'>数据自动刷新</div>", unsafe_allow_html=True)
    refresh_interval = st.slider("刷新间隔(秒)", min_value=5, max_value=60, value=30, step=5)
    st.markdown(f"""
    <div style='margin-top: 10px;'>
        <meta http-equiv="refresh" content="{refresh_interval}">
        <p>数据每 {refresh_interval} 秒自动刷新一次</p>
        <p>最后更新时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </div>
    """, unsafe_allow_html=True)
    
    # 最新数据点
    st.markdown("<div class='card-title'>最新数据</div>", unsafe_allow_html=True)
    last_points = df.tail(5).sort_values('上报时间', ascending=False)
    st.dataframe(last_points, width="stretch", height=200)
    
    st.markdown("</div>", unsafe_allow_html=True)

# -------- 历史查询与异常检测 --------

def render_history():
    st.markdown("<h2>历史查询</h2>", unsafe_allow_html=True)
    st.markdown("<div class='section-container'>", unsafe_allow_html=True)
    
    df = load_csv_safely("water_meter_data.csv")
    if df.empty or '上报时间' not in df.columns:
        st.info("暂无数据")
        st.markdown("</div>", unsafe_allow_html=True)
        return
    df['上报时间'] = pd.to_datetime(df['上报时间'], errors='coerce')
    df = df.dropna(subset=['上报时间']).sort_values('上报时间')
    for col in ['累计流量', '温度', '电池电压', '信号值']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    dates = sorted(df['上报时间'].dt.date.unique(), reverse=True)
    
    st.markdown("<div class='card-title'>选择日期</div>", unsafe_allow_html=True)
    sel = st.selectbox("选择要查看的日期", dates, index=0 if dates else None, format_func=lambda x: x.strftime('%Y-%m-%d'))
    if not sel:
        st.markdown("</div>", unsafe_allow_html=True)
        return
        
    day_df = df[df['上报时间'].dt.date == sel]
    iv = compute_intervals(day_df)

    st.markdown("<div class='card-title'>用水趋势分析</div>", unsafe_allow_html=True)
    col1, col2 = st.columns([2, 1])
    with col1:
        # 趋势 + 区间柱状
        fig = make_subplots(rows=1, cols=1)
        fig.add_trace(go.Scatter(x=day_df['上报时间'], y=day_df['累计流量'], mode='lines+markers', 
                                name='累计流量(m³)', line=dict(color='#3498db', width=2)))
        if not iv.empty:
            fig.add_trace(go.Bar(x=day_df['上报时间'].iloc[:len(iv)], y=iv['区间流量'], 
                                name='区间用水量(L)', marker_color='rgba(52, 152, 219, 0.5)'))
        fig.update_layout(
            height=420, 
            margin=dict(l=40, r=40, t=40, b=40),
            plot_bgcolor='white',
            paper_bgcolor='white',
            font=dict(color='#2c3e50'),
            xaxis=dict(gridcolor='#f0f0f0', showgrid=True),
            yaxis=dict(gridcolor='#f0f0f0', showgrid=True)
        )
        st.plotly_chart(fig, width="stretch")
    with col2:
        st.markdown("<div style='text-align: center;'><h4>用水行为分布</h4></div>", unsafe_allow_html=True)
        if not iv.empty:
            stats = iv.groupby('用水行为')['区间流量'].agg(['sum', 'count']).reset_index()
            colors = ['#3498db', '#2ecc71', '#e74c3c']
            figp = go.Figure(go.Pie(
                labels=stats['用水行为'], 
                values=stats['sum'], 
                textinfo='percent+label',
                marker=dict(colors=colors),
                hole=0.4
            ))
            figp.update_layout(
                height=420, 
                margin=dict(l=10, r=10, t=40, b=10),
                legend=dict(orientation="v", yanchor="middle", y=0.5, xanchor="center", x=0.5)
            )
            st.plotly_chart(figp)
        else:
            st.info("无有效区间")
    
    st.markdown("<div class='card-title'>增强图（中文）</div>", unsafe_allow_html=True)
    fig_cn = create_enhanced_figure_cn(day_df)
    if fig_cn:
        st.pyplot(fig_cn, clear_figure=True)

    st.markdown("<div class='card-title'>异常检测</div>", unsafe_allow_html=True)
    if not iv.empty:
        # 大流量
        large_flow = iv[iv['区间流量'] > 50]
        # 夜间用水
        day_df = day_df.copy()
        day_df['hour'] = day_df['上报时间'].dt.hour
        night_usage = iv.merge(day_df[['上报时间','hour']], left_index=True, right_index=True, how='left')
        night_usage = night_usage[(night_usage['hour'] >= 23) | (night_usage['hour'] <= 5)]
        night_usage = night_usage[night_usage['区间流量'] > 5]
        # 疑似漏水：持续小流量
        small_cont = iv[(iv['区间流量'] > 0) & (iv['区间流量'] < 1)]
        leak = len(small_cont) > 5
        
        # 展示异常指标
        colx, coly, colz = st.columns(3)
        with colx:
            st.markdown(f"<div class='dashboard-tile'>", unsafe_allow_html=True)
            st.markdown(f"<div class='metric-value'>{len(large_flow)}</div>", unsafe_allow_html=True)
            st.markdown(f"<div class='metric-label'>异常大流量次数</div>", unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)
        with coly:
            st.markdown(f"<div class='dashboard-tile'>", unsafe_allow_html=True)
            st.markdown(f"<div class='metric-value'>{len(night_usage)}</div>", unsafe_allow_html=True)
            st.markdown(f"<div class='metric-label'>夜间用水次数</div>", unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)
        with colz:
            status_color = "#e74c3c" if leak else "#2ecc71"
            st.markdown(f"<div class='dashboard-tile'>", unsafe_allow_html=True)
            st.markdown(f"<div class='metric-value' style='color:{status_color};'>{'是' if leak else '否'}</div>", unsafe_allow_html=True)
            st.markdown(f"<div class='metric-label'>疑似漏水</div>", unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)
        
        # 区间流量数据表格
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown("<div class='card-title'>区间用水详情</div>", unsafe_allow_html=True)
        st.dataframe(iv[['时间计算','区间流量','用水行为']].sort_values('区间流量', ascending=False), 
                    width="stretch", height=260)
    else:
        st.info("无异常记录")
    
    st.markdown("</div>", unsafe_allow_html=True)

# -------- 上传分析 --------

def _read_any(file) -> pd.DataFrame:
    try:
        if hasattr(file, 'name') and str(file.name).lower().endswith(('.xlsx','.xls')):
            return pd.read_excel(file)
        return pd.read_csv(file)
    except Exception:
        try:
            file.seek(0)
            return pd.read_csv(file, encoding='gbk')
        except Exception:
            return pd.DataFrame()

def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    rename_map = {
        '上传时间':'上报时间','上报时间':'上报时间','时间':'上报时间',
        'device_no':'表号','表号':'表号','deviceNo':'表号',
        'IMEI':'imei号','IMEI号':'imei号','imei':'imei号','imei号':'imei号',
        '累计':'累计流量','累计流量':'累计流量','总流量':'累计流量',
        '瞬时流量':'瞬时流量','数据L/s':'数据L/s','温度':'温度','电池电压':'电池电压','信号值':'信号值'
    }
    cols = {c: rename_map.get(c, c) for c in df.columns}
    df = df.rename(columns=cols)
    # 时间
    if '上报时间' in df.columns:
        df['上报时间'] = pd.to_datetime(df['上报时间'], errors='coerce')
        df = df.dropna(subset=['上报时间']).sort_values('上报时间')
    # 数值
    for c in ['累计流量','温度','电池电压','信号值','瞬时流量','数据L/s']:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')
    # 若只有 数据L/s，则换算为 瞬时流量(m³/h)
    if '瞬时流量' not in df.columns and '数据L/s' in df.columns:
        df['瞬时流量'] = df['数据L/s'] * 3.6
    return df

def render_upload_analysis():
    st.markdown("<h2>上传分析</h2>", unsafe_allow_html=True)
    st.markdown("<div class='section-container'>", unsafe_allow_html=True)
    
    st.markdown("<div class='card-title'>上传数据文件</div>", unsafe_allow_html=True)
    
    col1, col2 = st.columns([3, 1])
    with col1:
        file = st.file_uploader("上传 CSV/Excel 文件进行分析", type=["csv","xlsx","xls"], key="uploader")
    with col2:
        st.markdown("<div style='padding-top: 40px;'></div>", unsafe_allow_html=True)
        example_btn = st.button("使用示例数据", key="use_example")
    
    if example_btn:
        # 使用预置的示例数据
        if os.path.exists("1757125983314设备历史数据数据.csv"):
            raw = pd.read_csv("1757125983314设备历史数据数据.csv")
            st.success("已加载示例数据: 1757125983314设备历史数据数据.csv")
        else:
            st.error("示例数据文件不存在")
            st.markdown("</div>", unsafe_allow_html=True)
        return
    elif not file:
        st.info("请上传CSV或Excel格式的数据文件，或使用示例数据")
        st.markdown("</div>", unsafe_allow_html=True)
        return
    else:
        raw = _read_any(file)
        if raw.empty:
            st.error("无法读取文件，请检查格式/编码")
            st.markdown("</div>", unsafe_allow_html=True)
            return
        
        df = _normalize(raw)
        if df.empty or '上报时间' not in df.columns:
            st.error("无法标准化数据：缺少 '上报时间' 列")
            st.markdown("</div>", unsafe_allow_html=True)
            return
        
        dates = sorted(df['上报时间'].dt.date.unique(), reverse=True)
        
        st.markdown("<div class='card-title'>选择日期</div>", unsafe_allow_html=True)
        sel = st.selectbox("选择要分析的日期", dates, index=0 if dates else None, format_func=lambda x: x.strftime('%Y-%m-%d'))
        if not sel:
            st.markdown("</div>", unsafe_allow_html=True)
            return
        
        day_df = df[df['上报时间'].dt.date == sel]
        iv = compute_intervals(day_df)
        
        # 数据统计摘要
        st.markdown("<div class='card-title'>数据统计摘要</div>", unsafe_allow_html=True)
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.markdown(f"<div class='dashboard-tile'>", unsafe_allow_html=True)
            st.markdown(f"<div class='metric-value'>{len(day_df)}</div>", unsafe_allow_html=True)
            st.markdown(f"<div class='metric-label'>数据点数量</div>", unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)
        with col2:
            time_span = (day_df['上报时间'].max() - day_df['上报时间'].min()).total_seconds() / 3600
            st.markdown(f"<div class='dashboard-tile'>", unsafe_allow_html=True)
            st.markdown(f"<div class='metric-value'>{time_span:.2f}</div>", unsafe_allow_html=True)
            st.markdown(f"<div class='metric-label'>时间跨度(小时)</div>", unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)
        with col3:
            if '累计流量' in day_df.columns and len(day_df) > 1:
                flow_diff = (day_df['累计流量'].iloc[0] - day_df['累计流量'].iloc[-1]) * 1000
                st.markdown(f"<div class='dashboard-tile'>", unsafe_allow_html=True)
                st.markdown(f"<div class='metric-value'>{abs(flow_diff):.1f}</div>", unsafe_allow_html=True)
                st.markdown(f"<div class='metric-label'>总用水量(L)</div>", unsafe_allow_html=True)
                st.markdown("</div>", unsafe_allow_html=True)
            else:
                st.markdown(f"<div class='dashboard-tile'>", unsafe_allow_html=True)
                st.markdown(f"<div class='metric-value'>-</div>", unsafe_allow_html=True)
                st.markdown(f"<div class='metric-label'>总用水量(L)</div>", unsafe_allow_html=True)
                st.markdown("</div>", unsafe_allow_html=True)
        with col4:
            if not iv.empty:
                intervals = len(iv)
                st.markdown(f"<div class='dashboard-tile'>", unsafe_allow_html=True)
                st.markdown(f"<div class='metric-value'>{intervals}</div>", unsafe_allow_html=True)
                st.markdown(f"<div class='metric-label'>用水区间数</div>", unsafe_allow_html=True)
                st.markdown("</div>", unsafe_allow_html=True)
            else:
                st.markdown(f"<div class='dashboard-tile'>", unsafe_allow_html=True)
                st.markdown(f"<div class='metric-value'>0</div>", unsafe_allow_html=True)
                st.markdown(f"<div class='metric-label'>用水区间数</div>", unsafe_allow_html=True)
                st.markdown("</div>", unsafe_allow_html=True)
        
        st.markdown("<div class='card-title'>趋势图</div>", unsafe_allow_html=True)
        fig = make_subplots(rows=1, cols=1)
        if '累计流量' in day_df.columns:
            fig.add_trace(go.Scatter(x=day_df['上报时间'], y=day_df['累计流量'], 
                                  mode='lines+markers', name='累计流量(m³)', 
                                  line=dict(color='#3498db', width=2)))
        if not iv.empty:
            fig.add_trace(go.Bar(x=day_df['上报时间'].iloc[:len(iv)], y=iv['区间流量'], 
                              name='区间用水量(L)', marker_color='rgba(52, 152, 219, 0.5)'))
        
        fig.update_layout(
            height=450,
            margin=dict(l=40, r=40, t=40, b=40),
            plot_bgcolor='white',
            paper_bgcolor='white',
            font=dict(color='#2c3e50'),
            xaxis=dict(title='时间', gridcolor='#f0f0f0', showgrid=True),
            yaxis=dict(title='累计流量(m³)/区间用水量(L)', gridcolor='#f0f0f0', showgrid=True)
        )
        st.plotly_chart(fig, width="stretch")

        # 用水行为分布
        if not iv.empty:
            st.markdown("<div class='card-title'>用水行为分布</div>", unsafe_allow_html=True)
            col1, col2 = st.columns([2, 1])
            with col1:
                # 行为分布表格
                behavior_stats = iv.groupby('用水行为').agg({
                    '区间流量': ['sum', 'mean', 'count']
                }).reset_index()
                behavior_stats.columns = ['用水行为', '总量(L)', '平均(L)', '次数']
                if '总量(L)' in behavior_stats.columns:
                    behavior_stats['百分比'] = (behavior_stats['总量(L)'] / behavior_stats['总量(L)'].sum() * 100).round(1).astype(str) + '%'
                st.dataframe(behavior_stats, width="stretch")
            with col2:
                colors = {'冲洗用水': '#e74c3c', '桶箱用水': '#3498db', '零星用水': '#2ecc71'}
                fig = go.Figure(data=[go.Pie(
                    labels=behavior_stats['用水行为'],
                    values=behavior_stats['总量(L)'],
                    hole=.4,
                    marker=dict(colors=[colors.get(b, '#95a5a6') for b in behavior_stats['用水行为']]),
                    textinfo='percent+label'
                )])
                fig.update_layout(
                    height=300,
                    margin=dict(l=10, r=10, t=10, b=10)
                )
                st.plotly_chart(fig, width="stretch")

        st.markdown("<div class='card-title'>增强图（中文）</div>", unsafe_allow_html=True)
        fig_cn2 = create_enhanced_figure_cn(day_df)
        if fig_cn2:
            st.pyplot(fig_cn2, clear_figure=True)

        st.markdown("<div class='card-title'>原始数据预览</div>", unsafe_allow_html=True)
        with st.expander("展开查看原始数据"):
            st.dataframe(day_df.tail(100), width="stretch", height=300)
        
        # 导出分析结果
        st.markdown("<div class='card-title'>导出分析结果</div>", unsafe_allow_html=True)
        if not iv.empty:
            col1, col2, col3 = st.columns(3)
            with col1:
                csv_bytes = day_df.to_csv(index=False).encode('utf-8-sig')
                st.download_button("导出原始数据", data=csv_bytes, 
                                  file_name=f"原始数据_{sel.strftime('%Y%m%d')}.csv", 
                                  mime="text/csv", 
                                  key="export_raw")
            with col2:
                csv_intervals = iv.to_csv(index=False).encode('utf-8-sig')
                st.download_button("导出区间数据", data=csv_intervals, 
                                  file_name=f"区间数据_{sel.strftime('%Y%m%d')}.csv", 
                                  mime="text/csv",
                                  key="export_intervals")
            with col3:
                if fig_cn2:
                    from io import BytesIO
                    buf = BytesIO()
                    fig_cn2.savefig(buf, format="png", dpi=150)
                    buf.seek(0)
                    st.download_button("导出增强图", data=buf, 
                                      file_name=f"增强图_{sel.strftime('%Y%m%d')}.png", 
                                      mime="image/png",
                                      key="export_plot")
        
        st.markdown("</div>", unsafe_allow_html=True)

# -------- 数据管理 --------

def render_data_admin():
    st.markdown("<h2>数据管理</h2>", unsafe_allow_html=True)
    st.markdown("<div class='section-container'>", unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("<div class='card-title'>导出数据</div>", unsafe_allow_html=True)
        src_options = ["water_meter_data.csv"]
        if os.path.exists("device_push_data.csv"):
            src_options.append("device_push_data.csv")
        
        src = st.selectbox("选择导出源", src_options, index=0, key="exp_src")
        df = load_csv_safely(src)
        
        if not df.empty:
            # 创建数据统计摘要
            if '上报时间' in df.columns:
                df['上报时间'] = pd.to_datetime(df['上报时间'], errors='coerce')
                earliest = df['上报时间'].min().strftime('%Y-%m-%d') if not pd.isna(df['上报时间'].min()) else "未知"
                latest = df['上报时间'].max().strftime('%Y-%m-%d') if not pd.isna(df['上报时间'].max()) else "未知"
                time_range = f"{earliest} 至 {latest}"
            else:
                time_range = "未知"
                
            st.markdown(f"""
            <div style="background-color: #f8f9fa; padding: 15px; border-radius: 8px; margin: 10px 0;">
                <p><strong>数据文件:</strong> {src}</p>
                <p><strong>记录数量:</strong> {len(df)} 条</p>
                <p><strong>日期范围:</strong> {time_range}</p>
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown("##### 导出选项")
            
            export_all = st.checkbox("导出全部数据", value=True, key="export_all")
            
            if not export_all and '上报时间' in df.columns:
                # 如果不是导出全部，则显示日期选择器
                col_a, col_b = st.columns(2)
                with col_a:
                    dates = sorted(df['上报时间'].dt.date.unique())
                    if len(dates) > 0:
                        min_date = min(dates)
                        max_date = max(dates)
                        start_d = st.date_input("开始日期", min_date, key="exp_start")
                    else:
                        start_d = st.date_input("开始日期", datetime.now().date(), key="exp_start")
                with col_b:
                    if len(dates) > 0:
                        end_d = st.date_input("结束日期", max_date, key="exp_end")
                    else:
                        end_d = st.date_input("结束日期", datetime.now().date(), key="exp_end")
                
                # 过滤数据
                df = df[(df['上报时间'].dt.date >= start_d) & (df['上报时间'].dt.date <= end_d)]
                
            # 格式选择
            format_options = st.radio("导出格式", ["CSV", "Excel"], horizontal=True)
            
            if len(df) > 0:
                if format_options == "CSV":
                    csv_bytes = df.to_csv(index=False).encode('utf-8-sig')
                    filename = f"{os.path.splitext(src)[0]}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
                    st.download_button("下载CSV文件", csv_bytes, file_name=filename, mime="text/csv")
                else:
                    # Excel导出
                    import io
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        df.to_excel(writer, index=False, sheet_name='数据')
                    output.seek(0)
                    filename = f"{os.path.splitext(src)[0]}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
                    st.download_button("下载Excel文件", output, file_name=filename, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        else:
            st.info("暂无可导出数据")

        with col2:
            st.markdown("<div class='card-title'>删除数据</div>", unsafe_allow_html=True)
            st.warning("⚠️ 此操作将永久删除指定日期范围内的数据，无法撤销！", icon="⚠️")
            
            src2 = st.selectbox("选择删除源", src_options, index=0, key="del_src")
            
            df2 = load_csv_safely(src2)
            if df2.empty or '上报时间' not in df2.columns:
                st.warning("无数据或缺少上报时间")
            else:
                df2['上报时间'] = pd.to_datetime(df2['上报时间'], errors='coerce')
                dates = sorted(df2['上报时间'].dt.date.unique())
                
                if len(dates) > 0:
                    col_a, col_b = st.columns(2)
                    with col_a:
                        min_date = min(dates)
                        start_d = st.date_input("开始日期", min_date, key="del_start")
                    with col_b:
                        max_date = max(dates)
                        end_d = st.date_input("结束日期", max_date, key="del_end")
                    
                    if start_d > end_d:
                        st.error("开始日期不能晚于结束日期")
                    else:
                        # 计算将删除的记录数
                        to_delete = df2[(df2['上报时间'].dt.date >= start_d) & (df2['上报时间'].dt.date <= end_d)]
                        
                        st.markdown(f"""
                        <div style="background-color: #fef2f2; padding: 15px; border-radius: 8px; margin: 10px 0;">
                            <p><strong>将删除 {len(to_delete)} 条记录</strong></p>
                            <p>日期范围: {start_d.strftime('%Y-%m-%d')} 至 {end_d.strftime('%Y-%m-%d')}</p>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        confirm = st.checkbox("我已了解此操作不可撤销", key="confirm_delete")
                        if confirm:
                            if st.button("执行删除", key="btn_del", width="stretch"):
                                mask = (df2['上报时间'].dt.date < start_d) | (df2['上报时间'].dt.date > end_d)
                                kept = df2[mask]
                                kept.to_csv(src2, index=False)
                                st.success(f"删除完成，保留 {len(kept)} 条记录")
    
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("<div class='card-title'>API / 网络信息</div>", unsafe_allow_html=True)
    
    # 优雅处理API不可用的情况
    if not API_AVAILABLE:
        st.warning("API服务器未运行，无法获取网络信息。", icon="⚠️")
        
        # 本地网络信息
        try:
            import socket
            hostname = socket.gethostname()
            local_ip = socket.gethostbyname(hostname)
            
            st.markdown(f"""
            <div style="background-color: #f8f9fa; padding: 15px; border-radius: 8px; margin: 10px 0;">
                <p><strong>主机名:</strong> {hostname}</p>
                <p><strong>本地IP:</strong> {local_ip}</p>
                <p><strong>API端口:</strong> {API_PORT} (未运行)</p>
            </div>
            """, unsafe_allow_html=True)
        except:
            st.error("无法获取网络信息")
    else:
        # API正常，获取信息
        info = api_get("/public_info")
        
        if info:
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("##### 服务状态")
                
                db_status = "已连接" if info.get("db_enabled") else "未配置"
                db_color = "#2ecc71" if info.get("db_enabled") else "#e74c3c"
                
                st.markdown(f"""
                <div style="background-color: #f8f9fa; padding: 15px; border-radius: 8px; margin: 10px 0;">
                    <p><strong>数据库状态:</strong> <span style="color: {db_color};">{db_status}</span></p>
                    <p><strong>API端口:</strong> {info.get('external_port', '8000')}</p>
                </div>
                """, unsafe_allow_html=True)
                
            with col2:
                st.markdown("##### 数据接收地址")
                
                lan_ip = info.get("lan_ip_suggest")
                public_ip = info.get("public_ip")
                
                if info.get("lan_ips"):
                    st.markdown("##### 局域网接收地址")
                    for ip in info.get("lan_ips", []):
                        st.code(f"http://{ip}:{info.get('external_port', '8000')}/api/data", language="text")
                
                if public_ip:
                    st.markdown("##### 公网接收地址")
                    st.code(f"http://{public_ip}:{info.get('external_port', '8000')}/api/data", language="text")
    
    st.markdown("</div>", unsafe_allow_html=True)

# -------- 设备管理 --------

def render_device_mgmt():
    st.markdown("<h2>设备管理</h2>", unsafe_allow_html=True)
    st.markdown("<div class='section-container'>", unsafe_allow_html=True)
    
    if not API_AVAILABLE:
        st.warning("API服务器未运行，设备管理功能不可用。请启动API服务器。", icon="⚠️")
        
        st.markdown("""
        <div style="background-color: #f8f9fa; padding: 20px; border-radius: 8px; margin: 20px 0;">
            <h4>如何启动API服务器</h4>
            <p>请在命令行中运行以下命令：</p>
            <pre style="background-color: #eee; padding: 10px; border-radius: 5px;">
python run_app.py --api-port 8000
# 或仅启动API
python api_server_local.py
            </pre>
            <p>启动后，刷新此页面即可使用设备管理功能</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("</div>", unsafe_allow_html=True)
        return
    
    info = api_get("/public_info")
    # 本地存储版不再依赖数据库，若检测到 db_enabled=False，提示为“本地存储已启用”，不阻断设备管理
    if info and info.get("storage_type") == "local_file":
        st.info("使用本地文件存储，数据库配置已禁用（无需 NEON_URL/DATABASE_URL）")

    # 两个标签页：设备列表和设备详情
    tabs = st.tabs(["设备列表", "设备管理", "批量导入"])
    
    # 标签页 1: 设备列表
    with tabs[0]:
        st.markdown("<div class='card-title'>设备列表与搜索</div>", unsafe_allow_html=True)
        
        # 搜索和筛选
        col1, col2, col3 = st.columns([2, 1, 1])
        with col1:
            search = st.text_input("搜索设备 (表号/IMEI/别名)", key="device_search", placeholder="输入关键字搜索")
        with col2:
            status_filter = st.selectbox(
                "设备状态", 
                [("所有设备", "all"), ("激活设备", "active"), ("未激活设备", "inactive")],
                format_func=lambda x: x[0],
                key="status_filter"
            )
        with col3:
            st.markdown("<div style='padding-top: 32px;'></div>", unsafe_allow_html=True)
            refresh = st.button("刷新数据", width="stretch")
        
        # 调用API获取设备列表
        status_value = status_filter[1] if status_filter else None
        api_path = f"/api/devices?search={search}" if search else "/api/devices"
        if status_value and status_value != "all":
            api_path += f"&status={status_value}"
            
        resp = api_get(api_path)
        
        if resp and resp.get("data"):
            # 转换成DataFrame便于显示和操作
            devices_df = pd.DataFrame(resp["data"])
            
            # 设置选中设备的ID（用于详情查看）
            if 'selected_device_id' not in st.session_state:
                st.session_state.selected_device_id = None
                
            # 显示设备表格
            st.dataframe(
                devices_df,
                column_config={
                    "deviceNo": st.column_config.TextColumn("设备编号"),
                    "imei": st.column_config.TextColumn("IMEI号"),
                    "alias": st.column_config.TextColumn("设备别名"),
                    "location": st.column_config.TextColumn("安装位置"),
                    "is_active": st.column_config.CheckboxColumn("状态"),
                    "created_at": st.column_config.DatetimeColumn("注册时间", format="YYYY-MM-DD HH:mm"),
                    "data_count": st.column_config.NumberColumn("数据点数"),
                    "last_data": st.column_config.DatetimeColumn("最后数据时间", format="YYYY-MM-DD HH:mm")
                },
                width="stretch",
                hide_index=True,
                selection="single",
                height=400,
                key="devices_table"
            )
            
            # 设备统计
            st.markdown(f"""
            <div style="margin-top: 15px;">
                <span style="background-color: #e3f2fd; padding: 5px 10px; border-radius: 12px; font-size: 0.9rem;">
                    总设备数: {len(devices_df)}
                </span>
                <span style="background-color: #e8f5e9; padding: 5px 10px; border-radius: 12px; font-size: 0.9rem; margin-left: 10px;">
                    在线设备: {devices_df['is_active'].sum() if 'is_active' in devices_df else 0}
                </span>
            </div>
            """, unsafe_allow_html=True)
            
            # 检查是否有设备被选中
            if st.session_state.devices_table:
                selected_idx = st.session_state.devices_table['row_index'][0]
                selected_device = devices_df.iloc[selected_idx]
                st.session_state.selected_device_id = selected_device['deviceNo']
                
                # 显示设备详情
                with st.expander("设备详情", expanded=True):
                    detail_col1, detail_col2 = st.columns([1, 1])
                    with detail_col1:
                        st.markdown(f"**设备编号**: {selected_device['deviceNo']}")
                        st.markdown(f"**IMEI号**: {selected_device['imei'] or '未设置'}")
                        st.markdown(f"**设备别名**: {selected_device['alias'] or '未设置'}")
                    with detail_col2:
                        st.markdown(f"**安装位置**: {selected_device['location'] or '未设置'}")
                        st.markdown(f"**状态**: {'激活' if selected_device['is_active'] else '未激活'}")
                        st.markdown(f"**注册时间**: {pd.to_datetime(selected_device['created_at']).strftime('%Y-%m-%d %H:%M:%S')}")
                    
                    # 获取设备统计数据
                    stats = api_get(f"/api/devices/{selected_device['deviceNo']}/stats")
                    if stats:
                        st.markdown("### 数据统计")
                        stat_col1, stat_col2, stat_col3 = st.columns(3)
                        with stat_col1:
                            st.metric("数据点数量", stats.get("dataCount", 0))
                        with stat_col2:
                            start = pd.to_datetime(stats.get("firstDataTime")).strftime('%Y-%m-%d') if stats.get("firstDataTime") else "无数据"
                            end = pd.to_datetime(stats.get("lastDataTime")).strftime('%Y-%m-%d') if stats.get("lastDataTime") else "无数据"
                            st.metric("数据时间范围", f"{start} 至 {end}")
                        with stat_col3:
                            avg = stats.get("avgFlow")
                            st.metric("平均瞬时流量", f"{avg:.4f} m³/h" if avg else "无数据")
                        
                        # 操作按钮
                        action_col1, action_col2 = st.columns(2)
                        with action_col1:
                            if st.button("查看设备数据", key="view_device_data", width="stretch"):
                                # 可以跳转到历史查询页面并预填设备号
                                st.session_state.nav = "历史查询"
                                st.session_state.device_filter = selected_device['deviceNo']
                                st.rerun()
                        with action_col2:
                            status_action = "停用设备" if selected_device['is_active'] else "激活设备"
                            if st.button(status_action, key="toggle_status", width="stretch"): 
                                # 更新设备状态
                                ok = api_post(f"/api/devices/{selected_device['deviceNo']}", {
                                    "deviceNo": selected_device['deviceNo'],
                                    "is_active": not selected_device['is_active']
                                })
                                if ok:
                                    st.success(f"设备已{'停用' if selected_device['is_active'] else '激活'}")
                                    # 刷新页面
                                    st.rerun()
        else:
            st.info("暂无设备或无法连接接口")
    
    # 标签页 2: 设备管理（新增/更新）
    with tabs[1]:
        st.markdown("<div class='card-title'>新增/更新设备</div>", unsafe_allow_html=True)
        
        with st.form("device_form"):
            device_no = st.text_input("表号", key="dev_no", placeholder="必填，如：70666000038000")
            imei = st.text_input("IMEI", key="dev_imei", placeholder="选填，如：860329065551923")
            alias = st.text_input("别名", key="dev_alias", placeholder="选填，如：客厅水表")
            location = st.text_input("位置", key="dev_loc", placeholder="选填，如：一楼客厅西北角")
            is_active = st.checkbox("启用", value=True, key="dev_active")
            
            col1, col2 = st.columns(2)
            with col1:
                submitted = st.form_submit_button("保存设备", width="stretch")
            with col2:
                reset = st.form_submit_button("重置表单", width="stretch", type="secondary")
            
            if submitted:
                if not device_no:
                    st.warning("请输入表号")
                else:
                    body = {
                        "deviceNo": device_no, 
                        "imei": imei or None, 
                        "alias": alias or None, 
                        "location": location or None, 
                        "is_active": is_active
                    }
                    ok = api_post("/api/devices", body)
                    if ok:
                        st.success("设备信息已保存")
                        st.session_state.dev_no = ""
                        st.session_state.dev_imei = ""
                        st.session_state.dev_alias = ""
                        st.session_state.dev_loc = ""
                        st.session_state.dev_active = True
            
            if reset:
                st.session_state.dev_no = ""
                st.session_state.dev_imei = ""
                st.session_state.dev_alias = ""
                st.session_state.dev_loc = ""
                st.session_state.dev_active = True

    # 标签页 3: 批量导入
    with tabs[2]:
        st.markdown("<div class='card-title'>批量导入设备</div>", unsafe_allow_html=True)
        
        st.markdown("""
        支持通过CSV或Excel文件批量导入设备信息。文件必须包含以下列：
        - `表号` 或 `deviceNo`（必填）
        - `IMEI号` 或 `imei`（可选）
        - `别名` 或 `alias`（可选）
        - `位置` 或 `location`（可选）
        - `启用` 或 `is_active`（可选，默认为启用）
        """)
        
        uploaded_file = st.file_uploader("选择CSV或Excel文件", type=['csv', 'xlsx', 'xls'], key="bulk_import_file")
        
        if uploaded_file is not None:
            try:
                # 读取文件
                if uploaded_file.name.lower().endswith('.csv'):
                    df = pd.read_csv(uploaded_file)
                else:
                    df = pd.read_excel(uploaded_file)
                
                # 标准化列名
                col_map = {
                    '表号': 'deviceNo',
                    'IMEI号': 'imei', 
                    'imei号': 'imei',
                    '别名': 'alias',
                    '位置': 'location',
                    '启用': 'is_active'
                }
                df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
                
                # 验证必填字段
                if 'deviceNo' not in df.columns:
                    st.error("文件缺少必填的'表号'或'deviceNo'列")
                    st.markdown("</div>", unsafe_allow_html=True)
                    return
                
                # 预览数据
                st.markdown("### 数据预览")
                st.dataframe(df.head(10), width="stretch")
                
                # 确认导入
                if st.button("确认导入", key="confirm_bulk_import"):
                    # 处理列类型
                    if 'is_active' in df.columns:
                        df['is_active'] = df['is_active'].map({'是': True, '否': False, 1: True, 0: False, True: True, False: False}).fillna(True)
                    
                    # 准备请求数据
                    devices = []
                    for _, row in df.iterrows():
                        device = {"deviceNo": str(row['deviceNo'])}
                        if 'imei' in df.columns and not pd.isna(row['imei']):
                            device["imei"] = str(row['imei'])
                        if 'alias' in df.columns and not pd.isna(row['alias']):
                            device["alias"] = str(row['alias'])
                        if 'location' in df.columns and not pd.isna(row['location']):
                            device["location"] = str(row['location'])
                        if 'is_active' in df.columns:
                            device["is_active"] = bool(row['is_active'])
                        devices.append(device)
                    
                    # 发送请求
                    resp = api_post("/api/devices/bulk", {"devices": devices})
                    if resp and resp.get("count"):
                        st.success(f"成功导入 {resp['count']} 个设备")
                    else:
                        st.error("导入失败")
            
            except Exception as e:
                st.error(f"处理文件时出错: {e}")
        
        # 提供下载模板功能
        st.markdown("### 下载导入模板")
        col1, col2 = st.columns(2)
        with col1:
            csv_template = pd.DataFrame({
                '表号': ['70666000038001', '70666000038002'],
                'IMEI号': ['860329065551924', '860329065551925'],
                '别名': ['客厅水表', '厨房水表'],
                '位置': ['一楼客厅', '一楼厨房'],
                '启用': [True, True]
            })
            csv_bytes = csv_template.to_csv(index=False).encode('utf-8-sig')
            st.download_button("下载CSV模板", csv_bytes, file_name="设备导入模板.csv", mime="text/csv")
        
        with col2:
            # 提供Excel模板
            try:
                from io import BytesIO
                import openpyxl
                
                output = BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    csv_template.to_excel(writer, index=False, sheet_name='设备信息')
                output.seek(0)
                
                st.download_button("下载Excel模板", output, file_name="设备导入模板.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            except Exception:
                st.warning("无法生成Excel模板，请使用CSV模板")

    # 数据接收示例代码
    st.markdown("<div class='card-title'>数据接收示例</div>", unsafe_allow_html=True)
    with st.expander("查看 Python 数据推送示例代码"):
        st.code("""
import requests
from datetime import datetime

# 接收地址，根据实际部署环境替换IP和端口
URL = "http://YOUR_SERVER_IP:8000/api/data"

# 示例数据
payload = {
    "batteryVoltage": "3.626",
    "deviceNo": "70666000038000",  # 替换为您的设备编号
    "freezeDateFlow": "117.42",
    "imei": "860329065551923",     # 替换为您的IMEI号
    "instantaneousFlow": "0.0033",  # 单位：m³/h
    "pressure": "0.00",
    "reverseFlow": "0.00",
    "signalValue": "-85",           # 单位：dBm
    "startFrequency": "21160",
    "temprature": "22.48",          # 单位：°C
    "totalFlow": "117.4214",        # 单位：m³
    "valveStatu": "开",
    # 时间格式：YYYY-MM-DD HH:MM:SS
    "updateTime": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
}

# 发送请求
resp = requests.post(URL, json=payload, timeout=5)
print(resp.status_code, resp.text)
        """, language="python")
    
    st.markdown("</div>", unsafe_allow_html=True)

# -------- 主体 --------

st.markdown("## 用户用水行为识别系统")

if not st.session_state.auth:
    render_login()
else:
    # 添加侧边栏菜单
    st.sidebar.markdown("""
    <div style="text-align: center; margin-bottom: 20px;">
        <h3 style="color: white;">💧 用户用水行为识别系统</h3>
    </div>
    """, unsafe_allow_html=True)
    
    # 用户信息
    st.sidebar.success(f"已登录：{st.session_state.username}")
    
    # 侧边栏菜单
    with st.sidebar:
        selected = st.radio(
            "导航菜单",
            ["实时监测", "历史查询", "上传分析", "数据管理", "设备管理"],
            key="nav",
            format_func=lambda x: {
                "实时监测": "📊 实时监测",
                "历史查询": "📅 历史查询",
                "上传分析": "📤 上传分析",
                "数据管理": "💾 数据管理",
                "设备管理": "🔧 设备管理"
            }.get(x, x)
        )
        
        st.markdown("---")
        
        # 数据库状态
        info = api_get("/public_info")
        if info:
            storage = info.get("storage_type", "local_file")
            storage_text = "本地文件存储" if storage == "local_file" else "数据库"
            st.markdown(f"""
            <div style="font-size: 0.8rem; margin-top: 20px; color: #DDD;">
                <p>存储方式: {storage_text}</p>
                <p>API端口: {info.get('external_port', '8000')}</p>
            </div>
            """, unsafe_allow_html=True)
        
        # 注销按钮
        if st.button("注销", key="logout_btn"):
            st.session_state.auth = False
            st.session_state.role = None
            st.session_state.username = None
            if "token" in st.query_params:
                del st.query_params["token"]
            st.rerun()
     
    # 主内容区域
    if selected == "实时监测":
        render_realtime()
    elif selected == "历史查询":
        render_history()
    elif selected == "上传分析":
        render_upload_analysis()
    elif selected == "数据管理":
        render_data_admin()
    elif selected == "设备管理":
        render_device_mgmt() 
