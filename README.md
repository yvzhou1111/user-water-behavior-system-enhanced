# 用户用水行为识别系统

![系统徽标](https://img.icons8.com/color/96/000000/water.png)

一个基于水表数据分析的用水行为识别系统，帮助用户了解用水模式，发现潜在的漏水问题。

## 系统特点

- **多源数据接入**：支持多种格式的数据源，包括CSV、Excel以及实时设备推送
- **实时监测**：展示水表的实时数据，包括累计流量、瞬时流量、温度和电池电压
- **历史查询**：按日期查询历史用水数据，展示用水趋势和行为分类
- **异常检测**：识别大流量用水、夜间用水和疑似漏水等异常情况
- **数据管理**：支持数据导出和删除操作
- **设备管理**：管理水表设备信息，包括添加、更新设备
- **多级权限**：管理员登录功能，保护关键操作
- **数据库支持**：使用Neon PostgreSQL数据库存储数据，确保数据安全和高效查询
- **现代化界面**：优雅的可视化界面，提供丰富的数据分析能力

## 快速开始

### 环境需求

- Python 3.8+
- PostgreSQL数据库（推荐使用Neon云数据库）
- 网络环境：支持内网或公网部署

### 安装步骤

1. 克隆本仓库到本地

```bash
git clone <仓库URL>
cd 用户用水行为系统修改版
```

2. 安装依赖

```bash
pip install -r requirements.txt
```

3. 配置环境

运行配置向导：

```bash
python setup_env.py
```

按照提示完成系统配置，包括数据库连接、管理员账户和网络设置。

### 启动服务

启动完整服务（API后端 + Streamlit前端）：

```bash
python run.py
```

仅启动API服务：

```bash
python run.py --api-only
```

仅启动前端：

```bash
python run.py --streamlit-only
```

## 访问系统

- 后端API: `http://<您的IP>:8000`
- 前端界面: `http://<您的IP>:8501`

## 数据推送方式

外部设备可以通过HTTP POST请求向系统推送数据：

```python
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
```

## 系统模块

### 1. 数据接收与标准化

系统支持通过API接收水表设备推送的数据，数据格式需符合《水表数据推送接口文档》中的规范。接收到的数据会进行标准化处理，统一字段名称和数据类型，确保后续分析的准确性。

### 2. 用水行为分析

系统基于瞬时流量和用水时长对用水行为进行分类：

- **冲洗用水**：大流量用水事件，如冲厕所、洗澡等
- **桶箱用水**：中等流量用水，如洗衣服、洗碗等
- **零星用水**：小流量用水，如洗手、刷牙等

分析算法通过对流量数据进行分段和特征提取，识别关键点并进行分类。

### 3. 异常检测

系统提供多种异常检测功能：

- **大流量用水检测**：识别超过阈值的大流量用水事件
- **夜间用水检测**：识别夜间时段(23:00-6:00)的用水行为
- **漏水检测**：识别长时间持续的小流量用水，可能表示漏水

### 4. 可视化展示

系统提供多种可视化图表：

- **流量趋势图**：展示瞬时流量和累计流量的变化趋势
- **用水行为饼图**：展示不同用水行为的占比
- **增强图**：三联图展示流量、温度和电池/信号强度的变化，并标记关键用水点

## 外部访问配置

系统支持多种外部访问方式：

1. **局域网访问**：
   - 后端API：http://{局域网IP}:8000
   - 前端界面：http://{局域网IP}:8501

2. **公网访问**：
   - 配置路由器端口转发(8000和8501端口)
   - 如遇CGNAT问题，可使用Cloudflare Tunnel等工具

## 许可证

本系统基于MIT许可证开源。

## 贡献者

- 原始作者：[原作者]
- 优化修改：[您的名字]

## 更新日志

- 2023-11-20: 添加了现代化UI和更好的可视化
- 2023-11-10: 添加了Neon PostgreSQL数据库支持
- 2023-11-01: 初始版本发布 














@yvzhou1111 ➜ /workspaces/----------- (main) $ python run_app.py

┌─────────────────────────────────────────────────────┐
│                用户用水行为识别系统               │
│                   全功能版本                   │
└─────────────────────────────────────────────────────┘
    
[网络] 本机IP地址:
  - 10.0.6.167
  - 127.0.0.1
启动API服务器...
[API] 2025-09-10 13:06:21,342 - water-meter-api - INFO - 启动API服务器: 0.0.0.0:8000
[API] INFO:     Started server process [3265]
[API] INFO:     Waiting for application startup.
[API] INFO:     Application startup complete.
[API] INFO:     Uvicorn running on http://0.0.0.0:8000 (Press CTRL+C to quit)
[API] API服务器已启动: http://10.0.6.167:8000
[API] 数据推送地址: http://10.0.6.167:8000/api/data
启动Streamlit前端...
[Streamlit] 前端已启动: http://10.0.6.167:8501
[数据] 开始推送历史数据...
[API] INFO:     127.0.0.1:59146 - "GET /api/devices/70666000038000 HTTP/1.1" 200 OK