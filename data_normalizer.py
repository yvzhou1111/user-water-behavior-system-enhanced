# -*- coding: utf-8 -*-
import pandas as pd
from datetime import datetime

# 统一列名映射（尽量覆盖两个样本文件中的中文列名）
COLUMN_MAPS = [
    {
        '表号': '表号',
        'IMEI号': 'imei号',
        'imei号': 'imei号',
        '累计流量': '累计流量',
        '瞬时流量': '瞬时流量',
        '反向流量': '反向流量',
        '启动次数': '启动次数',
        '温度': '温度',
        '压力': '压力',
        '电池电压': '电池电压',
        '信号值': '信号值',
        '阀门状态': '阀门状态',
        '上报时间': '上报时间'
    },
    {
        'device_no': 'imei号',
        '电池电压': '电池电压',
        '表号': '表号',
        '冻结流量': '冻结流量',
        'imei号': 'imei号',
        '瞬时流量': '瞬时流量',
        '压力': '压力',
        '反向流量': '反向流量',
        '信号值': '信号值',
        '启动次数': '启动次数',
        '温度': '温度',
        '累计流量': '累计流量',
        '上传时间': '上报时间',
        '日期计算': '日期计算',
        '时间计算': '时间计算'
    }
]

REQUIRED_COLS = ['表号','imei号','累计流量','瞬时流量','温度','电池电压','信号值','上报时间']


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=REQUIRED_COLS + ['日期计算','时间计算','数据L/s'])

    # 合并重命名：遍历所有映射把能映射到的列都重命名
    rename_all = {}
    for cmap in COLUMN_MAPS:
        for k, v in cmap.items():
            if k in df.columns:
                rename_all[k] = v
    renamed = df.rename(columns=rename_all)

    # 合并同名重复列（如 device_no 与 imei号 -> imei号）：非空优先，保留首列
    duplicated_targets = [col for col in renamed.columns if (renamed.columns == col).sum() > 1]
    for col in pd.unique(duplicated_targets):
        cols = renamed.loc[:, renamed.columns == col]
        # 逐列向前填充以合并非空值
        merged = cols.bfill(axis=1).iloc[:, 0]
        renamed[col] = merged
        # 去重，仅保留第一列
        first_col_mask = ~renamed.columns.duplicated(keep='first')
        renamed = renamed.loc[:, first_col_mask]

    # 最终只保留需要的列（存在则保留）
    out = pd.DataFrame()
    for col in ['表号','imei号','累计流量','瞬时流量','温度','电池电压','信号值','反向流量','压力','启动次数','阀门状态','上报时间','日期计算','时间计算']:
        if col in renamed.columns:
            out[col] = renamed[col]

    # 如果没有“上报时间”但有“上传时间”（极端兜底）
    if '上报时间' not in out.columns and '上传时间' in renamed.columns:
        out['上报时间'] = renamed['上传时间']

    # 解析时间
    if '上报时间' in out.columns:
        out['上报时间'] = pd.to_datetime(out['上报时间'], errors='coerce')
        out['日期计算'] = out['上报时间'].dt.date.astype('datetime64[ns]')
        out['时间计算'] = out['上报时间'].dt.time

    # 数据L/s：若瞬时流量单位为 m³/h，则 L/s = m³/h / 3.6
    if '瞬时流量' in out.columns:
        out['数据L/s'] = pd.to_numeric(out['瞬时流量'], errors='coerce') / 3.6
    else:
        out['数据L/s'] = 0

    # 填充缺失列
    for col in REQUIRED_COLS:
        if col not in out.columns:
            out[col] = None

    return out

# ---------------- 新增：鲁棒文件读取 ----------------
def _read_any(path: str) -> pd.DataFrame:
    p = str(path)
    lower = p.lower()
    if lower.endswith('.xlsx') or lower.endswith('.xls'):
        return pd.read_excel(p)
    # CSV: 尝试UTF-8, 回退GBK
    try:
        return pd.read_csv(p)
    except Exception:
        return pd.read_csv(p, encoding='gbk')


def load_and_normalize(path) -> pd.DataFrame:
    """
    兼容 path 为字符串或被误写成数字的情况；支持Excel/CSV与中文编码
    """
    try:
        path_str = str(path)
        df = _read_any(path_str)
    except Exception:
        return pd.DataFrame(columns=REQUIRED_COLS + ['日期计算','时间计算','数据L/s'])
    return normalize_dataframe(df) 