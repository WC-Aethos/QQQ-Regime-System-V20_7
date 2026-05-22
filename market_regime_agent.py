"""
market_regime_agent.py
Google Sheet Regime System V20.7

V20.4.2 builds on V20.4.1 and fixes bootstrap backfill: when Daily_Tracker has fewer than 4 dated market rows, it fetches the prior 3 clean market days plus one older raw-price baseline so 1D/3D logic is not empty on first install. NDX50R_Computed is scored, while Nasdaq 100 A/D Line is logged as a warning-only divergence signal.

V20.7 keeps the V20.6 hardened data layer and reorganizes Decision_Output into grouped dashboard categories: Action Summary, Macro Liquidity, AI Leadership, Market Breadth, Risk Pressure, Breakout Confirmation, Trend / 1Y Entry, and Data Quality.

V16 fixes the core V4.3/V15 failure modes:
- Data_Status != OK is recorded but never scored and never enters 3D/composite logic.
- Reason_Log uses strict required-value parsing; missing scoring fields raise errors instead of becoming 0.
- Macro pressure is explicit: US10Y, DXY, UKOIL, and VIX are scored together.
- SOXX leadership is explicit: Semi_Leadership = SOXX_% - QQQ_%.
- 3D confirmation uses the most recent three clean valid OK days, not calendar days or last three rows.
- Decision_Output separates Main Negative Signals and Main Positive Signals and shows confidence.

V17 added:
- Long_Term_Entry_1Y sheet: Trend, Drawdown, Macro, Leadership 5D, Breadth 5D, Credit and Total_1Y_Entry_Score.

V19 kept AI modules and added:
- AI ROI / Depreciation Risk: narrative is logged; only confirmed/manual fundamental risk affects score.
- AI Chain Stress: SOXX / NVDA / TSM relative weakness vs QQQ, with 3-clean-day confirmation.
- Macro Valuation Pressure: US10Y / DXY / VIX pressure on high-duration growth valuation.
- Gamma Volatility Warning: short-term volatility amplifier, capped at -1.
- Energy / AI Infrastructure Rotation: secondary rotation signal, capped at +/-1.

Core rule: Narrative is recorded. Price confirms. Fundamentals validate.

Required GitHub Secrets:
- GOOGLE_SERVICE_ACCOUNT_JSON
- GOOGLE_SHEET_ID
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd

VERSION = "V20.7"
SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")
TIMEZONE = os.getenv("TIMEZONE", "Asia/Taipei")
TZ = ZoneInfo(TIMEZONE)

TICKERS = {
    "QQQ": "QQQ",
    "SOXX": "SOXX",
    "NVDA": "NVDA",
    "MSFT": "MSFT",
    "TSM": "TSM",
    "VIX": "^VIX",
    "US10Y": "^TNX",       # Yahoo ^TNX = 10Y yield * 10; script divides by 10.
    "US30Y": "^TYX",       # Yahoo ^TYX = 30Y yield * 10; script divides by 10.
    "DXY": "DX-Y.NYB",
    "UKOIL": "BZ=F",       # Brent futures proxy.
    "QQQE": "QQQE",
    "RSP": "RSP",
    "SPY": "SPY",
    "IWM": "IWM",
    "HYG": "HYG",
    "TLT": "TLT",
    "XLE": "XLE",       # Energy ETF proxy.
    "XLU": "XLU",       # Utilities / power infrastructure proxy.
}

# V20.2: do not fetch external NDX50R / A/D symbols. Compute Nasdaq 100 breadth directly.
# Set NDX_TICKERS="AAPL,MSFT,NVDA,..." in GitHub Secrets if you want to override the dynamic/fallback universe.
NDX_STATIC_TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "AVGO", "META", "GOOGL", "GOOG", "TSLA", "COST",
    "NFLX", "AMD", "PEP", "ADBE", "CSCO", "TMUS", "INTU", "QCOM", "TXN", "AMAT",
    "AMGN", "ISRG", "BKNG", "HON", "CMCSA", "PANW", "ADP", "VRTX", "SBUX", "GILD",
    "MU", "ADI", "LRCX", "MELI", "MDLZ", "REGN", "KLAC", "SNPS", "CDNS", "CRWD",
    "MAR", "ORLY", "CSX", "CEG", "PYPL", "ASML", "ABNB", "FTNT", "MRVL", "NXPI",
    "ROP", "WDAY", "ADSK", "PCAR", "CPRT", "MNST", "AEP", "CHTR", "PAYX", "KDP",
    "ROST", "KHC", "ODFL", "AZN", "EXC", "FAST", "FANG", "IDXX", "DDOG", "EA",
    "CTAS", "BKR", "GEHC", "LULU", "VRSK", "XEL", "CTSH", "CCEP", "TEAM", "BIIB",
    "ON", "ZS", "CDW", "DXCM", "TTD", "ANSS", "CSGP", "MCHP", "GFS", "MDB",
    "ILMN", "WBD", "SIRI", "WBA", "DLTR", "MRNA", "LCID", "PDD", "LIN", "ARM"
]

DAILY_HEADERS = [
    "Date", "QQQ_Close", "QQQ_50MA", "QQQ_200MA", "SOXX_Close", "NVDA_Close", "MSFT_Close", "TSM_Close",
    "VIX", "US10Y", "US30Y", "DXY", "UKOIL", "QQQE", "RSP", "SPY", "IWM", "HYG", "TLT", "XLE", "XLU",
    "NDX50R_Computed", "NDX_Advancers", "NDX_Decliners", "NDX_AD_Daily", "NDX_AD_Line_Computed", "NDX_Breadth_Universe", "NDX_Breadth_Missing",
    "QQQ_Source_Date", "VIX_Source_Date", "US10Y_Source_Date", "US30Y_Source_Date", "DXY_Source_Date", "UKOIL_Source_Date", "TSM_Source_Date",
    "QQQ_Volume", "QQQ_Vol_20D_Avg",
    "QQQ_%", "SOXX_%", "NVDA_%", "MSFT_%", "TSM_%", "VIX_%", "US10Y_%", "US30Y_%", "DXY_%", "UKOIL_%",
    "QQQE_%", "RSP_%", "SPY_%", "IWM_%", "HYG_%", "TLT_%", "XLE_%", "XLU_%", "NDX_AD_Line_%",
    "QQQE_vs_QQQ_%", "RSP_vs_SPY_%", "IWM_vs_SPY_%", "HYG_vs_TLT_%", "XLE_vs_QQQ_%", "XLU_vs_QQQ_%",
    "Semi_Leadership_%", "EqualWeight_Breadth_%", "NDX50R_Breadth_Score", "Breadth_Health", "AD_Alert", "QQQ_Vol_Ratio",
    "AI_ROI_Risk_Manual_Score", "AI_ROI_Risk_Note",
    "Gamma_Risk_Manual_Score", "Gamma_Risk_Note",
    "Narrative_Risk_Note", "Data_Status", "Warnings",
]

QQQ_CACHE_SHEET_NAME = "QQQ_History_Cache"
QQQ_CACHE_HEADERS = ["Date", "Close", "Volume", "Source", "Fetch_Time", "Data_Status", "Warnings"]
QQQ_CACHE_MIN_BARS = 220
QQQ_CACHE_BOOTSTRAP_PERIOD = "300d"
QQQ_CACHE_INCREMENTAL_PERIOD = os.getenv("QQQ_CACHE_INCREMENTAL_PERIOD", "7d")
YFINANCE_RETRIES = int(os.getenv("YFINANCE_RETRIES", "3"))
YFINANCE_BACKOFF_SECONDS = [float(x.strip()) for x in os.getenv("YFINANCE_BACKOFF_SECONDS", "5,15,45").split(",") if x.strip()]


AGENT_HEADERS = [
    "Date", "Valid_For_Signal", "Liquidity_Score", "Rate_Score", "Rotation_Score", "Leadership_Score",
    "Breadth_Score", "NDX50R_Breadth_Score", "Breadth_Health", "AD_Alert", "NDX50R_Computed", "NDX_Advancers", "NDX_Decliners", "NDX_AD_Daily", "NDX_AD_Line_Computed", "NDX_Breadth_Universe", "Positioning_Score", "Credit_Risk_Score", "Macro_Pressure_Score", "Total_Dynamics_Score",
    "Macro_Valuation_Pressure", "Long_End_Yield_Shock", "Rate_Regime", "Fragile_Risk_On", "AI_Chain_Stress", "AI_ROI_Risk", "Gamma_Volatility_Warning", "Energy_AI_Rotation",
    "V20_Adjusted_Score", "AI_Chain_Weak_Day", "AI_Chain_Weak_Count_3D",
    "Semi_Leadership_%", "Strong_Day", "Clean_Valid_Count_3D", "3D_Avg_Score", "Strong_Count_3D",
    "3_of_3_Confirmed", "Regime", "Action", "Agent_Comment", "V20_State", "V20_Action", "V20_Reason_Log", "Data_Status", "Reason_Summary",
]

MULTI_HEADERS = [
    "Date", "Valid_For_Signal", "Score_1D", "V20_Adjusted_Score", "Score_3D", "Score_10D", "Score_20D",
    "Clean_Valid_Count_3D", "Strong_Count_3D", "Strong_Count_10D", "AI_Chain_Weak_Count_3D", "Transition_Probability",
    "Momentum_State", "Breadth_State", "Breadth_Health", "AD_Alert", "Liquidity_State", "Credit_State",
    "Macro_Valuation_Pressure", "Long_End_Yield_Shock", "Rate_Regime", "Fragile_Risk_On", "AI_Chain_Stress", "AI_ROI_Risk", "Gamma_Volatility_Warning", "Energy_AI_Rotation",
    "Regime_Composite", "Action_Composite", "V20_State", "V20_Action",
    "Main_Negative_Signals", "Main_Positive_Signals", "Narrative_Risk_Log", "Confidence", "Commentary", "Data_Status",
]

REASON_HEADERS = [
    "Date", "Category", "Signal", "Score_Impact", "Severity", "Reason", "Raw_Value", "Data_Status",
]

DASHBOARD_HEADERS = ["Category", "Metric", "Value", "Score Meaning / How to Read"]

DECISION_MEANINGS = {
    "System Version": "版本號；用來確認目前是 V20 邏輯。",
    "Latest Date": "最新一筆有效市場資料日期。",
    "Data Status": "OK 才可進入 score / 3D / composite；WARNING 或 BASELINE_ONLY 只記錄，不計分。",
    "Valid For Signal": "TRUE 代表可作為交易訊號；FALSE 代表資料不完整或不同步，不能交易。",
    "Composite Regime": "原始多週期市場狀態；不是最終 V20 動作。",
    "Composite Action": "原始 composite 建議；需再看 V20 Adjusted Score 與 V20 State。",
    "V20 State": "V20 最終狀態：RISK-ON / FRAGILE / WARNING / DEFENSIVE。",
    "V20 Action": "依 V20 score 與風險模組產生的最終行動建議。",
    "Transition Probability": "0–100%；越高代表轉強機率越高。<40 偏弱，40–60 中性，>60 偏強。",
    "1D Score": "單日分數；≥+3 偏多，-2~+2 中性，≤-3 偏空。單日只做警示，不單獨決定進出。",
    "QQQ 50MA": "QQQ 50 日均線，由 yfinance 抓取 QQQ 約 300 個交易日歷史收盤價後自動計算；用來判斷短中期趨勢與回踩品質。",
    "QQQ 200MA": "QQQ 200 日均線，由 yfinance 抓取 QQQ 約 300 個交易日歷史收盤價後自動計算；用來判斷長期多空結構。",
    "QQQ Trend Regime": "QQQ 相對 50MA/200MA 的趨勢狀態：Bullish Trend、Bullish Pullback、Major Trend Risk 或 Insufficient MA Data。",
    "V20 Adjusted Score": "V20 最終調整分數，已納入 NDX50R breadth；若 NDX50R_Computed 缺失則輸出 PARTIAL，不進行交易判斷。≥+5 Risk-On，但若 US30Y 紅燈會被 cap；+2~+4 可持有不追高；-1~+1 中性；-2~-4 Warning；≤-5 Defensive。",
    "Breadth Health": "NDX50R_Computed 市場廣度：>70 強牛，50–70 健康偏多，35–50 中性，20–35 轉弱，<20 高風險。",
    "NDX50R Breadth Score": "NDX50R_Computed 分數：>70 +2，50–70 +1，35–50 0，20–35 -1，<20 -2；缺失時不給 0 分，改為 excluded / PARTIAL。",
    "NDX50R_Computed": "自算 Nasdaq 100 中收盤價高於 50MA 的比例；V20.2 核心 breadth 指標。",
    "NDX Advancers": "Nasdaq 100 今日收盤高於昨日收盤的成分股數。",
    "NDX Decliners": "Nasdaq 100 今日收盤低於昨日收盤的成分股數。",
    "NDX AD Daily": "Nasdaq 100 每日 A/D：Advancers - Decliners；只做警報，不計分。",
    "NDX AD Line Computed": "自算 Nasdaq 100 A/D 累積線：昨日 AD Line + 今日 AD Daily；只做警報，不計分。",
    "NDX Breadth Universe": "本次成功納入 50MA breadth 計算的 Nasdaq 100 成分股數；太低代表資料品質不夠。",
    "A/D Alert": "Nasdaq 100 A/D Line 只作警報不計分；缺失時顯示 no divergence check；QQQ 漲但 A/D 下降為 breadth divergence，QQQ 跌但 A/D 改善為內部轉強。",
    "Long-End Yield Shock": "US30Y 長端利率衝擊。0 正常，-1 黃燈，-2 橘燈，≤-3 紅燈；>5.15% 或快速上升會壓縮 QQQ 估值。",
    "Rate Regime": "NORMAL/YELLOW/ORANGE/RED/CRISIS。由 US30Y 絕對值與 US10Y/US30Y 變化判斷。",
    "Fragile Risk-On": "TRUE 表示 QQQ/AI 價格可能仍強，但 US30Y 壓力使上漲品質下降，不准 Strong Buy。",
    "3D Score": "近 3 個 clean OK 交易日平均。≥+3 趨勢改善；-2~+2 震盪；≤-3 轉弱。只用 OK rows。",
    "AI Chain Weak Count 3D": "近 3 個 clean OK 日中，AI 鏈弱於 QQQ 的天數。0 健康，1 警示，2 高警戒，3 確認轉弱。",
    "Macro Valuation Pressure": "宏觀估值壓力分數，範圍約 +1 到 -4。越負代表 US10Y/US30Y/DXY/VIX 對高估值股越不利。",
    "AI Chain Stress": "AI 鏈壓力分數，範圍約 +2 到 -5。越負代表 SOXX/NVDA/TSM 明顯弱於 QQQ。",
    "AI ROI Risk": "AI CapEx/折舊/FCF 風險。平日多為 0；財報確認 ROI 壓力時可到 -5，基本面改善可加分。",
    "Gamma Volatility Warning": "短線波動放大警示，通常 0 或 -1；不可當作長線賣出理由。",
    "Energy AI Rotation": "AI 基礎設施/能源輪動。+1 表示健康輪動，-1 可能是能源通膨壓力，0 中性。",
    "Narrative Risk Log": "新聞/影片/敘事只記錄，不直接進分數；需價格或財報確認。",
    "10D Score": "10 日中期分數；需足夠 clean OK rows。N/A 代表資料不足。",
    "20D Score": "20 日中期分數；需足夠 clean OK rows。N/A 代表資料不足。",
    "Clean Valid Count 3D": "3D 計算中真正可用的 OK rows 數；低於 3 時不可稱為三日確認。",
    "3-of-3 Confirmed": "TRUE 代表 3 個 clean OK 日都確認同一方向；FALSE 代表尚未完成三日確認。",
    "1Y Entry Score": "1 年進場分數。≥70 可分批，50–69 觀察，<50 等待；需配合趨勢與風險。",
    "1Y Entry Zone": "1 年進場區域：Buy/Accumulate/Wait/Avoid。",
    "1Y Suggested Action": "1Y 模組的行動建議；服務中長期，不覆蓋短線風控。",
    "1Y Confidence": "1Y 模組信心；資料不足時降低權重。",
    "Main Negative Signals": "主要扣分原因。用來檢查 V20 是否真的有理由，不准空白 fallback。",
    "Main Positive Signals": "主要加分原因。用來避免只看壞消息。",
    "Confidence": "整體訊號信心。Low 時不要過度交易；High 才能提高行動權重。",
    "Commentary": "機械化總結：列出 1D/3D/10D/20D、V20 adjusted 與資料排除狀態。",
}


DECISION_CATEGORIES = {
    "System Version": "00 系統 / 資料品質",
    "Latest Date": "00 系統 / 資料品質",
    "Data Status": "00 系統 / 資料品質",
    "Valid For Signal": "00 系統 / 資料品質",
    "Confidence": "00 系統 / 資料品質",
    "Commentary": "00 系統 / 資料品質",
    "Composite Regime": "01 總結行動",
    "Composite Action": "01 總結行動",
    "V20 State": "01 總結行動",
    "V20 Action": "01 總結行動",
    "Transition Probability": "01 總結行動",
    "V20 Adjusted Score": "01 總結行動",
    "Main Negative Signals": "01 總結行動",
    "Main Positive Signals": "01 總結行動",
    "Macro Valuation Pressure": "02 宏觀流動性",
    "Rate Regime": "02 宏觀流動性",
    "Long-End Yield Shock": "02 宏觀流動性",
    "Energy AI Rotation": "02 宏觀流動性",
    "AI Chain Stress": "03 AI 領導性",
    "AI Chain Weak Count 3D": "03 AI 領導性",
    "AI ROI Risk": "03 AI 領導性",
    "Narrative Risk Log": "03 AI 領導性",
    "Breadth Health": "04 市場廣度",
    "NDX50R_Computed": "04 市場廣度",
    "NDX50R Breadth Score": "04 市場廣度",
    "NDX Advancers": "04 市場廣度",
    "NDX Decliners": "04 市場廣度",
    "NDX AD Daily": "04 市場廣度",
    "NDX AD Line Computed": "04 市場廣度",
    "NDX Breadth Universe": "04 市場廣度",
    "A/D Alert": "04 市場廣度",
    "Fragile Risk-On": "05 風險壓力",
    "Gamma Volatility Warning": "05 風險壓力",
    "1D Score": "06 真假突破確認",
    "3D Score": "06 真假突破確認",
    "10D Score": "06 真假突破確認",
    "20D Score": "06 真假突破確認",
    "Clean Valid Count 3D": "06 真假突破確認",
    "3-of-3 Confirmed": "06 真假突破確認",
    "QQQ 50MA": "07 趨勢 / 1Y 進場",
    "QQQ 200MA": "07 趨勢 / 1Y 進場",
    "QQQ Trend Regime": "07 趨勢 / 1Y 進場",
    "1Y Entry Score": "07 趨勢 / 1Y 進場",
    "1Y Entry Zone": "07 趨勢 / 1Y 進場",
    "1Y Suggested Action": "07 趨勢 / 1Y 進場",
    "1Y Confidence": "07 趨勢 / 1Y 進場",
}


def decision_row(metric: str, value: Any, category: Optional[str] = None) -> List[Any]:
    category = category or DECISION_CATEGORIES.get(metric, "99 其他")
    return [category, metric, value, DECISION_MEANINGS.get(metric, "")]


def build_decision_output_rows(latest: Dict[str, Any], latest_agent: Dict[str, Any], latest_1y: Dict[str, Any]) -> List[List[Any]]:
    """V20.7 Decision_Output grouped by decision attributes, not raw indicator order."""
    return [
        # 01 summary / action
        decision_row("Composite Regime", latest.get("Regime_Composite", "")),
        decision_row("Composite Action", latest.get("Action_Composite", "")),
        decision_row("V20 State", latest.get("V20_State", "")),
        decision_row("V20 Action", latest.get("V20_Action", "")),
        decision_row("Transition Probability", latest.get("Transition_Probability", "")),
        decision_row("V20 Adjusted Score", latest.get("V20_Adjusted_Score", "")),
        decision_row("Main Negative Signals", latest.get("Main_Negative_Signals", "")),
        decision_row("Main Positive Signals", latest.get("Main_Positive_Signals", "")),

        # 02 macro liquidity
        decision_row("Macro Valuation Pressure", latest.get("Macro_Valuation_Pressure", "")),
        decision_row("Rate Regime", latest.get("Rate_Regime", "")),
        decision_row("Long-End Yield Shock", latest.get("Long_End_Yield_Shock", "")),
        decision_row("Energy AI Rotation", latest.get("Energy_AI_Rotation", "")),

        # 03 AI leadership
        decision_row("AI Chain Stress", latest.get("AI_Chain_Stress", "")),
        decision_row("AI Chain Weak Count 3D", latest.get("AI_Chain_Weak_Count_3D", "")),
        decision_row("AI ROI Risk", latest.get("AI_ROI_Risk", "")),
        decision_row("Narrative Risk Log", latest.get("Narrative_Risk_Log", "")),

        # 04 market breadth
        decision_row("Breadth Health", latest.get("Breadth_Health", latest_agent.get("Breadth_Health", ""))),
        decision_row("NDX50R_Computed", latest_agent.get("NDX50R_Computed", "")),
        decision_row("NDX50R Breadth Score", latest_agent.get("NDX50R_Breadth_Score", "")),
        decision_row("NDX Advancers", latest_agent.get("NDX_Advancers", "")),
        decision_row("NDX Decliners", latest_agent.get("NDX_Decliners", "")),
        decision_row("NDX AD Daily", latest_agent.get("NDX_AD_Daily", "")),
        decision_row("NDX AD Line Computed", latest_agent.get("NDX_AD_Line_Computed", "")),
        decision_row("NDX Breadth Universe", latest_agent.get("NDX_Breadth_Universe", "")),
        decision_row("A/D Alert", latest.get("AD_Alert", latest_agent.get("AD_Alert", ""))),

        # 05 risk pressure
        decision_row("Fragile Risk-On", latest.get("Fragile_Risk_On", "")),
        decision_row("Gamma Volatility Warning", latest.get("Gamma_Volatility_Warning", "")),

        # 06 breakout confirmation
        decision_row("1D Score", latest.get("Score_1D", "")),
        decision_row("3D Score", latest.get("Score_3D", "")),
        decision_row("10D Score", latest.get("Score_10D", "")),
        decision_row("20D Score", latest.get("Score_20D", "")),
        decision_row("Clean Valid Count 3D", latest.get("Clean_Valid_Count_3D", "")),
        decision_row("3-of-3 Confirmed", latest_agent.get("3_of_3_Confirmed", "")),

        # 07 trend / 1Y entry
        decision_row("QQQ 50MA", latest_1y.get("QQQ_50MA", "")),
        decision_row("QQQ 200MA", latest_1y.get("QQQ_200MA", "")),
        decision_row("QQQ Trend Regime", classify_qqq_trend_regime(optional_float(latest_1y.get("QQQ_Close")), optional_float(latest_1y.get("QQQ_50MA")), optional_float(latest_1y.get("QQQ_200MA")))),
        decision_row("1Y Entry Score", latest_1y.get("Total_1Y_Entry_Score", "")),
        decision_row("1Y Entry Zone", latest_1y.get("Entry_Zone", "")),
        decision_row("1Y Suggested Action", latest_1y.get("Suggested_Action", "")),
        decision_row("1Y Confidence", latest_1y.get("Confidence", "")),

        # 00 system / data quality at bottom because it is diagnostic, not market thesis
        decision_row("System Version", VERSION),
        decision_row("Latest Date", latest.get("Date", "")),
        decision_row("Data Status", latest.get("Data_Status", "")),
        decision_row("Valid For Signal", latest.get("Valid_For_Signal", "")),
        decision_row("Confidence", latest.get("Confidence", "")),
        decision_row("Commentary", latest.get("Commentary", "")),
    ]


LONG_TERM_HEADERS = [
    "Date", "Valid_For_1Y", "QQQ_Close", "QQQ_50MA", "QQQ_200MA", "QQQ_Drawdown_From_High",
    "Trend_Score", "Drawdown_Score", "Macro_Score", "Leadership_5D_Score", "Breadth_5D_Score", "NDX50R_Breadth_Score", "Breadth_Health", "AD_Alert",
    "Credit_Score", "AI_Chain_Stress", "AI_ROI_Risk", "Total_1Y_Entry_Score", "Entry_Zone", "Suggested_Action",
    "Main_Positive_Factors", "Main_Negative_Factors", "Narrative_Risk_Log", "Confidence", "Data_Status",
]

SCORING_REQUIRED_FIELDS = [
    "Date", "QQQ_%", "SOXX_%", "NVDA_%", "MSFT_%", "VIX_%", "US10Y_%", "US30Y_%", "DXY_%", "UKOIL_%",
    "QQQE_vs_QQQ_%", "RSP_vs_SPY_%", "IWM_vs_SPY_%", "HYG_vs_TLT_%", "QQQ_Vol_Ratio",
]


def log(msg: str) -> None:
    now = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
    print(f"[{now}] {msg}", flush=True)


def col_to_letter(n: int) -> str:
    out = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        out = chr(65 + rem) + out
    return out


def is_blank(v: Any) -> bool:
    return v is None or v == "" or (isinstance(v, float) and math.isnan(v))


def parse_float(v: Any, *, allow_blank: bool = False, field: str = "") -> Optional[float]:
    if is_blank(v):
        if allow_blank:
            return None
        raise ValueError(f"Missing required value: {field or '<unknown>'}")
    if isinstance(v, (int, float)):
        if isinstance(v, float) and math.isnan(v):
            if allow_blank:
                return None
            raise ValueError(f"NaN required value: {field or '<unknown>'}")
        return float(v)
    s = str(v).strip().replace(",", "")
    if s.endswith("%"):
        return float(s[:-1].strip()) / 100.0
    return float(s)


def optional_float(v: Any) -> Optional[float]:
    return parse_float(v, allow_blank=True)


def required_float(row: Dict[str, Any], field: str) -> float:
    if field not in row:
        raise ValueError(f"Missing required column: {field}")
    return parse_float(row[field], field=field)  # type: ignore[return-value]


def pct(curr: Any, prev: Any) -> Optional[float]:
    c = optional_float(curr)
    p = optional_float(prev)
    if p is None or p == 0 or c is None:
        return None
    return (c - p) / p


def fmt_pct(x: float) -> str:
    return f"{x:.2%}"


def clamp_int(value: Optional[float], low: int, high: int, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(max(low, min(high, round(float(value)))))
    except Exception:
        return default


def classify_v19_state(adjusted_score: float, ai_chain_weak_3d: int, ai_roi_risk: int, long_end_yield_shock: int = 0, rate_regime: str = "") -> Tuple[str, str]:
    """V20 final state. AI chain 3D and AI ROI reset override generic score."""
    if long_end_yield_shock <= -3:
        return "LONG-END YIELD SHOCK", "防守；停止追高；降低 QQQ/AI beta，等待 US30Y 回落或半導體修復"
    if rate_regime in ["RED", "CRISIS"] and adjusted_score >= 5:
        return "FRAGILE RISK-ON", "價格仍強但長端利率壓迫估值；持有可以，不追高"
    if ai_roi_risk <= -2:
        return "AI ROI RESET RISK", "中期降低 AI beta；等 revenue / margin / FCF 修復再加碼"
    if ai_chain_weak_3d >= 3:
        return "AI CHAIN CONFIRMED WEAKNESS", "減碼 QQQ/AI beta 10–20%，等待 SOXX/NVDA 修復"
    if adjusted_score >= 5:
        return "RISK-ON CONFIRMED", "可持有；只在回踩確認後分批加碼"
    if adjusted_score >= 2:
        return "CONSTRUCTIVE BUT WATCH", "持有，不追高"
    if adjusted_score >= -1:
        return "NEUTRAL / FRAGILE", "觀望；等待三日確認"
    if adjusted_score >= -4:
        return "WARNING", "停止新買；降低追價；等待三日確認"
    return "DEFENSIVE", "減碼 / 降低 AI beta"



def normalize_ratio(value: Optional[float]) -> Optional[float]:
    """Accept either 62.5 or 0.625 and normalize to 0.625."""
    if value is None:
        return None
    return value / 100.0 if value > 1.0 else value


def score_ndx50r(value: Optional[float]) -> Tuple[int, str]:
    v = normalize_ratio(value)
    if v is None:
        return 0, "NDX50R_Computed unavailable; computed breadth excluded from scoring"
    pct_txt = f"{v*100:.1f}%"
    if v > 0.70:
        return 2, f"Strong breadth: NDX50R_Computed {pct_txt}"
    if v >= 0.50:
        return 1, f"Healthy breadth: NDX50R_Computed {pct_txt}"
    if v >= 0.35:
        return 0, f"Neutral breadth: NDX50R_Computed {pct_txt}"
    if v >= 0.20:
        return -1, f"Weak breadth: NDX50R_Computed {pct_txt}"
    return -2, f"High-risk breadth: NDX50R_Computed {pct_txt}"


def ad_line_alert(qqq_pct: Optional[float], ad_pct: Optional[float]) -> str:
    if qqq_pct is None or ad_pct is None:
        return "A/D unavailable; no divergence check"
    if qqq_pct > 0 and ad_pct < 0:
        return "⚠️ Breadth Divergence Warning: QQQ up but Nasdaq 100 A/D Line down; mega-cap support risk"
    if qqq_pct < 0 and ad_pct > 0:
        return "🟡 Internal Strength Improving: QQQ down but Nasdaq 100 A/D Line up"
    return "No A/D divergence"


def is_ndx50r_missing(row: Dict[str, Any]) -> bool:
    """V20.2 strict guard: missing computed Nasdaq 100 breadth is not neutral.

    NDX50R_Computed is the core breadth input in V20.2. If it is unavailable, the
    system must not silently assign 0. The row becomes PARTIAL and the final
    action becomes WAIT / DATA INCOMPLETE.
    """
    return normalize_ratio(optional_float(row.get("NDX50R_Computed"))) is None


def mark_ndx50r_missing(row: Dict[str, Any], warnings: List[str]) -> None:
    """Mutate a Daily_Tracker row so the dashboard is honest about missing breadth."""
    if is_ndx50r_missing(row):
        row["NDX50R_Breadth_Score"] = ""
        row["Breadth_Health"] = "NDX50R_Computed unavailable; computed breadth excluded from scoring"
        if not row.get("AD_Alert"):
            row["AD_Alert"] = "A/D unavailable; no divergence check"
        warnings.append("NDX50R_Computed unavailable; computed breadth excluded from scoring; Composite_Status=PARTIAL; Final_Action=WAIT / DATA INCOMPLETE")

def clean_json_value(v: Any) -> Any:
    if isinstance(v, float) and math.isnan(v):
        return ""
    if isinstance(v, (pd.Timestamp, datetime)):
        return v.date().isoformat()
    return v


def get_gspread_client():
    import gspread
    from google.oauth2.service_account import Credentials
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    if os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"):
        info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
        creds = Credentials.from_service_account_info(info, scopes=scopes)
    elif os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        creds = Credentials.from_service_account_file(os.environ["GOOGLE_APPLICATION_CREDENTIALS"], scopes=scopes)
    else:
        raise RuntimeError("Missing Google credentials. Set GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_APPLICATION_CREDENTIALS.")
    return gspread.authorize(creds)


def get_or_add_ws(sh, title: str, rows: int = 1000, cols: int = 30):
    try:
        return sh.worksheet(title)
    except Exception as e:
        try:
            import gspread
            worksheet_not_found = gspread.WorksheetNotFound
        except Exception:
            worksheet_not_found = Exception
        if not isinstance(e, worksheet_not_found):
            raise
        return sh.add_worksheet(title=title, rows=rows, cols=cols)


def ensure_headers_preserve(ws, headers: List[str]) -> None:
    """Migrate headers without silently destroying old rows.

    Existing columns with matching names are preserved; new columns are blank.
    This is safer than V4.3's clear-and-recreate behavior.
    """
    existing = ws.row_values(1)
    if existing[: len(headers)] == headers:
        return
    old_records = ws.get_all_records() if existing else []
    rows = [[r.get(h, "") for h in headers] for r in old_records]
    ws.clear()
    ws.update("A1", [headers])
    if rows:
        ws.append_rows(rows, value_input_option="USER_ENTERED")
    try:
        ws.freeze(rows=1)
    except Exception:
        pass


def get_records(ws) -> List[Dict[str, Any]]:
    return ws.get_all_records()


@dataclass
class TickerSnapshot:
    key: str
    ticker: str
    date: str
    close: float
    prev_close: float
    pct_change: Optional[float]
    volume: int = 0
    vol20: Optional[float] = None
    ma50: Optional[float] = None
    ma200: Optional[float] = None
    warning: str = ""


def _date_only(value: Any) -> str:
    return pd.Timestamp(value).date().isoformat()


def dataframe_from_qqq_cache_rows(cache_rows: List[Dict[str, Any]]) -> pd.DataFrame:
    records: List[Dict[str, Any]] = []
    for r in cache_rows:
        try:
            d = _date_only(r.get("Date"))
            close = optional_float(r.get("Close"))
            if close is None:
                continue
            volume = optional_float(r.get("Volume")) or 0.0
            records.append({"Date": d, "Close": float(close), "Volume": float(volume)})
        except Exception:
            continue
    if not records:
        return pd.DataFrame(columns=["Close", "Volume"])
    df = pd.DataFrame(records).drop_duplicates(subset=["Date"], keep="last")
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values("Date").set_index("Date")
    return df[["Close", "Volume"]]


def qqq_cache_rows_from_history(hist: pd.DataFrame, source: str, status: str = "OK", warning: str = "") -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    fetch_time = datetime.now(TZ).isoformat(timespec="seconds")
    for idx, row in hist.dropna(subset=["Close"]).iterrows():
        rows.append({
            "Date": idx.date().isoformat(),
            "Close": float(row["Close"]),
            "Volume": int(row.get("Volume", 0) or 0),
            "Source": source,
            "Fetch_Time": fetch_time,
            "Data_Status": status,
            "Warnings": warning,
        })
    return rows


def merge_qqq_cache_rows(existing_rows: List[Dict[str, Any]], new_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    for r in existing_rows + new_rows:
        try:
            d = _date_only(r.get("Date"))
            close = optional_float(r.get("Close"))
            if close is None:
                continue
            merged[d] = {
                "Date": d,
                "Close": float(close),
                "Volume": int(optional_float(r.get("Volume")) or 0),
                "Source": r.get("Source", "cache"),
                "Fetch_Time": r.get("Fetch_Time", ""),
                "Data_Status": r.get("Data_Status", "OK"),
                "Warnings": r.get("Warnings", ""),
            }
        except Exception:
            continue
    return [merged[d] for d in sorted(merged.keys())]


def update_qqq_cache_rows(existing_rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[str], bool]:
    """Return (cache_rows, warnings, stale_used).

    V20.6 rule: QQQ uses cached history. First real run bootstraps 300d.
    Later runs fetch only a short incremental window and append/overwrite recent
    bars. If Yahoo fails but a cache exists, keep the last cached bar and mark
    the market row STALE instead of crashing.
    """
    warnings: List[str] = []
    existing_df = dataframe_from_qqq_cache_rows(existing_rows)
    has_enough_cache = len(existing_df) >= QQQ_CACHE_MIN_BARS
    period = QQQ_CACHE_INCREMENTAL_PERIOD if has_enough_cache else QQQ_CACHE_BOOTSTRAP_PERIOD
    source = "yfinance_incremental" if has_enough_cache else "yfinance_bootstrap_300d"
    try:
        hist = get_history("QQQ", period=period)
        new_rows = qqq_cache_rows_from_history(hist, source=source)
        merged_rows = merge_qqq_cache_rows(existing_rows, new_rows)
        if len(dataframe_from_qqq_cache_rows(merged_rows)) < 2:
            raise RuntimeError("QQQ cache still has fewer than two bars after update")
        return merged_rows, warnings, False
    except Exception as e:
        if len(existing_df) >= 2:
            warnings.append(f"QQQ yfinance update failed; using cached last QQQ bar as STALE: {e}")
            return merge_qqq_cache_rows(existing_rows, []), warnings, True
        warnings.append(f"QQQ yfinance bootstrap failed and no usable QQQ cache exists: {e}")
        return merge_qqq_cache_rows(existing_rows, []), warnings, True


def qqq_snapshot_from_cache(cache_rows: List[Dict[str, Any]], stale: bool) -> TickerSnapshot:
    hist = dataframe_from_qqq_cache_rows(cache_rows)
    if hist is None or hist.empty or len(hist) < 2:
        raise RuntimeError("QQQ cache has fewer than two usable rows")
    last = hist.iloc[-1]
    prev = hist.iloc[-2]
    date = hist.index[-1].date().isoformat()
    close = float(last["Close"])
    prev_close = float(prev["Close"])
    volume = int(last.get("Volume", 0) or 0)
    vols = [float(x) for x in hist["Volume"].tail(21).iloc[:-1].dropna().tolist() if float(x) > 0]
    vol20 = sum(vols[-20:]) / len(vols[-20:]) if vols else None
    snap = TickerSnapshot(
        key="QQQ", ticker="QQQ", date=date, close=close, prev_close=prev_close,
        pct_change=pct(close, prev_close), volume=volume, vol20=vol20,
        ma50=compute_moving_average_from_history(hist, window=50),
        ma200=compute_moving_average_from_history(hist, window=200),
    )
    if stale:
        snap.warning = "QQQ latest close from cached history; yfinance update failed; Data_Status=STALE"
    return snap


def fallback_snapshot_from_previous(previous_rows: List[Dict[str, Any]], key: str, ticker: str, warning: str) -> TickerSnapshot:
    close_field = "QQQ_Close" if key == "QQQ" else f"{key}_Close"
    if key in ["VIX", "US10Y", "US30Y", "DXY", "UKOIL", "QQQE", "RSP", "SPY", "IWM", "HYG", "TLT", "XLE", "XLU"]:
        close_field = key
    for r in reversed(previous_rows):
        close = optional_float(r.get(close_field))
        if close is not None:
            source_date = str(r.get(f"{key}_Source_Date", "") or r.get("Date", ""))
            snap = TickerSnapshot(key=key, ticker=ticker, date=source_date, close=close, prev_close=close, pct_change=None, warning=warning)
            if key == "QQQ":
                snap.volume = int(optional_float(r.get("QQQ_Volume")) or 0)
                snap.vol20 = optional_float(r.get("QQQ_Vol_20D_Avg"))
                snap.ma50 = optional_float(r.get("QQQ_50MA"))
                snap.ma200 = optional_float(r.get("QQQ_200MA"))
            return snap
    today = datetime.now(TZ).date().isoformat()
    return TickerSnapshot(key=key, ticker=ticker, date=today, close=0.0, prev_close=0.0, pct_change=None, warning=warning)


def get_history(ticker: str, period: str = "45d") -> pd.DataFrame:
    """Fetch yfinance history with retry/backoff.

    V20.6 rule: transient Yahoo/yfinance failures must be retried before the
    caller decides whether to mark the row WARNING/STALE. This function still
    raises after all retries, but fetch_market_snapshot must not let that crash
    the full agent.
    """
    import yfinance as yf

    last_error: Optional[Exception] = None
    retries = max(1, YFINANCE_RETRIES)
    for attempt in range(1, retries + 1):
        try:
            hist = yf.Ticker(ticker).history(period=period, interval="1d", auto_adjust=False)
            if hist is None or hist.empty:
                raise RuntimeError(f"No data for ticker {ticker}")
            hist = hist.dropna(subset=["Close"])
            if len(hist) < 2:
                raise RuntimeError(f"Not enough data for ticker {ticker}")
            return hist
        except Exception as e:
            last_error = e
            if attempt >= retries:
                break
            delay = YFINANCE_BACKOFF_SECONDS[min(attempt - 1, len(YFINANCE_BACKOFF_SECONDS) - 1)] if YFINANCE_BACKOFF_SECONDS else 5.0
            delay = delay + random.uniform(0.0, 1.5)
            log(f"yfinance retry {attempt}/{retries} for {ticker} period={period} failed: {e}. Sleeping {delay:.1f}s")
            time.sleep(delay)
    raise RuntimeError(f"yfinance failed for {ticker} period={period} after {retries} attempt(s): {last_error}")


def compute_moving_average_from_history(hist: pd.DataFrame, target_date: Optional[Any] = None, window: int = 50) -> Optional[float]:
    """Compute moving average using bars up to target_date, or latest bar if target_date is None."""
    if hist is None or hist.empty or "Close" not in hist.columns:
        return None
    closes = hist["Close"].dropna()
    if target_date is not None:
        target = pd.Timestamp(target_date).date()
        closes = closes[closes.index.date <= target]
    if len(closes) < window:
        return None
    return float(closes.tail(window).mean())


def classify_qqq_trend_regime(close: Optional[float], ma50: Optional[float], ma200: Optional[float]) -> str:
    if close is None or ma50 is None or ma200 is None:
        return "Insufficient MA Data"
    if close > ma50 > ma200:
        return "Bullish Trend: QQQ > 50MA > 200MA"
    if close > ma200 and close <= ma50:
        return "Bullish Pullback: QQQ above 200MA but below/near 50MA"
    if close > ma50 and ma50 <= ma200:
        return "Recovering / Unconfirmed: QQQ above 50MA but 50MA not above 200MA"
    if close < ma200:
        return "Major Trend Risk: QQQ below 200MA"
    return "Neutral Trend"


def fetch_one(key: str, ticker: str) -> TickerSnapshot:
    hist = get_history(ticker, period="300d" if key == "QQQ" else "45d")
    last = hist.iloc[-1]
    prev = hist.iloc[-2]
    date = hist.index[-1].date().isoformat()
    close = float(last["Close"])
    prev_close = float(prev["Close"])
    if key in ["US10Y", "US30Y"]:
        close /= 10.0
        prev_close /= 10.0
    volume = int(last.get("Volume", 0) or 0)
    vol20 = None
    if "Volume" in hist.columns:
        vols = [float(x) for x in hist["Volume"].tail(21).iloc[:-1].dropna().tolist() if float(x) > 0]
        if vols:
            vol20 = sum(vols[-20:]) / len(vols[-20:])
    ma50 = compute_moving_average_from_history(hist, window=50) if key == "QQQ" else None
    ma200 = compute_moving_average_from_history(hist, window=200) if key == "QQQ" else None
    return TickerSnapshot(key, ticker, date, close, prev_close, pct(close, prev_close), volume, vol20, ma50, ma200)


def detect_stale_macro(previous_rows: List[Dict[str, Any]], snapshot: Dict[str, Any], n: int = 3) -> List[str]:
    warnings: List[str] = []
    combined = previous_rows + [snapshot]
    for col in ["UKOIL", "US10Y", "US30Y", "DXY"]:
        vals: List[float] = []
        for r in combined[-n:]:
            v = optional_float(r.get(col))
            if v is not None:
                vals.append(round(v, 6))
        if len(vals) == n and len(set(vals)) == 1:
            warnings.append(f"{col} stale: unchanged for last {n} rows")
    return warnings



def get_ndx_tickers() -> List[str]:
    """Return Nasdaq 100 universe for computed breadth.

    Priority:
    1. NDX_TICKERS env override, comma-separated.
    2. Wikipedia Nasdaq-100 table, refreshed at runtime in GitHub Actions.
    3. Static fallback list, so the agent still runs if Wikipedia is unavailable.
    """
    override = os.getenv("NDX_TICKERS", "").strip()
    if override:
        return sorted({x.strip().upper().replace(".", "-") for x in override.split(",") if x.strip()})
    try:
        tables = pd.read_html("https://en.wikipedia.org/wiki/Nasdaq-100")
        for tbl in tables:
            cols = [str(c).strip().lower() for c in tbl.columns]
            if "ticker" in cols:
                col = tbl.columns[cols.index("ticker")]
                vals = [str(x).strip().upper().replace(".", "-") for x in tbl[col].dropna().tolist()]
                vals = [x for x in vals if x and x != "NAN"]
                if len(vals) >= 90:
                    return sorted(set(vals))
            if "symbol" in cols:
                col = tbl.columns[cols.index("symbol")]
                vals = [str(x).strip().upper().replace(".", "-") for x in tbl[col].dropna().tolist()]
                vals = [x for x in vals if x and x != "NAN"]
                if len(vals) >= 90:
                    return sorted(set(vals))
    except Exception as e:
        log(f"NDX universe dynamic fetch failed; using static fallback: {e}")
    return sorted(set(NDX_STATIC_TICKERS))


def last_previous_value(rows: List[Dict[str, Any]], field: str) -> Optional[float]:
    for r in reversed(rows):
        v = optional_float(r.get(field))
        if v is not None:
            return v
    return None


def close_series_from_download(df: pd.DataFrame, ticker: str) -> Optional[pd.Series]:
    """Extract Close series from yfinance.download output for one ticker."""
    if df is None or df.empty:
        return None
    try:
        if isinstance(df.columns, pd.MultiIndex):
            if (ticker, "Close") in df.columns:
                return df[(ticker, "Close")].dropna()
            if ("Close", ticker) in df.columns:
                return df[("Close", ticker)].dropna()
            # Try case-insensitive ticker matching.
            for col in df.columns:
                if len(col) >= 2 and str(col[0]).upper() == ticker.upper() and str(col[1]).lower() == "close":
                    return df[col].dropna()
                if len(col) >= 2 and str(col[0]).lower() == "close" and str(col[1]).upper() == ticker.upper():
                    return df[col].dropna()
        elif "Close" in df.columns:
            return df["Close"].dropna()
    except Exception:
        return None
    return None


def fetch_ndx_breadth(previous_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Compute Nasdaq 100 breadth internally.

    NDX50R_Computed = count(close > 50DMA) / valid Nasdaq 100 constituents.
    NDX_AD_Daily = advancers - decliners.
    NDX_AD_Line_Computed = previous NDX_AD_Line_Computed + NDX_AD_Daily.

    This intentionally replaces external NDX50R_Computed / A-D symbols, because those
    are StockCharts/TradingView-style indicators and are not reliable yfinance tickers.
    """
    import yfinance as yf

    tickers = get_ndx_tickers()
    result: Dict[str, Any] = {
        "NDX50R_Computed": "",
        "NDX_Advancers": "",
        "NDX_Decliners": "",
        "NDX_AD_Daily": "",
        "NDX_AD_Line_Computed": "",
        "NDX_AD_Line_%": "",
        "NDX_Breadth_Universe": len(tickers),
        "NDX_Breadth_Missing": len(tickers),
        "Warning": "",
    }
    if not tickers:
        result["Warning"] = "NDX breadth universe unavailable"
        return result

    try:
        df = yf.download(
            tickers=" ".join(tickers),
            period="90d",
            interval="1d",
            auto_adjust=False,
            group_by="ticker",
            threads=True,
            progress=False,
        )
    except Exception as e:
        result["Warning"] = f"NDX breadth download failed: {e}"
        return result

    advancers = 0
    decliners = 0
    valid_ad = 0
    above50 = 0
    valid50 = 0

    for t in tickers:
        ser = close_series_from_download(df, t)
        if ser is None or len(ser) < 2:
            continue
        last = float(ser.iloc[-1])
        prev = float(ser.iloc[-2])
        valid_ad += 1
        if last > prev:
            advancers += 1
        elif last < prev:
            decliners += 1
        if len(ser) >= 50:
            ma50 = float(ser.tail(50).mean())
            valid50 += 1
            if last > ma50:
                above50 += 1

    daily_ad = advancers - decliners if valid_ad else None
    prev_line = last_previous_value(previous_rows, "NDX_AD_Line_Computed")
    if prev_line is None:
        prev_line = optional_float(os.getenv("NDX_AD_LINE_BASELINE", "")) or 0.0
    ad_line = (prev_line + daily_ad) if daily_ad is not None else None
    ad_line_pct = pct(ad_line, prev_line) if ad_line is not None and prev_line not in (None, 0) else None
    ndx50r = (above50 / valid50) if valid50 else None

    result.update({
        "NDX50R_Computed": ndx50r if ndx50r is not None else "",
        "NDX_Advancers": advancers if valid_ad else "",
        "NDX_Decliners": decliners if valid_ad else "",
        "NDX_AD_Daily": daily_ad if daily_ad is not None else "",
        "NDX_AD_Line_Computed": ad_line if ad_line is not None else "",
        "NDX_AD_Line_%": ad_line_pct if ad_line_pct is not None else "",
        "NDX_Breadth_Universe": valid50,
        "NDX_Breadth_Missing": max(0, len(tickers) - valid50),
    })

    if valid50 < 80:
        result["Warning"] = f"NDX breadth coverage low: valid50={valid50}, universe={len(tickers)}"
    return result



def _hist_close_for_date(hist: pd.DataFrame, target_date: Any) -> Tuple[str, pd.Series, pd.Series]:
    """Return (source_date, row, previous_row) using the latest bar <= target_date."""
    if hist is None or hist.empty:
        raise RuntimeError("empty history")
    dates = [idx.date() for idx in hist.index]
    target = pd.Timestamp(target_date).date()
    pos = None
    for i, d in enumerate(dates):
        if d <= target:
            pos = i
        else:
            break
    if pos is None or pos <= 0:
        raise RuntimeError(f"not enough history on or before {target}")
    return dates[pos].isoformat(), hist.iloc[pos], hist.iloc[pos - 1]


def _snapshot_from_history(key: str, ticker: str, hist: pd.DataFrame, target_date: Any) -> TickerSnapshot:
    src_date, row, prev = _hist_close_for_date(hist, target_date)
    close = float(row["Close"])
    prev_close = float(prev["Close"])
    if key in ["US10Y", "US30Y"]:
        close /= 10.0
        prev_close /= 10.0
    volume = int(row.get("Volume", 0) or 0)
    loc = hist.index.get_loc(row.name)
    vol20 = None
    if "Volume" in hist.columns and loc >= 1:
        prev_vols = hist.iloc[max(0, loc - 20):loc].get("Volume", pd.Series(dtype=float)).dropna().tolist()
        prev_vols = [float(x) for x in prev_vols if float(x) > 0]
        if prev_vols:
            vol20 = sum(prev_vols[-20:]) / len(prev_vols[-20:])
    ma50 = compute_moving_average_from_history(hist, target_date, window=50) if key == "QQQ" else None
    ma200 = compute_moving_average_from_history(hist, target_date, window=200) if key == "QQQ" else None
    return TickerSnapshot(key, ticker, src_date, close, prev_close, pct(close, prev_close), volume, vol20, ma50, ma200)


def close_series_from_download_for_ticker(df: pd.DataFrame, ticker: str) -> Optional[pd.Series]:
    """More tolerant yfinance.download close extraction for V20.4 backfill."""
    ser = close_series_from_download(df, ticker)
    if ser is not None and len(ser) > 0:
        return ser
    # Some yfinance versions return columns as ('Close', 'AAPL') while others return ('AAPL', 'Close').
    if isinstance(df.columns, pd.MultiIndex):
        for col in df.columns:
            parts = [str(x).upper() for x in col]
            if ticker.upper() in parts and "CLOSE" in parts:
                try:
                    return df[col].dropna()
                except Exception:
                    return None
    return None


def fetch_ndx_breadth_history(target_dates: List[str], previous_rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Compute historical Nasdaq 100 breadth for bootstrap/backfill dates.

    This avoids the first-install blank dashboard problem. The AD line baseline is arbitrary
    unless NDX_AD_LINE_BASELINE is provided; divergence alerts mainly use NDX_AD_Daily.
    """
    import yfinance as yf

    tickers = get_ndx_tickers()
    out: Dict[str, Dict[str, Any]] = {}
    if not tickers:
        return {d: {"Warning": "NDX breadth universe unavailable"} for d in target_dates}
    try:
        df = yf.download(
            tickers=" ".join(tickers),
            period="140d",
            interval="1d",
            auto_adjust=False,
            group_by="ticker",
            threads=True,
            progress=False,
        )
    except Exception as e:
        return {d: {"Warning": f"NDX breadth backfill download failed: {e}"} for d in target_dates}

    prev_line = last_previous_value(previous_rows, "NDX_AD_Line_Computed")
    if prev_line is None:
        prev_line = optional_float(os.getenv("NDX_AD_LINE_BASELINE", "")) or 0.0

    for d in target_dates:
        target = pd.Timestamp(d).date()
        advancers = decliners = valid_ad = above50 = valid50 = 0
        for t in tickers:
            ser = close_series_from_download_for_ticker(df, t)
            if ser is None or len(ser) < 2:
                continue
            eligible = ser[ser.index.date <= target]
            if len(eligible) < 2:
                continue
            last = float(eligible.iloc[-1])
            prev = float(eligible.iloc[-2])
            valid_ad += 1
            if last > prev:
                advancers += 1
            elif last < prev:
                decliners += 1
            if len(eligible) >= 50:
                ma50 = float(eligible.tail(50).mean())
                valid50 += 1
                if last > ma50:
                    above50 += 1
        daily_ad = advancers - decliners if valid_ad else None
        ad_line = (prev_line + daily_ad) if daily_ad is not None else None
        ad_line_pct = pct(ad_line, prev_line) if ad_line is not None and prev_line not in (None, 0) else None
        if ad_line is not None:
            prev_line = ad_line
        ndx50r = (above50 / valid50) if valid50 else None
        warning = ""
        if valid50 < 80:
            warning = f"NDX breadth coverage low: valid50={valid50}, universe={len(tickers)}"
        out[d] = {
            "NDX50R_Computed": ndx50r if ndx50r is not None else "",
            "NDX_Advancers": advancers if valid_ad else "",
            "NDX_Decliners": decliners if valid_ad else "",
            "NDX_AD_Daily": daily_ad if daily_ad is not None else "",
            "NDX_AD_Line_Computed": ad_line if ad_line is not None else "",
            "NDX_AD_Line_%": ad_line_pct if ad_line_pct is not None else "",
            "NDX_Breadth_Universe": valid50,
            "NDX_Breadth_Missing": max(0, len(tickers) - valid50),
            "Warning": warning,
        }
    return out


def _build_row_from_snapshots(target_date: str, data: Dict[str, TickerSnapshot], ndx_breadth: Dict[str, Any], previous_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    warnings: List[str] = []
    soft_date_notes: List[str] = []
    qqq_date = data["QQQ"].date
    for key, snap in data.items():
        # V20.4.1: for historical bootstrap, tolerate normal FX/futures calendar offsets
        # for DXY and UKOIL exactly the same way live fetch does. Otherwise the third
        # bootstrap row can become WARNING and destroy the intended 3D seed.
        if snap.date != qqq_date:
            try:
                qd = datetime.fromisoformat(qqq_date).date()
                sd = datetime.fromisoformat(snap.date).date()
                diff_days = abs((sd - qd).days)
            except Exception:
                diff_days = 999
            if key in ["DXY", "UKOIL"] and diff_days <= 1:
                soft_date_notes.append(f"{key} source date {snap.date} differs from QQQ date {qqq_date}; tolerated historical non-equity calendar offset")
            else:
                warnings.append(f"{key} source date {snap.date} differs from QQQ date {qqq_date}; historical backfill used latest bar <= QQQ date")
    if ndx_breadth.get("Warning"):
        warnings.append(str(ndx_breadth["Warning"]))

    qqq = data["QQQ"]
    qqq_pct = data["QQQ"].pct_change
    soxx_pct = data["SOXX"].pct_change
    qqqe_vs_qqq = (data["QQQE"].pct_change or 0) - (qqq_pct or 0)
    rsp_vs_spy = (data["RSP"].pct_change or 0) - (data["SPY"].pct_change or 0)
    iwm_vs_spy = (data["IWM"].pct_change or 0) - (data["SPY"].pct_change or 0)
    hyg_vs_tlt = (data["HYG"].pct_change or 0) - (data["TLT"].pct_change or 0)
    xle_vs_qqq = (data["XLE"].pct_change or 0) - (qqq_pct or 0)
    xlu_vs_qqq = (data["XLU"].pct_change or 0) - (qqq_pct or 0)
    semi_leadership = (soxx_pct or 0) - (qqq_pct or 0)
    vol_ratio = qqq.volume / qqq.vol20 if qqq.vol20 else ""
    ndx50r_value = optional_float(ndx_breadth.get("NDX50R_Computed"))
    ndx_score, ndx_health = score_ndx50r(ndx50r_value)

    row = {
        "Date": target_date,
        "QQQ_Close": data["QQQ"].close,
        "QQQ_50MA": round(data["QQQ"].ma50, 4) if data["QQQ"].ma50 is not None else "",
        "QQQ_200MA": round(data["QQQ"].ma200, 4) if data["QQQ"].ma200 is not None else "",
        "SOXX_Close": data["SOXX"].close,
        "NVDA_Close": data["NVDA"].close,
        "MSFT_Close": data["MSFT"].close,
        "TSM_Close": data["TSM"].close,
        "VIX": data["VIX"].close,
        "US10Y": data["US10Y"].close,
        "US30Y": data["US30Y"].close,
        "DXY": data["DXY"].close,
        "UKOIL": data["UKOIL"].close,
        "QQQE": data["QQQE"].close,
        "RSP": data["RSP"].close,
        "SPY": data["SPY"].close,
        "IWM": data["IWM"].close,
        "HYG": data["HYG"].close,
        "TLT": data["TLT"].close,
        "XLE": data["XLE"].close,
        "XLU": data["XLU"].close,
        "NDX50R_Computed": ndx_breadth.get("NDX50R_Computed", ""),
        "NDX_Advancers": ndx_breadth.get("NDX_Advancers", ""),
        "NDX_Decliners": ndx_breadth.get("NDX_Decliners", ""),
        "NDX_AD_Daily": ndx_breadth.get("NDX_AD_Daily", ""),
        "NDX_AD_Line_Computed": ndx_breadth.get("NDX_AD_Line_Computed", ""),
        "NDX_Breadth_Universe": ndx_breadth.get("NDX_Breadth_Universe", ""),
        "NDX_Breadth_Missing": ndx_breadth.get("NDX_Breadth_Missing", ""),
        "QQQ_Source_Date": data["QQQ"].date,
        "VIX_Source_Date": data["VIX"].date,
        "US10Y_Source_Date": data["US10Y"].date,
        "US30Y_Source_Date": data["US30Y"].date,
        "DXY_Source_Date": data["DXY"].date,
        "UKOIL_Source_Date": data["UKOIL"].date,
        "TSM_Source_Date": data["TSM"].date,
        "QQQ_Volume": qqq.volume,
        "QQQ_Vol_20D_Avg": round(qqq.vol20, 0) if qqq.vol20 else "",
        "QQQ_%": qqq_pct,
        "SOXX_%": soxx_pct,
        "NVDA_%": data["NVDA"].pct_change,
        "MSFT_%": data["MSFT"].pct_change,
        "TSM_%": data["TSM"].pct_change,
        "VIX_%": data["VIX"].pct_change,
        "US10Y_%": data["US10Y"].pct_change,
        "US30Y_%": data["US30Y"].pct_change,
        "DXY_%": data["DXY"].pct_change,
        "UKOIL_%": data["UKOIL"].pct_change,
        "QQQE_%": data["QQQE"].pct_change,
        "RSP_%": data["RSP"].pct_change,
        "SPY_%": data["SPY"].pct_change,
        "IWM_%": data["IWM"].pct_change,
        "HYG_%": data["HYG"].pct_change,
        "TLT_%": data["TLT"].pct_change,
        "XLE_%": data["XLE"].pct_change,
        "XLU_%": data["XLU"].pct_change,
        "NDX_AD_Line_%": ndx_breadth.get("NDX_AD_Line_%", ""),
        "QQQE_vs_QQQ_%": qqqe_vs_qqq,
        "RSP_vs_SPY_%": rsp_vs_spy,
        "IWM_vs_SPY_%": iwm_vs_spy,
        "HYG_vs_TLT_%": hyg_vs_tlt,
        "XLE_vs_QQQ_%": xle_vs_qqq,
        "XLU_vs_QQQ_%": xlu_vs_qqq,
        "Semi_Leadership_%": semi_leadership,
        "EqualWeight_Breadth_%": qqqe_vs_qqq,
        "NDX50R_Breadth_Score": ndx_score,
        "Breadth_Health": ndx_health,
        "AD_Alert": ad_line_alert(qqq_pct, optional_float(ndx_breadth.get("NDX_AD_Daily"))),
        "QQQ_Vol_Ratio": vol_ratio,
        "AI_ROI_Risk_Manual_Score": "",
        "AI_ROI_Risk_Note": "",
        "Gamma_Risk_Manual_Score": "",
        "Gamma_Risk_Note": "",
        "Narrative_Risk_Note": "",
    }
    mark_ndx50r_missing(row, warnings)
    row["Data_Status"] = "OK" if not warnings else "WARNING"
    # Soft date notes are informational only; they do not block scoring.
    row["Warnings"] = "; ".join(warnings + soft_date_notes)
    return row


def blank_signal_fields_for_bootstrap_baseline(row: Dict[str, Any]) -> Dict[str, Any]:
    """Oldest bootstrap row is raw-price baseline only; no breadth/AD/trade signal shown."""
    for h in DAILY_HEADERS:
        if (
            h.endswith("_%")
            or h in [
                "QQQ_Vol_Ratio", "NDX50R_Breadth_Score", "Breadth_Health", "AD_Alert",
                "AI_ROI_Risk_Manual_Score", "AI_ROI_Risk_Note", "Gamma_Risk_Manual_Score",
                "Gamma_Risk_Note", "Narrative_Risk_Note",
            ]
        ):
            row[h] = ""
    row["Data_Status"] = "BASELINE_ONLY"
    row["Warnings"] = "V20.4 bootstrap raw-price baseline. Prior 3 clean market days are backfilled after this row when available."
    return row


def fetch_bootstrap_history_rows(previous_rows: Optional[List[Dict[str, Any]]] = None, scored_days: int = 3, qqq_cache_rows: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    """Fetch one raw-price baseline + prior scored_days market rows on first install.

    This fixes the first-run blank dashboard problem. To score 3 days, we need 4 QQQ
    closes: the oldest is BASELINE_ONLY, the next three have daily % changes.
    """
    previous_rows = previous_rows or []
    needed_rows = scored_days + 1
    histories: Dict[str, pd.DataFrame] = {}
    data_by_date: Dict[str, Dict[str, TickerSnapshot]] = {}
    qqq_cache_rows = qqq_cache_rows or []
    for key, ticker in TICKERS.items():
        try:
            if key == "QQQ":
                qqq_cached_hist = dataframe_from_qqq_cache_rows(qqq_cache_rows)
                if len(qqq_cached_hist) >= needed_rows:
                    histories[key] = qqq_cached_hist
                else:
                    histories[key] = get_history(ticker, period=QQQ_CACHE_BOOTSTRAP_PERIOD)
            else:
                histories[key] = get_history(ticker, period="45d")
        except Exception as e:
            if key == "SOXX":
                fallback = "SMH"
                histories[key] = get_history(fallback)
                log(f"Bootstrap: SOXX fallback to {fallback}: {e}")
            else:
                raise RuntimeError(f"Bootstrap failed to fetch {key} ({ticker}): {e}") from e
    qqq_hist = histories["QQQ"]
    qqq_dates = [idx.date().isoformat() for idx in qqq_hist.index][-needed_rows:]
    if len(qqq_dates) < needed_rows:
        raise RuntimeError(f"Bootstrap needs {needed_rows} QQQ market rows; got {len(qqq_dates)}")
    ndx_by_date = fetch_ndx_breadth_history(qqq_dates, previous_rows)
    rows: List[Dict[str, Any]] = []
    rolling_previous_rows = list(previous_rows)
    for d in qqq_dates:
        snapshots: Dict[str, TickerSnapshot] = {}
        for key, ticker in TICKERS.items():
            snapshots[key] = _snapshot_from_history(key, ticker, histories[key], d)
        row = _build_row_from_snapshots(d, snapshots, ndx_by_date.get(d, {}), rolling_previous_rows)
        rows.append(row)
        rolling_previous_rows.append(row)
    if rows:
        rows[0] = blank_signal_fields_for_bootstrap_baseline(rows[0])
    return rows

def fetch_market_snapshot(previous_rows: Optional[List[Dict[str, Any]]] = None, qqq_cache_rows: Optional[List[Dict[str, Any]]] = None) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    warnings: List[str] = []
    data: Dict[str, TickerSnapshot] = {}
    previous_rows = previous_rows or []
    qqq_cache_rows = qqq_cache_rows or []

    # V20.6: QQQ is special. It is the anchor for close, 50MA, and 200MA.
    # Use the Google Sheet QQQ_History_Cache instead of re-downloading 300d every run.
    updated_cache_rows, qqq_cache_warnings, qqq_stale = update_qqq_cache_rows(qqq_cache_rows)
    warnings.extend(qqq_cache_warnings)
    try:
        data["QQQ"] = qqq_snapshot_from_cache(updated_cache_rows, stale=qqq_stale)
    except Exception as e:
        warn = f"QQQ cache unusable; fallback to previous Daily_Tracker row; Data_Status=WARNING: {e}"
        warnings.append(warn)
        data["QQQ"] = fallback_snapshot_from_previous(previous_rows, "QQQ", "QQQ", warn)
        qqq_stale = True

    for key, ticker in TICKERS.items():
        if key == "QQQ" or not ticker:
            continue
        try:
            data[key] = fetch_one(key, ticker)
        except Exception as e:
            if key == "SOXX":
                fallback = "SMH"
                try:
                    snap = fetch_one(key, fallback)
                    snap.warning = f"SOXX yfinance failed; fallback to {fallback}: {e}"
                    data[key] = snap
                    warnings.append(snap.warning)
                    continue
                except Exception as e2:
                    warn = f"SOXX and SMH fallback failed; using previous cached Daily_Tracker value; Data_Status=WARNING: SOXX={e}; SMH={e2}"
                    warnings.append(warn)
                    data[key] = fallback_snapshot_from_previous(previous_rows, key, ticker, warn)
                    continue
            warn = f"{key} yfinance failed; using previous Daily_Tracker value; Data_Status=WARNING: {e}"
            warnings.append(warn)
            data[key] = fallback_snapshot_from_previous(previous_rows, key, ticker, warn)

    qqq_date = data["QQQ"].date
    # FX / futures can print the next calendar date after the US equity close.
    # Treat a +/-1 calendar-day offset in DXY and UKOIL as an informational note, not a hard WARNING.
    soft_date_notes: List[str] = []
    for key, snap in data.items():
        if snap.warning:
            # already captured as STALE/WARNING; do not duplicate too aggressively
            pass
        if snap.date != qqq_date:
            try:
                qd = datetime.fromisoformat(qqq_date).date()
                sd = datetime.fromisoformat(snap.date).date()
                diff_days = abs((sd - qd).days)
            except Exception:
                diff_days = 999
            if key in ["DXY", "UKOIL"] and diff_days <= 1:
                soft_date_notes.append(f"{key} date {snap.date} differs from QQQ date {qqq_date}; tolerated non-equity calendar offset")
            else:
                warnings.append(f"{key} date {snap.date} differs from QQQ date {qqq_date}")

    qqq = data["QQQ"]
    if qqq.volume <= 0:
        warnings.append("QQQ volume is zero or missing")
    if qqq.vol20 is None or qqq.vol20 <= 0:
        warnings.append("QQQ 20D average volume missing")

    qqq_pct = data["QQQ"].pct_change
    soxx_pct = data["SOXX"].pct_change
    qqqe_vs_qqq = (data["QQQE"].pct_change or 0) - (qqq_pct or 0)
    rsp_vs_spy = (data["RSP"].pct_change or 0) - (data["SPY"].pct_change or 0)
    iwm_vs_spy = (data["IWM"].pct_change or 0) - (data["SPY"].pct_change or 0)
    hyg_vs_tlt = (data["HYG"].pct_change or 0) - (data["TLT"].pct_change or 0)
    xle_vs_qqq = (data["XLE"].pct_change or 0) - (qqq_pct or 0)
    xlu_vs_qqq = (data["XLU"].pct_change or 0) - (qqq_pct or 0)
    semi_leadership = (soxx_pct or 0) - (qqq_pct or 0)
    vol_ratio = qqq.volume / qqq.vol20 if qqq.vol20 else ""

    ndx_breadth = fetch_ndx_breadth(previous_rows)
    if ndx_breadth.get("Warning"):
        warnings.append(str(ndx_breadth["Warning"]))

    row = {
        "Date": qqq_date,
        "QQQ_Close": data["QQQ"].close,
        "QQQ_50MA": round(data["QQQ"].ma50, 4) if data["QQQ"].ma50 is not None else "",
        "QQQ_200MA": round(data["QQQ"].ma200, 4) if data["QQQ"].ma200 is not None else "",
        "SOXX_Close": data["SOXX"].close,
        "NVDA_Close": data["NVDA"].close,
        "MSFT_Close": data["MSFT"].close,
        "TSM_Close": data["TSM"].close,
        "VIX": data["VIX"].close,
        "US10Y": data["US10Y"].close,
        "US30Y": data["US30Y"].close,
        "DXY": data["DXY"].close,
        "UKOIL": data["UKOIL"].close,
        "QQQE": data["QQQE"].close,
        "RSP": data["RSP"].close,
        "SPY": data["SPY"].close,
        "IWM": data["IWM"].close,
        "HYG": data["HYG"].close,
        "TLT": data["TLT"].close,
        "XLE": data["XLE"].close,
        "XLU": data["XLU"].close,
        "NDX50R_Computed": ndx_breadth.get("NDX50R_Computed", ""),
        "NDX_Advancers": ndx_breadth.get("NDX_Advancers", ""),
        "NDX_Decliners": ndx_breadth.get("NDX_Decliners", ""),
        "NDX_AD_Daily": ndx_breadth.get("NDX_AD_Daily", ""),
        "NDX_AD_Line_Computed": ndx_breadth.get("NDX_AD_Line_Computed", ""),
        "NDX_Breadth_Universe": ndx_breadth.get("NDX_Breadth_Universe", ""),
        "NDX_Breadth_Missing": ndx_breadth.get("NDX_Breadth_Missing", ""),
        "QQQ_Source_Date": data["QQQ"].date,
        "VIX_Source_Date": data["VIX"].date,
        "US10Y_Source_Date": data["US10Y"].date,
        "US30Y_Source_Date": data["US30Y"].date,
        "DXY_Source_Date": data["DXY"].date,
        "UKOIL_Source_Date": data["UKOIL"].date,
        "TSM_Source_Date": data["TSM"].date,
        "QQQ_Volume": qqq.volume,
        "QQQ_Vol_20D_Avg": round(qqq.vol20, 0) if qqq.vol20 else "",
        "QQQ_%": qqq_pct,
        "SOXX_%": soxx_pct,
        "NVDA_%": data["NVDA"].pct_change,
        "MSFT_%": data["MSFT"].pct_change,
        "TSM_%": data["TSM"].pct_change,
        "VIX_%": data["VIX"].pct_change,
        "US10Y_%": data["US10Y"].pct_change,
        "US30Y_%": data["US30Y"].pct_change,
        "DXY_%": data["DXY"].pct_change,
        "UKOIL_%": data["UKOIL"].pct_change,
        "QQQE_%": data["QQQE"].pct_change,
        "RSP_%": data["RSP"].pct_change,
        "SPY_%": data["SPY"].pct_change,
        "IWM_%": data["IWM"].pct_change,
        "HYG_%": data["HYG"].pct_change,
        "TLT_%": data["TLT"].pct_change,
        "XLE_%": data["XLE"].pct_change,
        "XLU_%": data["XLU"].pct_change,
        "NDX_AD_Line_%": ndx_breadth.get("NDX_AD_Line_%", ""),
        "QQQE_vs_QQQ_%": qqqe_vs_qqq,
        "RSP_vs_SPY_%": rsp_vs_spy,
        "IWM_vs_SPY_%": iwm_vs_spy,
        "HYG_vs_TLT_%": hyg_vs_tlt,
        "XLE_vs_QQQ_%": xle_vs_qqq,
        "XLU_vs_QQQ_%": xlu_vs_qqq,
        "Semi_Leadership_%": semi_leadership,
        "EqualWeight_Breadth_%": qqqe_vs_qqq,
        "NDX50R_Breadth_Score": score_ndx50r(optional_float(ndx_breadth.get("NDX50R_Computed")))[0],
        "Breadth_Health": score_ndx50r(optional_float(ndx_breadth.get("NDX50R_Computed")))[1],
        "AD_Alert": ad_line_alert(qqq_pct, optional_float(ndx_breadth.get("NDX_AD_Daily"))),
        "QQQ_Vol_Ratio": vol_ratio,
        "AI_ROI_Risk_Manual_Score": "",
        "AI_ROI_Risk_Note": "",
        "Gamma_Risk_Manual_Score": "",
        "Gamma_Risk_Note": "",
        "Narrative_Risk_Note": "",
    }

    # V20.2 strict rule: NDX50R_Computed missing is not neutral. It makes the row PARTIAL/WARNING.
    mark_ndx50r_missing(row, warnings)

    stale_warnings = detect_stale_macro(previous_rows, row, n=3)
    warnings.extend(stale_warnings)
    if qqq_stale:
        row["Data_Status"] = "STALE"
    else:
        row["Data_Status"] = "OK" if not warnings else "WARNING"
    # Soft date notes are informative only; they do not block scoring.
    row["Warnings"] = "; ".join(warnings + soft_date_notes)

    log("Fetched market snapshot:")
    for key in ["QQQ", "SOXX", "NVDA", "MSFT", "TSM", "VIX", "US10Y", "US30Y", "DXY", "UKOIL", "QQQE", "RSP", "SPY", "IWM", "HYG", "TLT", "XLE", "XLU"]:
        if key not in data:
            continue
        snap = data[key]
        pct_txt = "N/A" if snap.pct_change is None else f"{snap.pct_change:.4%}"
        log(f"  {key:<6} date={snap.date} close={snap.close:.4f} prev={snap.prev_close:.4f} pct={pct_txt}")
    log(f"  QQQ volume={qqq.volume:,} vol20={(qqq.vol20 or 0):,.0f} vol_ratio={vol_ratio if vol_ratio == '' else round(vol_ratio, 3)}")
    if warnings:
        log("Warnings: " + "; ".join(warnings))
    return row, updated_cache_rows

def append_or_update(ws, row: Dict[str, Any], headers: List[str]) -> int:
    all_rows = ws.get_all_values()
    target_date = str(row["Date"])
    values = [clean_json_value(row.get(h, "")) for h in headers]
    end_col = col_to_letter(len(headers))

    for idx, r in enumerate(all_rows[1:], start=2):
        if r and r[0] == target_date:
            ws.update(f"A{idx}:{end_col}{idx}", [values], value_input_option="USER_ENTERED")
            log(f"Updated existing Daily_Tracker row for {target_date} at row {idx}")
            return idx
    ws.append_row(values, value_input_option="USER_ENTERED")
    log(f"Appended new Daily_Tracker row for {target_date}")
    return len(all_rows) + 1


def add_reason(reasons: List[Dict[str, Any]], date: str, category: str, signal: str, impact: int, severity: str, reason: str, raw: Any, status: str) -> None:
    reasons.append({
        "Date": date,
        "Category": category,
        "Signal": signal,
        "Score_Impact": impact,
        "Severity": severity,
        "Reason": reason,
        "Raw_Value": raw,
        "Data_Status": status,
    })


def validate_scoring_row(r: Dict[str, Any]) -> None:
    missing = [field for field in SCORING_REQUIRED_FIELDS if field not in r or is_blank(r.get(field))]
    if missing:
        raise ValueError(f"Missing required scoring fields for {r.get('Date', '<unknown date>')}: {missing}")


def score_row(r: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[str], List[str]]:
    status = str(r.get("Data_Status", "OK"))
    if status != "OK":
        raise ValueError("score_row called on non-OK row. V16 forbids scoring invalid data.")
    validate_scoring_row(r)

    date = str(r["Date"])
    qqq = required_float(r, "QQQ_%")
    soxx = required_float(r, "SOXX_%")
    nvda = required_float(r, "NVDA_%")
    msft = required_float(r, "MSFT_%")
    vix = required_float(r, "VIX_%")
    us10y = required_float(r, "US10Y_%")
    us30y = required_float(r, "US30Y_%")
    us30y_level = required_float(r, "US30Y")
    prev_us30y_level = us30y_level / (1.0 + us30y) if (1.0 + us30y) != 0 else us30y_level
    us30y_bps_1d = (us30y_level - prev_us30y_level) * 100.0
    dxy = required_float(r, "DXY_%")
    oil = required_float(r, "UKOIL_%")
    qqqe_vs_qqq = required_float(r, "QQQE_vs_QQQ_%")
    ndx50r_value = normalize_ratio(optional_float(r.get("NDX50R_Computed")))
    ndx50r_score, breadth_health = score_ndx50r(ndx50r_value)
    ad_alert_value = str(r.get("AD_Alert", "") or ad_line_alert(qqq, optional_float(r.get("NDX_AD_Daily"))))
    rsp_vs_spy = required_float(r, "RSP_vs_SPY_%")
    iwm_vs_spy = required_float(r, "IWM_vs_SPY_%")
    hyg_vs_tlt = required_float(r, "HYG_vs_TLT_%")
    vol_ratio = required_float(r, "QQQ_Vol_Ratio")
    semi_leadership = soxx - qqq
    tsm = optional_float(r.get("TSM_%"))
    xle = optional_float(r.get("XLE_%"))
    xlu = optional_float(r.get("XLU_%"))
    ai_roi_manual = clamp_int(optional_float(r.get("AI_ROI_Risk_Manual_Score")), -5, 3, 0)
    gamma_manual = clamp_int(optional_float(r.get("Gamma_Risk_Manual_Score")), -1, 0, 0)
    ai_roi_note = str(r.get("AI_ROI_Risk_Note", "") or "")
    gamma_note = str(r.get("Gamma_Risk_Note", "") or "")
    narrative_note = str(r.get("Narrative_Risk_Note", "") or "")

    reasons: List[Dict[str, Any]] = []
    positive_signals: List[str] = []
    negative_signals: List[str] = []

    def pos(category: str, signal: str, impact: int, reason: str, raw: str) -> int:
        add_reason(reasons, date, category, signal, impact, "positive", reason, raw, status)
        positive_signals.append(f"{signal}: {raw}")
        return impact

    def neg(category: str, signal: str, impact: int, reason: str, raw: str) -> int:
        add_reason(reasons, date, category, signal, impact, "negative", reason, raw, status)
        negative_signals.append(f"{signal}: {raw}")
        return impact

    def warn(category: str, signal: str, impact: int, reason: str, raw: str) -> int:
        add_reason(reasons, date, category, signal, impact, "warning", reason, raw, status)
        negative_signals.append(f"{signal}: {raw}")
        return impact

    # Macro pressure: explicit and auditable.
    macro_pressure = 0
    liquidity = 0
    if dxy > 0.003:
        macro_pressure -= 1; liquidity += neg("Macro", "DXY rising", -1, "Dollar strength can tighten global liquidity.", fmt_pct(dxy))
    elif dxy < -0.003:
        macro_pressure += 1; liquidity += pos("Macro", "DXY falling", 1, "Dollar pressure easing supports risk assets.", fmt_pct(dxy))

    if oil > 0.04:
        macro_pressure -= 2; liquidity += neg("Macro", "Oil shock", -2, "Oil surge adds inflation and margin pressure.", fmt_pct(oil))
    elif oil > 0.02:
        macro_pressure -= 1; liquidity += neg("Macro", "Oil pressure", -1, "Oil rise adds inflation pressure.", fmt_pct(oil))
    elif oil < -0.02:
        macro_pressure += 1; liquidity += pos("Macro", "Oil easing", 1, "Oil decline reduces inflation pressure.", fmt_pct(oil))

    if vix > 0.02:
        macro_pressure -= 1; liquidity += neg("Macro", "VIX rising", -1, "Volatility expansion signals risk-off pressure.", fmt_pct(vix))
    elif vix < -0.02:
        macro_pressure += 1; liquidity += pos("Macro", "VIX falling", 1, "Volatility compression supports risk-taking.", fmt_pct(vix))

    rate = 0
    if us10y > 0.015:
        macro_pressure -= 2; rate += neg("Rates", "US10Y rising sharply", -2, "Higher yields pressure long-duration growth assets.", fmt_pct(us10y))
    elif us10y > 0.008:
        macro_pressure -= 1; rate += neg("Rates", "US10Y rising", -1, "Higher yields pressure growth-stock valuation.", fmt_pct(us10y))
    elif us10y < -0.008:
        macro_pressure += 1; rate += pos("Rates", "US10Y falling", 1, "Lower yields reduce discount-rate pressure on growth stocks.", fmt_pct(us10y))

    # V19: US30Y is the long-duration valuation killer. Use both level and speed.
    if us30y_level >= 5.25:
        macro_pressure -= 2; rate += neg("Rates", "US30Y crisis level", -2, "30Y yield above 5.25% creates severe valuation compression risk.", f"{us30y_level:.3f}%")
    elif us30y_level >= 5.15:
        macro_pressure -= 1; rate += neg("Rates", "US30Y red-zone level", -1, "30Y yield above 5.15% pressures long-duration QQQ valuation.", f"{us30y_level:.3f}%")
    elif us30y_level >= 5.00:
        macro_pressure -= 1; rate += neg("Rates", "US30Y above 5% Maginot line", -1, "30Y yield above 5% is a high-valuation warning zone.", f"{us30y_level:.3f}%")
    if us30y_bps_1d >= 12:
        macro_pressure -= 2; rate += neg("Rates", "US30Y fast 1D jump", -2, "Long-end yield rose more than 12 bps in one day: duration shock.", f"{us30y_bps_1d:.1f} bps")
    elif us30y_bps_1d >= 8:
        macro_pressure -= 1; rate += neg("Rates", "US30Y 1D pressure", -1, "Long-end yield rose more than 8 bps in one day.", f"{us30y_bps_1d:.1f} bps")

    rotation = 0
    if semi_leadership > 0.01:
        rotation += pos("Rotation", "Strong SOXX leadership", 2, "Semiconductors are strongly leading QQQ.", f"SOXX {fmt_pct(soxx)} vs QQQ {fmt_pct(qqq)}, delta {fmt_pct(semi_leadership)}")
    elif semi_leadership > 0.003:
        rotation += pos("Rotation", "SOXX leadership", 1, "Semiconductors are modestly leading QQQ.", f"SOXX {fmt_pct(soxx)} vs QQQ {fmt_pct(qqq)}, delta {fmt_pct(semi_leadership)}")
    elif semi_leadership < -0.015:
        rotation += neg("Rotation", "Serious semiconductor deterioration", -2, "SOXX materially lagged QQQ; AI/semiconductor leadership is broken.", f"SOXX {fmt_pct(soxx)} vs QQQ {fmt_pct(qqq)}, delta {fmt_pct(semi_leadership)}")
    elif semi_leadership < -0.005:
        rotation += neg("Rotation", "SOXX lagging", -1, "QQQ lacks semiconductor leadership.", f"SOXX {fmt_pct(soxx)} vs QQQ {fmt_pct(qqq)}, delta {fmt_pct(semi_leadership)}")

    leadership = 0
    if nvda > qqq + 0.005:
        leadership += pos("Leadership", "NVDA leading", 1, "Core AI leader is outperforming QQQ.", f"NVDA {fmt_pct(nvda)} vs QQQ {fmt_pct(qqq)}")
    elif nvda < qqq - 0.01:
        leadership += neg("Leadership", "NVDA materially lagging", -1, "AI leadership is weakening.", f"NVDA {fmt_pct(nvda)} vs QQQ {fmt_pct(qqq)}, spread {fmt_pct(nvda-qqq)}")

    if msft > qqq + 0.005:
        leadership += pos("Leadership", "MSFT leading", 1, "Mega-cap quality leadership supports QQQ.", f"MSFT {fmt_pct(msft)} vs QQQ {fmt_pct(qqq)}")
    elif msft < -0.01:
        leadership += neg("Leadership", "MSFT weak", -1, "MSFT is directly negative and not confirming QQQ strength.", fmt_pct(msft))
    elif msft < qqq - 0.01:
        leadership += neg("Leadership", "MSFT materially lagging", -1, "Mega-cap quality leadership is not confirming.", f"spread {fmt_pct(msft-qqq)}")

    breadth = 0
    positive_count = sum([qqq > 0, soxx > 0, nvda > 0, msft > 0])
    if positive_count == 4:
        breadth += pos("Breadth", "Core names all positive", 2, "QQQ, SOXX, NVDA, and MSFT are all positive.", str(positive_count))
    elif positive_count >= 3:
        breadth += pos("Breadth", "Most core names positive", 1, "Most core technology exposures are participating.", str(positive_count))
    elif positive_count <= 1:
        breadth += neg("Breadth", "Poor core participation", -1, "Too few key exposures are positive.", str(positive_count))

    if qqqe_vs_qqq > 0.003:
        breadth += pos("Breadth", "QQQE beats QQQ", 1, "Equal-weight Nasdaq 100 is outperforming cap-weight QQQ.", fmt_pct(qqqe_vs_qqq))
    elif qqqe_vs_qqq < -0.005:
        breadth += neg("Breadth", "QQQE lags QQQ", -1, "Mega-cap concentration risk: equal-weight Nasdaq lags.", fmt_pct(qqqe_vs_qqq))

    if rsp_vs_spy > 0.003:
        breadth += pos("Breadth", "RSP beats SPY", 1, "Equal-weight S&P participation is improving.", fmt_pct(rsp_vs_spy))
    elif rsp_vs_spy < -0.005:
        breadth += neg("Breadth", "RSP lags SPY", -1, "Broad participation lags cap-weight index.", fmt_pct(rsp_vs_spy))

    if ndx50r_score > 0:
        breadth += pos("Breadth", "NDX50R breadth healthy", ndx50r_score, "Nasdaq stocks above 50DMA confirm participation.", breadth_health)
    elif ndx50r_score < 0:
        breadth += neg("Breadth", "NDX50R breadth weak", ndx50r_score, "Nasdaq stocks above 50DMA show narrow/weak participation.", breadth_health)
    else:
        add_reason(reasons, date, "Breadth", "NDX50R_Computed neutral", 0, "neutral", "NDX50R_Computed is 35–50%; no score impact.", breadth_health, status)
    if ad_alert_value and ad_alert_value != "No A/D divergence":
        add_reason(reasons, date, "Breadth", "A/D Line alert", 0, "warning", "A/D Line is warning-only in V20 and does not affect score.", ad_alert_value, status)

    positioning = 0
    if qqq > 0 and vol_ratio > 1.1:
        positioning += pos("Positioning", "Up on strong volume", 1, "QQQ gained with above-average volume.", f"{vol_ratio:.2f}x")
    if qqq > 0 and vol_ratio < 0.8:
        positioning += warn("Positioning", "Up on weak volume", -1, "QQQ gained but volume confirmation is weak.", f"{vol_ratio:.2f}x")
    if qqq < 0 and vol_ratio > 1.2:
        positioning += neg("Positioning", "Down on heavy volume", -1, "QQQ fell with heavy volume.", f"{vol_ratio:.2f}x")

    credit = 0
    if hyg_vs_tlt > 0.003:
        credit += pos("Credit", "HYG beats TLT", 1, "Credit risk appetite is better than duration safety bid.", fmt_pct(hyg_vs_tlt))
    elif hyg_vs_tlt < -0.005:
        credit += neg("Credit", "HYG lags TLT", -1, "Credit risk appetite is weakening.", fmt_pct(hyg_vs_tlt))

    if iwm_vs_spy > 0.003:
        credit += pos("Risk Appetite", "IWM beats SPY", 1, "Small caps outperforming suggests risk appetite expansion.", fmt_pct(iwm_vs_spy))
    elif iwm_vs_spy < -0.006:
        credit += neg("Risk Appetite", "IWM lags SPY", -1, "Small-cap lag shows risk appetite is narrow.", fmt_pct(iwm_vs_spy))

    # V19 overlay modules. Narrative risk is logged but does not score unless it is confirmed by price or manual/fundamental input.
    macro_valuation_pressure = 0
    v19_notes: List[str] = []
    if us10y > 0.012:
        macro_valuation_pressure -= 1; v19_notes.append(f"US10Y valuation pressure {fmt_pct(us10y)}")
    if us30y_level >= 5.25:
        macro_valuation_pressure -= 3; v19_notes.append(f"US30Y crisis level {us30y_level:.3f}%")
    elif us30y_level >= 5.15:
        macro_valuation_pressure -= 2; v19_notes.append(f"US30Y red-zone level {us30y_level:.3f}%")
    elif us30y_level >= 5.00:
        macro_valuation_pressure -= 1; v19_notes.append(f"US30Y above 5% warning line {us30y_level:.3f}%")
    if us30y_bps_1d >= 12:
        macro_valuation_pressure -= 2; v19_notes.append(f"US30Y 1D jump {us30y_bps_1d:.1f} bps")
    elif us30y_bps_1d >= 8:
        macro_valuation_pressure -= 1; v19_notes.append(f"US30Y 1D pressure {us30y_bps_1d:.1f} bps")
    if dxy > 0.003:
        macro_valuation_pressure -= 1; v19_notes.append(f"DXY valuation pressure {fmt_pct(dxy)}")
    if (us10y > 0 or us30y > 0) and dxy > 0 and qqq < 0:
        macro_valuation_pressure -= 2; v19_notes.append("US10Y/US30Y + DXY up while QQQ down: valuation compression risk")
    if vix > 0 and qqq < 0:
        macro_valuation_pressure -= 1; v19_notes.append("VIX up while QQQ down: risk premium expanding")
    if us10y < 0 and us30y < 0 and dxy < 0 and qqq > 0:
        macro_valuation_pressure += 1; v19_notes.append("US10Y + US30Y + DXY easing while QQQ up")
    macro_valuation_pressure = max(-6, min(1, macro_valuation_pressure))

    # Dedicated long-end shock: separated from generic macro pressure so it can cap buy signals.
    long_end_yield_shock = 0
    if us30y_level >= 5.25:
        long_end_yield_shock -= 3
    elif us30y_level >= 5.15:
        long_end_yield_shock -= 2
    elif us30y_level >= 5.00:
        long_end_yield_shock -= 1
    if us30y_bps_1d >= 12:
        long_end_yield_shock -= 2
    elif us30y_bps_1d >= 8:
        long_end_yield_shock -= 1
    long_end_yield_shock = max(-4, min(0, long_end_yield_shock))
    if us30y_level >= 5.25 or long_end_yield_shock <= -3:
        rate_regime = "CRISIS"
    elif us30y_level >= 5.15:
        rate_regime = "RED"
    elif us30y_level >= 5.00:
        rate_regime = "ORANGE"
    elif us30y_level >= 4.75:
        rate_regime = "YELLOW"
    else:
        rate_regime = "NORMAL"
    fragile_risk_on = bool(qqq > 0 and (soxx > 0 or nvda > 0) and rate_regime in ["ORANGE", "RED", "CRISIS"])
    if fragile_risk_on:
        v19_notes.append("Fragile risk-on: price positive while US30Y is in pressure zone")

    if macro_valuation_pressure != 0:
        add_reason(reasons, date, "V19 Macro Valuation", "Macro Valuation Pressure", macro_valuation_pressure, "negative" if macro_valuation_pressure < 0 else "positive", "High-duration QQQ valuation pressure from US10Y/US30Y/DXY/VIX mix.", macro_valuation_pressure, status)
    if long_end_yield_shock != 0:
        add_reason(reasons, date, "V19 Rates", "Long-End Yield Shock", long_end_yield_shock, "negative", "US30Y level/speed is stressing long-duration QQQ valuation.", f"US30Y {us30y_level:.3f}%, 1D {us30y_bps_1d:.1f} bps", status)

    ai_chain_stress = 0
    soxx_weak = soxx < qqq - 0.010
    nvda_weak = nvda < qqq - 0.015
    tsm_weak = bool(tsm is not None and tsm < qqq - 0.010)
    if soxx_weak:
        ai_chain_stress -= 1; v19_notes.append(f"SOXX weak vs QQQ: {fmt_pct(soxx-qqq)}")
    if nvda_weak:
        ai_chain_stress -= 1; v19_notes.append(f"NVDA weak vs QQQ: {fmt_pct(nvda-qqq)}")
    if tsm_weak and tsm is not None:
        ai_chain_stress -= 1; v19_notes.append(f"TSM weak vs QQQ: {fmt_pct(tsm-qqq)}")
    if soxx_weak and nvda_weak:
        ai_chain_stress -= 1; v19_notes.append("SOXX + NVDA both weak: AI leadership stress")
    if soxx_weak and nvda_weak and tsm_weak:
        ai_chain_stress -= 1; v19_notes.append("SOXX + NVDA + TSM all weak: full AI chain stress")
    if qqq > 0 and (soxx < 0 or nvda < 0):
        ai_chain_stress -= 1; v19_notes.append("QQQ up but SOXX/NVDA not confirming: fake QQQ strength risk")
    if qqq > 0 and semi_leadership < -0.015 and ndx50r_value is not None and ndx50r_value < 0.35:
        v19_notes.append("V20 fake-bull high risk: QQQ positive, SOXX materially lags, NDX50R_Computed below 35%")
        add_reason(reasons, date, "V20 Breadth", "Fake bull high risk", 0, "warning", "Warning-only confirmation of narrow market; NDX50R_Computed already affects Breadth_Score.", f"NDX50R_Computed {ndx50r_value*100:.1f}%, semi delta {fmt_pct(semi_leadership)}", status)
    if soxx > qqq + 0.010 and nvda > qqq:
        ai_chain_stress += 2; v19_notes.append("SOXX + NVDA confirm AI leadership")
    ai_chain_stress = max(-5, min(2, ai_chain_stress))
    ai_chain_weak_day = ai_chain_stress <= -2
    if ai_chain_stress != 0:
        add_reason(reasons, date, "V19 AI Chain", "AI Chain Stress", ai_chain_stress, "negative" if ai_chain_stress < 0 else "positive", "SOXX / NVDA / TSM relative strength versus QQQ.", ai_chain_stress, status)

    ai_roi_risk = ai_roi_manual
    if narrative_note:
        add_reason(reasons, date, "Narrative", "Narrative risk logged only", 0, "warning", "Narrative recorded but not scored until price or fundamentals confirm it.", narrative_note, status)
    if ai_roi_note or ai_roi_risk != 0:
        add_reason(reasons, date, "V19 AI ROI", "AI ROI / Depreciation Risk", ai_roi_risk, "negative" if ai_roi_risk < 0 else "positive" if ai_roi_risk > 0 else "warning", "Manual/fundamental AI ROI input. Use only on earnings/guidance confirmation, not pure headlines.", ai_roi_note or ai_roi_risk, status)

    gamma_volatility = gamma_manual
    if qqq < 0 and vol_ratio > 1.2 and vix > 0:
        gamma_volatility = min(gamma_volatility, -1); v19_notes.append("QQQ down on heavy volume with VIX up: liquidation/gamma-style volatility risk")
    gamma_volatility = max(-1, min(0, gamma_volatility))
    if gamma_volatility != 0 or gamma_note:
        add_reason(reasons, date, "V19 Gamma", "Gamma Volatility Warning", gamma_volatility, "warning", "Gamma is a short-term volatility amplifier only; capped at -1.", gamma_note or gamma_volatility, status)

    energy_ai_rotation = 0
    if oil > 0.02 and (us10y > 0 or us30y > 0) and qqq < 0:
        energy_ai_rotation -= 1; v19_notes.append("Oil/rate pressure hurts QQQ despite energy strength")
    elif xle is not None and xlu is not None and xle > qqq and xlu > qqq and not ai_chain_weak_day:
        energy_ai_rotation += 1; v19_notes.append("Energy/utilities stronger without AI-chain breakdown: AI infrastructure rotation")
    elif (xle is not None and xle > qqq) and ai_chain_weak_day:
        v19_notes.append("Energy strong but AI chain weak: rotation is not enough to confirm QQQ")
    energy_ai_rotation = max(-1, min(1, energy_ai_rotation))
    if energy_ai_rotation != 0:
        add_reason(reasons, date, "V19 Energy", "Energy / AI Infrastructure Rotation", energy_ai_rotation, "negative" if energy_ai_rotation < 0 else "positive", "Energy/power infrastructure is a secondary signal, not a core QQQ buy signal.", energy_ai_rotation, status)

    total = liquidity + rate + rotation + leadership + breadth + positioning + credit
    v19_adjusted = total + macro_valuation_pressure + long_end_yield_shock + ai_chain_stress + ai_roi_risk + gamma_volatility + energy_ai_rotation
    # Macro cap: when US30Y is red/orange, prevent Strong Buy mechanics from overriding valuation stress.
    if rate_regime in ["RED", "CRISIS"] and v19_adjusted > 4:
        v19_notes.append("Macro cap: US30Y red-zone prevents strong-buy score")
        v19_adjusted = 4
    elif rate_regime == "ORANGE" and v19_adjusted > 6:
        v19_notes.append("Macro cap: US30Y orange-zone caps aggressive risk-on")
        v19_adjusted = 6
    scored = {
        "Date": date,
        "Valid_For_Signal": True,
        "Liquidity_Score": liquidity,
        "Rate_Score": rate,
        "Rotation_Score": rotation,
        "Leadership_Score": leadership,
        "Breadth_Score": breadth,
        "NDX50R_Breadth_Score": ndx50r_score,
        "Breadth_Health": breadth_health,
        "AD_Alert": ad_alert_value,
        "NDX50R_Computed": ndx50r_value if ndx50r_value is not None else "",
        "NDX_Advancers": r.get("NDX_Advancers", ""),
        "NDX_Decliners": r.get("NDX_Decliners", ""),
        "NDX_AD_Daily": r.get("NDX_AD_Daily", ""),
        "NDX_AD_Line_Computed": r.get("NDX_AD_Line_Computed", ""),
        "NDX_Breadth_Universe": r.get("NDX_Breadth_Universe", ""),
        "Positioning_Score": positioning,
        "Credit_Risk_Score": credit,
        "Macro_Pressure_Score": macro_pressure,
        "Total_Dynamics_Score": total,
        "Macro_Valuation_Pressure": macro_valuation_pressure,
        "Long_End_Yield_Shock": long_end_yield_shock,
        "Rate_Regime": rate_regime,
        "Fragile_Risk_On": fragile_risk_on,
        "AI_Chain_Stress": ai_chain_stress,
        "AI_ROI_Risk": ai_roi_risk,
        "Gamma_Volatility_Warning": gamma_volatility,
        "Energy_AI_Rotation": energy_ai_rotation,
        "V20_Adjusted_Score": v19_adjusted,
        "AI_Chain_Weak_Day": ai_chain_weak_day,
        "V20_Reason_Log": " | ".join(v19_notes[:10]),
        "Semi_Leadership_%": semi_leadership,
        "Strong_Day": total >= 7,
        "Data_Status": status,
        "Reason_Summary": " | ".join([x["Signal"] for x in reasons[:8]]),
    }
    return scored, reasons, negative_signals, positive_signals


def classify_single(total: float, confirmed: bool) -> Tuple[str, str, str]:
    if confirmed:
        return "Risk-On Confirmed", "可考慮分批加碼", "最近三個 clean OK 日全部達強勢門檻，3D risk-on confirmed。"
    if total >= 8:
        return "Risk-On Candidate", "可小部位試單", "單日條件強，但尚未完成 clean 3D 確認。"
    if total >= 5:
        return "Transition Improving", "觀察，不追高", "市場結構改善，但可能只是反彈。"
    if total >= 2:
        return "Weak Rebound", "等待確認", "資金動力不足。"
    if total >= -1:
        return "Neutral / Choppy", "不主動進場", "市場容易震盪洗盤。"
    return "Risk-Off", "降低風險", "資金動力偏弱，QQQ 上漲也要懷疑是假突破。"


def state_label(score: Optional[float]) -> str:
    if score is None:
        return "N/A"
    if score >= 7:
        return "Strong"
    if score >= 5:
        return "Improving"
    if score >= 2:
        return "Weak Positive"
    if score >= -1:
        return "Neutral"
    return "Weak / Risk-Off"


def transition_probability(score1: float, score3: float, score10: Optional[float], score20: Optional[float], strong3: int) -> int:
    raw = 50
    raw += max(min(score1, 10), -5) * 2.0
    raw += max(min(score3, 10), -5) * 3.0
    if score10 is not None:
        raw += max(min(score10, 10), -5) * 1.2
    if score20 is not None:
        raw += max(min(score20, 10), -5) * 0.6
    raw += (strong3 - 1) * 7
    return int(max(0, min(100, round(raw))))


def partial_breadth_rows(date: str, warnings: str, clean_scores: List[float]) -> Tuple[Dict[str, Any], Dict[str, Any], List[Dict[str, Any]]]:
    """Return dashboard rows for missing NDX50R_Computed. This is stricter than neutral.

    V20.2 does not treat unavailable breadth as score 0. The row is recorded,
    but final action is WAIT / DATA INCOMPLETE and it must not enter 3D/composite.
    """
    recent3 = clean_scores[-3:]
    avg3 = sum(recent3) / len(recent3) if recent3 else ""
    reason = warnings or "NDX50R_Computed unavailable; computed breadth excluded from scoring"
    agent = {h: "" for h in AGENT_HEADERS}
    agent.update({
        "Date": date,
        "Valid_For_Signal": False,
        "NDX50R_Breadth_Score": "",
        "Breadth_Health": "NDX50R_Computed unavailable; computed breadth excluded from scoring",
        "AD_Alert": "A/D unavailable; no divergence check",
        "Strong_Day": False,
        "Clean_Valid_Count_3D": len(recent3),
        "3D_Avg_Score": round(avg3, 2) if isinstance(avg3, float) else "",
        "Strong_Count_3D": sum(1 for x in recent3 if x >= 7),
        "3_of_3_Confirmed": False,
        "Regime": "PARTIAL / DATA INCOMPLETE",
        "Action": "WAIT / DATA INCOMPLETE",
        "Agent_Comment": "NDX50R_Computed is missing. V20.2 excludes breadth from scoring and blocks final trade action.",
        "V20_State": "PARTIAL / DATA INCOMPLETE",
        "V20_Action": "WAIT / DATA INCOMPLETE",
        "Data_Status": "PARTIAL",
        "Reason_Summary": reason,
    })
    multi = {h: "" for h in MULTI_HEADERS}
    multi.update({
        "Date": date,
        "Valid_For_Signal": False,
        "Clean_Valid_Count_3D": len(recent3),
        "Strong_Count_3D": sum(1 for x in recent3 if x >= 7),
        "Breadth_Health": "NDX50R_Computed unavailable; computed breadth excluded from scoring",
        "AD_Alert": "A/D unavailable; no divergence check",
        "Regime_Composite": "PARTIAL / DATA INCOMPLETE",
        "Action_Composite": "WAIT / DATA INCOMPLETE",
        "V20_State": "PARTIAL / DATA INCOMPLETE",
        "V20_Action": "WAIT / DATA INCOMPLETE",
        "Main_Negative_Signals": reason,
        "Main_Positive_Signals": "",
        "Confidence": "PARTIAL: NDX50R_Computed missing; breadth not scored.",
        "Commentary": "NDX50R_Computed missing. This row did not enter Score_1D, Score_3D, 10D/20D, transition probability, or V20 adjusted score.",
        "Data_Status": "PARTIAL",
    })
    reason_row = [{
        "Date": date,
        "Category": "Data Quality",
        "Signal": "NDX50R_Computed missing; breadth excluded",
        "Score_Impact": 0,
        "Severity": "warning",
        "Reason": reason,
        "Raw_Value": "NDX50R_Computed missing",
        "Data_Status": "PARTIAL",
    }]
    return agent, multi, reason_row


def invalid_rows(date: str, data_status: str, warnings: str, clean_scores: List[float]) -> Tuple[Dict[str, Any], Dict[str, Any], List[Dict[str, Any]]]:
    recent3 = clean_scores[-3:]
    avg3 = sum(recent3) / len(recent3) if recent3 else ""
    reason = warnings or f"Data_Status={data_status}; row is recorded but not scored."
    agent = {h: "" for h in AGENT_HEADERS}
    agent.update({
        "Date": date,
        "Valid_For_Signal": False,
        "Strong_Day": False,
        "Clean_Valid_Count_3D": len(recent3),
        "3D_Avg_Score": round(avg3, 2) if isinstance(avg3, float) else "",
        "Strong_Count_3D": sum(1 for x in recent3 if x >= 7),
        "3_of_3_Confirmed": False,
        "Regime": "Data Invalid / No Signal",
        "Action": "不交易、不確認",
        "Agent_Comment": "Data_Status is not OK. V20 records this row but excludes it from score, 3D, composite, and AI-chain confirmation.",
        "Data_Status": data_status,
        "Reason_Summary": reason,
    })
    multi = {h: "" for h in MULTI_HEADERS}
    multi.update({
        "Date": date,
        "Valid_For_Signal": False,
        "Clean_Valid_Count_3D": len(recent3),
        "Strong_Count_3D": sum(1 for x in recent3 if x >= 7),
        "Regime_Composite": "Data Invalid / No Signal",
        "Action_Composite": "不交易、不確認",
        "Main_Negative_Signals": reason,
        "Main_Positive_Signals": "",
        "Confidence": "Invalid data; excluded from scoring.",
        "Commentary": "Data_Status != OK. This row did not enter Score_1D, Score_3D, 10D/20D, or transition probability.",
        "Data_Status": data_status,
    })
    reason_row = [{
        "Date": date,
        "Category": "Data Quality",
        "Signal": "Data invalid; score skipped",
        "Score_Impact": 0,
        "Severity": "warning",
        "Reason": reason,
        "Raw_Value": data_status,
        "Data_Status": data_status,
    }]
    return agent, multi, reason_row


def compute_tables(daily_rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    agent_rows: List[Dict[str, Any]] = []
    multi_rows: List[Dict[str, Any]] = []
    reason_rows: List[Dict[str, Any]] = []
    clean_scores: List[float] = []
    clean_records: List[Dict[str, Any]] = []
    ai_chain_flags: List[bool] = []

    for i, r in enumerate(daily_rows):
        date = str(r.get("Date", ""))
        status = str(r.get("Data_Status", "OK") or "OK")
        warnings = str(r.get("Warnings", "") or "")

        if i == 0 or status == "BASELINE_ONLY":
            baseline_agent = {h: "" for h in AGENT_HEADERS}
            baseline_agent.update({
                "Date": date,
                "Valid_For_Signal": False,
                "Regime": "Baseline Only",
                "Action": "不運算",
                "Agent_Comment": "第一筆資料只作為前日基準，不做 regime 判斷。",
                "Data_Status": status,
                "Reason_Summary": "Baseline row",
            })
            agent_rows.append(baseline_agent)
            baseline_multi = {h: "" for h in MULTI_HEADERS}
            baseline_multi.update({
                "Date": date,
                "Valid_For_Signal": False,
                "Regime_Composite": "Baseline Only",
                "Action_Composite": "不運算",
                "Confidence": "Baseline only",
                "Commentary": "第一筆資料為 baseline。下一個 clean OK 交易日開始才有可用 regime 計算。",
                "Data_Status": status,
            })
            multi_rows.append(baseline_multi)
            continue

        # V20.2 strict guard: missing NDX50R_Computed must not be treated as neutral 0.
        if is_ndx50r_missing(r):
            reason = warnings or "NDX50R_Computed unavailable; computed breadth excluded from scoring"
            agent, multi, rs = partial_breadth_rows(date, reason, clean_scores)
            agent_rows.append(agent)
            multi_rows.append(multi)
            reason_rows.extend(rs)
            continue

        if status != "OK":
            agent, multi, rs = invalid_rows(date, status, warnings, clean_scores)
            agent_rows.append(agent)
            multi_rows.append(multi)
            reason_rows.extend(rs)
            continue

        scored, reasons, negative_signals, positive_signals = score_row(r)
        total = float(scored["Total_Dynamics_Score"])
        v19_adjusted = float(scored.get("V20_Adjusted_Score", total))
        clean_scores.append(total)
        clean_records.append(scored)
        ai_chain_flags.append(bool(scored.get("AI_Chain_Weak_Day", False)))

        recent3 = clean_scores[-3:]
        recent10 = clean_scores[-10:]
        recent20 = clean_scores[-20:]
        recent_ai_chain = ai_chain_flags[-3:]
        ai_chain_weak_3d = sum(1 for x in recent_ai_chain if x)
        avg3 = sum(recent3) / len(recent3)
        avg10 = sum(recent10) / len(recent10) if len(recent10) >= 3 else None
        avg20 = sum(recent20) / len(recent20) if len(recent20) >= 5 else None
        strong3 = sum(1 for x in recent3 if x >= 7)
        strong10 = sum(1 for x in recent10 if x >= 7)
        confirmed = len(recent3) == 3 and avg3 >= 6.5 and strong3 >= 3

        regime, action, comment = classify_single(total, confirmed)
        v19_state, v19_action = classify_v19_state(v19_adjusted, ai_chain_weak_3d, int(scored.get("AI_ROI_Risk", 0) or 0), int(scored.get("Long_End_Yield_Shock", 0) or 0), str(scored.get("Rate_Regime", "")))
        agent = {h: "" for h in AGENT_HEADERS}
        agent.update(scored)
        agent.update({
            "Clean_Valid_Count_3D": len(recent3),
            "3D_Avg_Score": round(avg3, 2),
            "Strong_Count_3D": strong3,
            "AI_Chain_Weak_Count_3D": ai_chain_weak_3d,
            "3_of_3_Confirmed": confirmed,
            "Regime": regime,
            "Action": action,
            "Agent_Comment": comment,
            "V20_State": v19_state,
            "V20_Action": v19_action,
        })
        agent_rows.append(agent)
        reason_rows.extend(reasons)

        prob = transition_probability(total, avg3, avg10, avg20, strong3)
        liquidity_state = state_label(scored["Liquidity_Score"] + scored["Rate_Score"])
        breadth_state = state_label(scored["Breadth_Score"])
        credit_state = state_label(scored["Credit_Risk_Score"])
        momentum_state = state_label(avg3)

        if confirmed and prob >= 75:
            composite = "Risk-On Confirmed"
            composite_action = "分批加碼 / 維持風險資產配置"
        elif prob >= 70:
            composite = "Risk-On Transition"
            composite_action = "可小部位試單，等待延續確認"
        elif prob >= 55:
            composite = "Improving but Unconfirmed"
            composite_action = "觀察，不追高"
        elif prob >= 40:
            composite = "Neutral / Choppy"
            composite_action = "降低主觀交易頻率"
        else:
            composite = "Risk-Off / Deteriorating"
            composite_action = "防守，避免追多"

        if int(scored.get("Long_End_Yield_Shock", 0) or 0) <= -3:
            composite = "Long-End Yield Shock"
            composite_action = "防守；停止追高；降低 QQQ/AI beta，等 US30Y 回落"
        elif bool(scored.get("Fragile_Risk_On", False)):
            composite = "Fragile Risk-On"
            composite_action = "持有可以，不追高；等待 US30Y/半導體確認"
        elif int(scored.get("AI_ROI_Risk", 0) or 0) <= -2:
            composite = "AI ROI Reset Risk"
            composite_action = "中期降低 AI beta；避免把 NVDA 強勢誤判成整體 AI ROI 健康"
        elif ai_chain_weak_3d >= 3:
            composite = "AI Chain Confirmed Weakness"
            composite_action = "減碼 10–20%，等待 SOXX/NVDA/TSM 修復"

        confidence = "High" if len(clean_scores) >= 20 else "Medium" if len(clean_scores) >= 10 else f"Low: only {len(clean_scores)} clean valid day(s); 10D/20D may be unavailable."
        commentary = (
            f"1D={total:.1f}, 3D={avg3:.1f}, "
            f"10D={'N/A' if avg10 is None else round(avg10,1)}, "
            f"20D={'N/A' if avg20 is None else round(avg20,1)}. "
            f"Transition probability={prob}%. "
            f"Momentum={momentum_state}; Breadth={breadth_state}; Liquidity={liquidity_state}; Credit={credit_state}. "
            f"V20 adjusted={v19_adjusted:.1f}; AI chain weak days in clean 3D={ai_chain_weak_3d}. "
            f"Clean 3D count={len(recent3)}; invalid/WARNING rows excluded."
        )
        multi_rows.append({
            "Date": date,
            "Valid_For_Signal": True,
            "Score_1D": total,
            "V20_Adjusted_Score": round(v19_adjusted, 2),
            "Score_3D": round(avg3, 2),
            "Score_10D": "" if avg10 is None else round(avg10, 2),
            "Score_20D": "" if avg20 is None else round(avg20, 2),
            "Clean_Valid_Count_3D": len(recent3),
            "Strong_Count_3D": strong3,
            "Strong_Count_10D": strong10,
            "AI_Chain_Weak_Count_3D": ai_chain_weak_3d,
            "Transition_Probability": prob,
            "Momentum_State": momentum_state,
            "Breadth_State": breadth_state,
            "Breadth_Health": scored.get("Breadth_Health", ""),
            "AD_Alert": scored.get("AD_Alert", ""),
            "Liquidity_State": liquidity_state,
            "Credit_State": credit_state,
            "Macro_Valuation_Pressure": scored.get("Macro_Valuation_Pressure", ""),
            "Long_End_Yield_Shock": scored.get("Long_End_Yield_Shock", ""),
            "Rate_Regime": scored.get("Rate_Regime", ""),
            "Fragile_Risk_On": scored.get("Fragile_Risk_On", ""),
            "AI_Chain_Stress": scored.get("AI_Chain_Stress", ""),
            "AI_ROI_Risk": scored.get("AI_ROI_Risk", ""),
            "Gamma_Volatility_Warning": scored.get("Gamma_Volatility_Warning", ""),
            "Energy_AI_Rotation": scored.get("Energy_AI_Rotation", ""),
            "Regime_Composite": composite,
            "Action_Composite": composite_action,
            "V20_State": v19_state,
            "V20_Action": v19_action,
            "Main_Negative_Signals": " | ".join(negative_signals[:8]),
            "Main_Positive_Signals": " | ".join(positive_signals[:8]),
            "Narrative_Risk_Log": str(r.get("Narrative_Risk_Note", "") or ""),
            "Confidence": confidence,
            "Commentary": commentary,
            "Data_Status": status,
        })

    return agent_rows, multi_rows, reason_rows




def pct_change_from_values(curr: Optional[float], prev: Optional[float]) -> Optional[float]:
    if curr is None or prev is None or prev == 0:
        return None
    return (curr - prev) / prev


def rolling_mean(values: List[Optional[float]], window: int) -> Optional[float]:
    clean = [v for v in values[-window:] if v is not None]
    if len(clean) < window:
        return None
    return sum(clean) / window


def classify_1y_entry(score: Optional[int], confidence: str) -> Tuple[str, str]:
    if score is None:
        return "Invalid / No 1Y Signal", "不交易、不確認"
    if score >= 80:
        return "Strong 1Y Entry Zone", "可分批建立 40–60% 預定部位"
    if score >= 70:
        return "Good 1Y Staggered Entry", "可分批建立 25–40% 預定部位"
    if score >= 50:
        return "Probe Only", "僅允許 10–20% 試單，等待修復確認"
    return "Wait", "等待，不因下跌而買入"


def compute_1y_entry_table(daily_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Build the V17 1-year entry scoring table.

    This is not a trade bot. It evaluates whether the current QQQ setup has a
    favorable 1-year entry profile by combining trend, drawdown, macro pressure,
    semiconductor leadership, breadth, and credit/risk appetite.
    """
    rows: List[Dict[str, Any]] = []
    clean_rows: List[Dict[str, Any]] = []

    def close_at(seq: List[Dict[str, Any]], field: str, idx: int) -> Optional[float]:
        if len(seq) <= idx:
            return None
        return optional_float(seq[-1-idx].get(field))

    for i, r in enumerate(daily_rows):
        date = str(r.get("Date", ""))
        status = str(r.get("Data_Status", "OK") or "OK")
        if i == 0 or status in ("BASELINE_ONLY", "") or status != "OK" or is_ndx50r_missing(r):
            ndx_missing = (i != 0 and status == "OK" and is_ndx50r_missing(r))
            rows.append({
                "Date": date,
                "Valid_For_1Y": False,
                "NDX50R_Breadth_Score": "" if (ndx_missing or i == 0 or status == "BASELINE_ONLY") else r.get("NDX50R_Breadth_Score", ""),
                "Breadth_Health": "NDX50R_Computed unavailable; computed breadth excluded from scoring" if ndx_missing else "",
                "AD_Alert": "A/D unavailable; no divergence check" if ndx_missing else "",
                "Entry_Zone": "PARTIAL / DATA INCOMPLETE" if ndx_missing else ("Baseline Only" if i == 0 or status == "BASELINE_ONLY" else "Data Invalid / No 1Y Signal"),
                "Suggested_Action": "WAIT / DATA INCOMPLETE" if ndx_missing else ("不運算" if i == 0 or status == "BASELINE_ONLY" else "不交易、不確認"),
                "Main_Positive_Factors": "",
                "Main_Negative_Factors": "NDX50R_Computed unavailable; breadth excluded from 1Y scoring" if ndx_missing else ("Baseline row" if i == 0 or status == "BASELINE_ONLY" else (str(r.get("Warnings", "")) or f"Data_Status={status}; excluded from 1Y scoring.")),
                "Confidence": "PARTIAL: NDX50R_Computed missing; 1Y entry score blocked." if ndx_missing else ("Baseline only" if i == 0 or status == "BASELINE_ONLY" else "Invalid data; excluded from 1Y scoring."),
                "Data_Status": "PARTIAL" if ndx_missing else status,
            })
            continue

        # Strict required fields for 1Y scoring. Missing should break, not become zero.
        required = ["QQQ_Close", "VIX", "QQQ_%", "SOXX_%", "NVDA_%", "MSFT_%", "US10Y_%", "US30Y_%", "US30Y", "DXY_%", "UKOIL_%", "QQQE_vs_QQQ_%", "RSP_vs_SPY_%", "IWM_vs_SPY_%", "HYG_vs_TLT_%"]
        missing = [f for f in required if f not in r or is_blank(r.get(f))]
        if missing:
            raise ValueError(f"Missing required 1Y fields for {date}: {missing}")

        clean_rows.append(r)
        qqq_close = required_float(r, "QQQ_Close")
        qqq_values = [optional_float(x.get("QQQ_Close")) for x in clean_rows]
        # V20.5: prefer automatically fetched MA fields from Daily_Tracker,
        # so 50MA/200MA are available immediately after bootstrap instead of waiting
        # for 50/200 clean rows inside the sheet.
        qqq_50ma = optional_float(r.get("QQQ_50MA")) or rolling_mean(qqq_values, 50)
        qqq_200ma = optional_float(r.get("QQQ_200MA")) or rolling_mean(qqq_values, 200)
        prev_50ma = optional_float(clean_rows[-2].get("QQQ_50MA")) if len(clean_rows) > 1 else None
        if prev_50ma is None:
            prev_50ma = rolling_mean(qqq_values[:-1], 50) if len(qqq_values) > 50 else None
        prev_200ma = optional_float(clean_rows[-2].get("QQQ_200MA")) if len(clean_rows) > 1 else None
        if prev_200ma is None:
            prev_200ma = rolling_mean(qqq_values[:-1], 200) if len(qqq_values) > 200 else None
        high_window = [v for v in qqq_values[-252:] if v is not None]
        high = max(high_window) if high_window else qqq_close
        drawdown = (qqq_close / high - 1.0) if high else 0.0

        positives: List[str] = []
        negatives: List[str] = []

        # A. Trend score: 20 points.
        trend = 0
        if qqq_50ma is not None:
            if qqq_close > qqq_50ma:
                trend += 5; positives.append(f"QQQ above 50MA: close {qqq_close:.2f} > 50MA {qqq_50ma:.2f}")
            else:
                negatives.append(f"QQQ below 50MA: close {qqq_close:.2f} <= 50MA {qqq_50ma:.2f}")
            if prev_50ma is not None and qqq_50ma > prev_50ma:
                trend += 5; positives.append("50MA rising")
            elif prev_50ma is not None:
                negatives.append("50MA not rising")
        else:
            negatives.append("50MA unavailable: need at least 50 clean OK rows")
        if qqq_200ma is not None:
            if qqq_close > qqq_200ma:
                trend += 5; positives.append(f"QQQ above 200MA: close {qqq_close:.2f} > 200MA {qqq_200ma:.2f}")
            else:
                negatives.append(f"QQQ below 200MA: close {qqq_close:.2f} <= 200MA {qqq_200ma:.2f}")
            if prev_200ma is not None and qqq_200ma > prev_200ma:
                trend += 5; positives.append("200MA rising")
            elif prev_200ma is not None:
                negatives.append("200MA not rising")
        else:
            negatives.append("200MA unavailable: need at least 200 clean OK rows")

        # B. Drawdown score: 15 points.
        dd_abs = abs(drawdown)
        if dd_abs < 0.03:
            draw_score = 0; negatives.append(f"Little discount from high: drawdown {fmt_pct(drawdown)}")
        elif dd_abs < 0.07:
            draw_score = 5; positives.append(f"Modest pullback: drawdown {fmt_pct(drawdown)}")
        elif dd_abs < 0.12:
            draw_score = 10; positives.append(f"Useful 1Y entry pullback: drawdown {fmt_pct(drawdown)}")
        elif dd_abs < 0.20:
            draw_score = 15; positives.append(f"Deep correction with potential 1Y value: drawdown {fmt_pct(drawdown)}")
        else:
            draw_score = 10; negatives.append(f"Bear-market scale drawdown: {fmt_pct(drawdown)}; require credit/macro repair")

        # C. Macro score: 20 points.
        macro = 0
        us10y = required_float(r, "US10Y_%")
        us30y = required_float(r, "US30Y_%")
        us30y_level = required_float(r, "US30Y")
        dxy = required_float(r, "DXY_%")
        oil = required_float(r, "UKOIL_%")
        vix_pct = required_float(r, "VIX_%")
        vix_level = required_float(r, "VIX")
        if us10y <= 0 and us30y <= 0:
            macro += 7; positives.append(f"US10Y/US30Y not rising: 10Y {fmt_pct(us10y)}, 30Y {fmt_pct(us30y)}")
        elif us10y <= 0.008 and us30y_level < 5.00:
            macro += 4; positives.append(f"Rate rise contained and US30Y below 5%: 10Y {fmt_pct(us10y)}, 30Y {us30y_level:.3f}%")
        elif us30y_level >= 5.15:
            negatives.append(f"US30Y red-zone pressure: {us30y_level:.3f}%")
        else:
            negatives.append(f"Rate pressure: US10Y {fmt_pct(us10y)}, US30Y {us30y_level:.3f}%")
        if dxy <= 0.003:
            macro += 4; positives.append(f"DXY not strong: {fmt_pct(dxy)}")
        else:
            negatives.append(f"DXY strong: {fmt_pct(dxy)}")
        if oil <= 0.02:
            macro += 4; positives.append(f"Oil not surging: {fmt_pct(oil)}")
        else:
            negatives.append(f"Oil pressure: {fmt_pct(oil)}")
        if vix_level >= 20 and vix_pct < 0:
            macro += 5; positives.append(f"VIX elevated but falling: VIX {vix_level:.2f}, {fmt_pct(vix_pct)}")
        elif vix_pct < -0.02:
            macro += 3; positives.append(f"VIX falling: {fmt_pct(vix_pct)}")
        elif vix_level < 15:
            negatives.append(f"VIX low; risk premium thin: {vix_level:.2f}")
        else:
            negatives.append(f"VIX not providing clear post-panic entry signal: VIX {vix_level:.2f}, {fmt_pct(vix_pct)}")

        # D. Leadership 5D score: 20 points.
        leadership = 0
        def ret_5d(field: str) -> Optional[float]:
            curr = close_at(clean_rows, field, 0)
            prev = close_at(clean_rows, field, 5)
            return pct_change_from_values(curr, prev)
        qqq_5d = ret_5d("QQQ_Close")
        soxx_5d = ret_5d("SOXX_Close")
        nvda_5d = ret_5d("NVDA_Close")
        msft_5d = ret_5d("MSFT_Close")
        if qqq_5d is not None and soxx_5d is not None:
            semi_5d = soxx_5d - qqq_5d
            if semi_5d >= 0:
                leadership += 7; positives.append(f"SOXX 5D not weaker than QQQ: delta {fmt_pct(semi_5d)}")
            elif semi_5d > -0.015:
                leadership += 3; positives.append(f"SOXX 5D lag is limited: delta {fmt_pct(semi_5d)}")
            else:
                negatives.append(f"SOXX 5D materially weaker than QQQ: delta {fmt_pct(semi_5d)}")
        else:
            # Fallback to 1D, but lower confidence.
            semi_1d = required_float(r, "SOXX_%") - required_float(r, "QQQ_%")
            if semi_1d >= 0:
                leadership += 3; positives.append(f"SOXX 1D not weaker than QQQ fallback: delta {fmt_pct(semi_1d)}")
            else:
                negatives.append(f"SOXX 5D unavailable; 1D delta {fmt_pct(semi_1d)}")
        if qqq_5d is not None and nvda_5d is not None:
            if nvda_5d > qqq_5d:
                leadership += 5; positives.append(f"NVDA 5D beats QQQ: delta {fmt_pct(nvda_5d-qqq_5d)}")
            else:
                negatives.append(f"NVDA 5D does not beat QQQ: delta {fmt_pct(nvda_5d-qqq_5d)}")
        elif required_float(r, "NVDA_%") > required_float(r, "QQQ_%"):
            leadership += 2; positives.append("NVDA 1D beats QQQ fallback")
        if qqq_5d is not None and msft_5d is not None:
            if msft_5d >= qqq_5d - 0.005:
                leadership += 5; positives.append("MSFT 5D not materially worse than QQQ")
            else:
                negatives.append(f"MSFT 5D weaker than QQQ: delta {fmt_pct(msft_5d-qqq_5d)}")
        elif required_float(r, "MSFT_%") >= required_float(r, "QQQ_%") - 0.005:
            leadership += 2; positives.append("MSFT 1D not materially worse than QQQ fallback")
        # Extra stabilization point for positive core participation.
        core_pos = sum([required_float(r, "QQQ_%") > 0, required_float(r, "SOXX_%") > 0, required_float(r, "NVDA_%") > 0, required_float(r, "MSFT_%") > 0])
        if core_pos >= 3:
            leadership += 3; positives.append(f"Core tech participation: {core_pos}/4 positive")
        elif core_pos <= 1:
            negatives.append(f"Poor core tech participation: {core_pos}/4 positive")
        leadership = min(20, leadership)

        # E. Breadth 5D score: 15 points.
        breadth = 0
        qqqe_5d = ret_5d("QQQE")
        rsp_5d = ret_5d("RSP")
        spy_5d = ret_5d("SPY")
        iwm_5d = ret_5d("IWM")
        if qqq_5d is not None and qqqe_5d is not None:
            if qqqe_5d >= qqq_5d:
                breadth += 5; positives.append(f"QQQE 5D beats/equal QQQ: delta {fmt_pct(qqqe_5d-qqq_5d)}")
            else:
                negatives.append(f"QQQE 5D lags QQQ: delta {fmt_pct(qqqe_5d-qqq_5d)}")
        elif required_float(r, "QQQE_vs_QQQ_%") >= 0:
            breadth += 2; positives.append("QQQE 1D not weaker than QQQ fallback")
        if rsp_5d is not None and spy_5d is not None:
            if rsp_5d >= spy_5d:
                breadth += 4; positives.append("RSP 5D beats/equal SPY")
            else:
                negatives.append(f"RSP 5D lags SPY: delta {fmt_pct(rsp_5d-spy_5d)}")
        elif required_float(r, "RSP_vs_SPY_%") >= 0:
            breadth += 2; positives.append("RSP 1D not weaker than SPY fallback")
        if iwm_5d is not None and spy_5d is not None:
            if iwm_5d >= spy_5d:
                breadth += 3; positives.append("IWM 5D beats/equal SPY")
            else:
                negatives.append(f"IWM 5D lags SPY: delta {fmt_pct(iwm_5d-spy_5d)}")
        elif required_float(r, "IWM_vs_SPY_%") >= 0:
            breadth += 1; positives.append("IWM 1D not weaker than SPY fallback")
        if spy_5d is not None and spy_5d > 0:
            breadth += 3; positives.append(f"SPY 5D positive: {fmt_pct(spy_5d)}")
        elif required_float(r, "SPY_%") > 0:
            breadth += 1; positives.append("SPY 1D positive fallback")
        ndx50r_1y_score, ndx50r_1y_health = score_ndx50r(normalize_ratio(optional_float(r.get("NDX50R_Computed"))))
        if ndx50r_1y_score > 0:
            breadth += min(4, ndx50r_1y_score * 2); positives.append(ndx50r_1y_health)
        elif ndx50r_1y_score < 0:
            negatives.append(ndx50r_1y_health)
        breadth = min(15, breadth)

        # F. Credit / risk appetite: 10 points.
        credit = 0
        hyg_5d = ret_5d("HYG")
        tlt_5d = ret_5d("TLT")
        if hyg_5d is not None and tlt_5d is not None:
            if hyg_5d >= tlt_5d:
                credit += 5; positives.append(f"HYG 5D beats/equal TLT: delta {fmt_pct(hyg_5d-tlt_5d)}")
            else:
                negatives.append(f"HYG 5D lags TLT: delta {fmt_pct(hyg_5d-tlt_5d)}")
            if hyg_5d >= -0.01:
                credit += 3; positives.append(f"HYG not breaking down 5D: {fmt_pct(hyg_5d)}")
            else:
                negatives.append(f"HYG weak 5D: {fmt_pct(hyg_5d)}")
        else:
            hyg_vs_tlt = required_float(r, "HYG_vs_TLT_%")
            if hyg_vs_tlt >= 0:
                credit += 4; positives.append(f"HYG 1D beats/equal TLT fallback: {fmt_pct(hyg_vs_tlt)}")
            else:
                negatives.append(f"HYG 1D lags TLT fallback: {fmt_pct(hyg_vs_tlt)}")
        if required_float(r, "HYG_%") > 0:
            credit += 2; positives.append(f"HYG 1D positive: {fmt_pct(required_float(r, 'HYG_%'))}")
        credit = min(10, credit)

        # V19 long-term overlay: AI chain and AI ROI risk reduce 1Y entry quality only when confirmed by price/fundamental input.
        tsm_1d = optional_float(r.get("TSM_%"))
        ai_chain_1y = 0
        semi_1d_for_1y = required_float(r, "SOXX_%") - required_float(r, "QQQ_%")
        if semi_1d_for_1y < -0.015:
            ai_chain_1y -= 2; negatives.append(f"AI chain stress: SOXX materially weaker than QQQ {fmt_pct(semi_1d_for_1y)}")
        if required_float(r, "NVDA_%") < required_float(r, "QQQ_%") - 0.015:
            ai_chain_1y -= 2; negatives.append("AI chain stress: NVDA materially weaker than QQQ")
        if tsm_1d is not None and tsm_1d < required_float(r, "QQQ_%") - 0.010:
            ai_chain_1y -= 1; negatives.append("AI chain stress: TSM ADR weaker than QQQ")
        ai_chain_1y = max(-5, ai_chain_1y)
        ai_roi_1y = clamp_int(optional_float(r.get("AI_ROI_Risk_Manual_Score")), -5, 3, 0)
        if ai_roi_1y < 0:
            negatives.append("AI ROI / depreciation risk confirmed by manual fundamental input")
        elif ai_roi_1y > 0:
            positives.append("AI ROI / monetization confirmed by manual fundamental input")
        narrative_log = str(r.get("Narrative_Risk_Note", "") or "")
        if narrative_log:
            negatives.append("Narrative risk logged only; no score impact unless confirmed")
        total = int(trend + draw_score + macro + leadership + breadth + credit + ai_chain_1y + ai_roi_1y)
        total = max(0, min(100, total))
        if len(clean_rows) >= 200:
            conf = "High: 200MA and full 1Y context available."
        elif len(clean_rows) >= 50:
            conf = "Medium: 50MA available; 200MA/52W context may be incomplete."
        elif len(clean_rows) >= 10:
            conf = "Low-Medium: short history; 5D signals available but 50/200MA unavailable."
        else:
            conf = f"Low: only {len(clean_rows)} clean OK row(s); 5D/50MA/200MA context limited."
        zone, action = classify_1y_entry(total, conf)

        rows.append({
            "Date": date,
            "Valid_For_1Y": True,
            "QQQ_Close": qqq_close,
            "QQQ_50MA": "" if qqq_50ma is None else round(qqq_50ma, 2),
            "QQQ_200MA": "" if qqq_200ma is None else round(qqq_200ma, 2),
            "QQQ_Drawdown_From_High": drawdown,
            "Trend_Score": trend,
            "Drawdown_Score": draw_score,
            "Macro_Score": macro,
            "Leadership_5D_Score": leadership,
            "Breadth_5D_Score": breadth,
            "NDX50R_Breadth_Score": score_ndx50r(normalize_ratio(optional_float(r.get("NDX50R_Computed"))))[0],
            "Breadth_Health": score_ndx50r(normalize_ratio(optional_float(r.get("NDX50R_Computed"))))[1],
            "AD_Alert": str(r.get("AD_Alert", "") or ad_line_alert(required_float(r, "QQQ_%"), optional_float(r.get("NDX_AD_Daily")))),
            "Credit_Score": credit,
            "AI_Chain_Stress": ai_chain_1y,
            "AI_ROI_Risk": ai_roi_1y,
            "Total_1Y_Entry_Score": total,
            "Entry_Zone": zone,
            "Suggested_Action": action,
            "Main_Positive_Factors": " | ".join(positives[:10]),
            "Main_Negative_Factors": " | ".join(negatives[:10]),
            "Narrative_Risk_Log": narrative_log,
            "Confidence": conf,
            "Data_Status": status,
        })
    return rows

def replace_table(ws, headers: List[str], rows: List[Dict[str, Any]]) -> None:
    ws.clear()
    ws.update("A1", [headers])
    if rows:
        ws.append_rows([[clean_json_value(r.get(h, "")) for h in headers] for r in rows], value_input_option="USER_ENTERED")
    try:
        ws.freeze(rows=1)
    except Exception:
        pass


def update_decision_output(ws, multi_rows: List[Dict[str, Any]], agent_rows: List[Dict[str, Any]], long_term_rows: Optional[List[Dict[str, Any]]] = None) -> None:
    ws.clear()
    ws.update("A1", [DASHBOARD_HEADERS])
    if not multi_rows:
        return
    latest = multi_rows[-1]
    latest_agent = agent_rows[-1] if agent_rows else {}
    long_term_rows = long_term_rows or []
    latest_1y = long_term_rows[-1] if long_term_rows else {}
    rows = build_decision_output_rows(latest, latest_agent, latest_1y)
    ws.update("A2", rows, value_input_option="USER_ENTERED")
    try:
        ws.freeze(rows=1)
    except Exception:
        pass



def is_valid_iso_date_string(value: Any) -> bool:
    """Return True only for real YYYY-MM-DD dates. Blank template rows must not count."""
    if is_blank(value):
        return False
    try:
        datetime.fromisoformat(str(value).strip()).date()
        return True
    except Exception:
        return False


def dated_daily_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Drop blank Google Sheet template rows before deciding whether bootstrap is needed."""
    return [r for r in rows if is_valid_iso_date_string(r.get("Date"))]


def merge_rows_by_date(existing_rows: List[Dict[str, Any]], new_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Merge rows by Date and return chronological rows.

    Bootstrap rows are allowed to update an existing date because they contain the
    complete computed breadth fields. This also removes empty template rows.
    """
    merged: Dict[str, Dict[str, Any]] = {}
    for r in existing_rows:
        d = str(r.get("Date", "")).strip()
        if is_valid_iso_date_string(d):
            merged[d] = r
    for r in new_rows:
        d = str(r.get("Date", "")).strip()
        if is_valid_iso_date_string(d):
            merged[d] = r
    return [merged[d] for d in sorted(merged.keys())]

def run_google_sheet() -> None:
    if not SHEET_ID:
        raise RuntimeError("Missing GOOGLE_SHEET_ID")
    log(f"Starting Market Regime Agent {VERSION}")
    gc = get_gspread_client()
    sh = gc.open_by_key(SHEET_ID)

    daily_ws = get_or_add_ws(sh, "Daily_Tracker", rows=1000, cols=len(DAILY_HEADERS) + 2)
    agent_ws = get_or_add_ws(sh, "Regime_3D_Agent", rows=1000, cols=len(AGENT_HEADERS) + 2)
    multi_ws = get_or_add_ws(sh, "Multi_Timeframe", rows=1000, cols=len(MULTI_HEADERS) + 2)
    reason_ws = get_or_add_ws(sh, "Reason_Log", rows=3000, cols=len(REASON_HEADERS) + 2)
    decision_ws = get_or_add_ws(sh, "Decision_Output", rows=100, cols=4)
    long_term_ws = get_or_add_ws(sh, "Long_Term_Entry_1Y", rows=1000, cols=len(LONG_TERM_HEADERS) + 2)
    qqq_cache_ws = get_or_add_ws(sh, QQQ_CACHE_SHEET_NAME, rows=400, cols=len(QQQ_CACHE_HEADERS) + 2)

    ensure_headers_preserve(daily_ws, DAILY_HEADERS)
    ensure_headers_preserve(agent_ws, AGENT_HEADERS)
    ensure_headers_preserve(multi_ws, MULTI_HEADERS)
    ensure_headers_preserve(reason_ws, REASON_HEADERS)
    ensure_headers_preserve(long_term_ws, LONG_TERM_HEADERS)
    ensure_headers_preserve(qqq_cache_ws, QQQ_CACHE_HEADERS)

    raw_previous_rows = get_records(daily_ws)
    previous_rows = dated_daily_rows(raw_previous_rows)
    qqq_cache_rows = get_records(qqq_cache_ws)

    # V20.4.2: bootstrap must trigger on partially populated templates too.
    # A Google Sheet template may contain blank rows, or the first manual/run row may
    # already exist. In both cases `if not previous_rows` is too weak and will skip
    # the intended 1 baseline + 3 scored historical rows.
    if len(previous_rows) < 4:
        log(f"Daily_Tracker has only {len(previous_rows)} dated row(s). V20.7 bootstrap will backfill 1 raw baseline + 3 prior market rows and remove blank template rows.")
        try:
            # V20.7: initialize QQQ cache first. This is the only intended 300d QQQ fetch.
            qqq_cache_rows, qqq_cache_warnings, _ = update_qqq_cache_rows(qqq_cache_rows)
            replace_table(qqq_cache_ws, QQQ_CACHE_HEADERS, qqq_cache_rows)
            if qqq_cache_warnings:
                log("QQQ cache bootstrap warnings: " + "; ".join(qqq_cache_warnings))
            bootstrap_rows = fetch_bootstrap_history_rows(previous_rows, scored_days=3, qqq_cache_rows=qqq_cache_rows)
            merged_rows = merge_rows_by_date(previous_rows, bootstrap_rows)
            replace_table(daily_ws, DAILY_HEADERS, merged_rows)
        except Exception as e:
            log(f"Bootstrap backfill failed; falling back to single BASELINE_ONLY snapshot: {e}")
            snapshot, qqq_cache_rows = fetch_market_snapshot(previous_rows, qqq_cache_rows)
            replace_table(qqq_cache_ws, QQQ_CACHE_HEADERS, qqq_cache_rows)
            snapshot = blank_signal_fields_for_bootstrap_baseline(snapshot)
            snapshot["Warnings"] = f"Bootstrap backfill failed: {e}; single raw-price baseline only."
            merged_rows = merge_rows_by_date(previous_rows, [snapshot])
            replace_table(daily_ws, DAILY_HEADERS, merged_rows)
    else:
        snapshot, qqq_cache_rows = fetch_market_snapshot(previous_rows, qqq_cache_rows)
        replace_table(qqq_cache_ws, QQQ_CACHE_HEADERS, qqq_cache_rows)
        append_or_update(daily_ws, snapshot, DAILY_HEADERS)

    updated_daily = get_records(daily_ws)
    agent_rows, multi_rows, reason_rows = compute_tables(updated_daily)
    long_term_rows = compute_1y_entry_table(updated_daily)
    replace_table(agent_ws, AGENT_HEADERS, agent_rows)
    replace_table(multi_ws, MULTI_HEADERS, multi_rows)
    replace_table(reason_ws, REASON_HEADERS, reason_rows)
    replace_table(long_term_ws, LONG_TERM_HEADERS, long_term_rows)
    update_decision_output(decision_ws, multi_rows, agent_rows, long_term_rows)

    if multi_rows:
        latest = multi_rows[-1]
        log(f"Latest composite={latest.get('Regime_Composite')} | transition={latest.get('Transition_Probability')} | action={latest.get('Action_Composite')}")
    log(f"Market Regime Agent {VERSION} completed")


def run_local_csv(local_csv: str, out_dir: str) -> None:
    df = pd.read_csv(local_csv)
    rows = df.fillna("").to_dict(orient="records")
    agent_rows, multi_rows, reason_rows = compute_tables(rows)
    long_term_rows = compute_1y_entry_table(rows)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(agent_rows, columns=AGENT_HEADERS).to_csv(out / "Regime_3D_Agent.csv", index=False)
    pd.DataFrame(multi_rows, columns=MULTI_HEADERS).to_csv(out / "Multi_Timeframe.csv", index=False)
    pd.DataFrame(reason_rows, columns=REASON_HEADERS).to_csv(out / "Reason_Log.csv", index=False)
    pd.DataFrame(long_term_rows, columns=LONG_TERM_HEADERS).to_csv(out / "Long_Term_Entry_1Y.csv", index=False)
    decision_rows: List[List[Any]] = []
    if multi_rows:
        latest = multi_rows[-1]
        latest_agent = agent_rows[-1] if agent_rows else {}
        latest_1y = long_term_rows[-1] if long_term_rows else {}
        decision_rows = build_decision_output_rows(latest, latest_agent, latest_1y)
    pd.DataFrame(decision_rows, columns=DASHBOARD_HEADERS).to_csv(out / "Decision_Output.csv", index=False)
    log(f"Local outputs written to {out.resolve()}")


def main() -> None:
    parser = argparse.ArgumentParser(description=f"QQQ Regime System {VERSION}")
    parser.add_argument("--local-csv", help="Run V20 locally from a Daily_Tracker CSV instead of Google Sheets.")
    parser.add_argument("--out-dir", default="outputs_local", help="Output folder for local CSV mode.")
    args = parser.parse_args()
    if args.local_csv:
        run_local_csv(args.local_csv, args.out_dir)
    else:
        run_google_sheet()


if __name__ == "__main__":
    main()