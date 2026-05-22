# QQQ Regime System V20.7

V20.7 keeps the V20.6 hardened data layer and upgrades the `Decision_Output` page into a grouped decision dashboard.

## V20.7 main change

`Decision_Output` now has four columns:

- `Category`
- `Metric`
- `Value`
- `Score Meaning / How to Read`

The output is grouped into decision attributes instead of a flat metric list:

1. `01 總結行動`
   - Composite Regime
   - Composite Action
   - V20 State
   - V20 Action
   - Transition Probability
   - V20 Adjusted Score
   - Main Negative Signals
   - Main Positive Signals

2. `02 宏觀流動性`
   - Macro Valuation Pressure
   - Rate Regime
   - Long-End Yield Shock
   - Energy AI Rotation

3. `03 AI 領導性`
   - AI Chain Stress
   - AI Chain Weak Count 3D
   - AI ROI Risk
   - Narrative Risk Log

4. `04 市場廣度`
   - Breadth Health
   - NDX50R_Computed
   - NDX50R Breadth Score
   - NDX Advancers
   - NDX Decliners
   - NDX AD Daily
   - NDX AD Line Computed
   - NDX Breadth Universe
   - A/D Alert

5. `05 風險壓力`
   - Fragile Risk-On
   - Gamma Volatility Warning

6. `06 真假突破確認`
   - 1D Score
   - 3D Score
   - 10D Score
   - 20D Score
   - Clean Valid Count 3D
   - 3-of-3 Confirmed

7. `07 趨勢 / 1Y 進場`
   - QQQ 50MA
   - QQQ 200MA
   - QQQ Trend Regime
   - 1Y Entry Score
   - 1Y Entry Zone
   - 1Y Suggested Action
   - 1Y Confidence

8. `00 系統 / 資料品質`
   - System Version
   - Latest Date
   - Data Status
   - Valid For Signal
   - Confidence
   - Commentary

## V20.6 retained behavior

- yfinance retry/backoff.
- `QQQ_History_Cache` bootstrap-once logic.
- First run grabs about 300d QQQ history to seed cache.
- Later runs append recent QQQ bars only; QQQ latest close / 50MA / 200MA are computed from cached history.
- yfinance failure no longer crashes the whole workflow.
- If QQQ latest close cannot be fetched but cache exists, previous cached close is reused and row is marked `STALE`.
- `STALE`, `WARNING`, `PARTIAL`, and `BASELINE_ONLY` rows are recorded but excluded from score / 3D / composite / 1Y signal.

## Required GitHub secrets

- `GOOGLE_SERVICE_ACCOUNT_JSON`
- `GOOGLE_SHEET_ID`

## Optional environment variables

- `TIMEZONE` default: `Asia/Taipei`
- `YFINANCE_RETRIES` default: `3`
- `YFINANCE_BACKOFF_SECONDS` default: `5,15,45`
- `QQQ_INCREMENTAL_PERIOD` default: `7d`
- `NDX_TICKERS` optional comma-separated Nasdaq 100 override
- `NDX_AD_LINE_BASELINE` optional A/D line baseline
