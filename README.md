# StopAt30M

AI驱动的A股量化交易系统。基于 Microsoft Qlib 进行因子研究和模型训练，通过 vn.py 执行实盘交易。

## 系统架构

```
 ┌──────────────┐
 │   数据系统    │  AKShare / Tushare / Qlib → Qlib 格式
 └──────┬───────┘
        │
 ┌──────▼───────┐
 │  因子引擎     │  Alpha158 + 514 扩展因子 = 672 因子
 └──────┬───────┘
        │
 ┌──────▼───────┐
 │  ML模型训练   │  LightGBM / XGBoost / MLP / LSTM / Transformer
 └──────┬───────┘
        │
 ┌──────▼───────┐
 │  回测引擎     │  Top-K 等权 / 交易成本 / 基准对比
 └──────┬───────┘
        │
 ┌──────▼───────┐
 │  信号生成器   │  Top-K / Long-Short / Quantile
 └──────┬───────┘
        │
 ┌──────▼───────┐
 │  交易引擎     │  vn.py (XTP/CTP Gateway)
 │  风控系统     │  止损/止盈/熔断/集中度控制
 └──────┬───────┘
        │
 ┌──────▼───────┐
 │  Web监控      │  Streamlit Dashboard
 └──────────────┘
```

## 项目结构

```
stopat30m/
├── config.yaml                    # 全局配置
├── main.py                        # CLI 入口
├── setup.py
├── requirements.txt
├── stopat30m/                     # 核心包
│   ├── config.py                  # 配置加载 (支持环境变量覆盖)
│   ├── data/
│   │   ├── provider.py            # 数据下载编排与 Qlib 初始化
│   │   └── fetcher.py             # AKShare/Tushare 抓取 + Qlib 格式转换
│   ├── factors/
│   │   ├── expressions.py         # 514 个扩展因子表达式 (29 大类)
│   │   └── handler.py             # Alpha158 + 扩展因子 DataHandler
│   ├── model/
│   │   ├── trainer.py             # 模型训练 (多模型支持)
│   │   └── evaluator.py           # IC / RankIC / 组合绩效评估
│   ├── backtest/
│   │   └── engine.py              # 回测引擎 (Top-K 等权, 交易成本, 基准对比)
│   ├── signal/
│   │   └── generator.py           # 信号生成 + CSV/Redis 输出
│   ├── trading/
│   │   ├── engine.py              # vn.py 引擎封装
│   │   ├── strategy.py            # Alpha 组合策略
│   │   ├── risk.py                # 风控引擎
│   │   └── ledger.py              # 手动交易记录 (CSV 存储, 持仓/盈亏计算)
│   └── web/
│       └── dashboard.py           # Streamlit 监控面板
└── scripts/                       # 独立脚本
    ├── download_data.py
    ├── train_model.py
    ├── generate_signal.py
    └── run_trading.py
```

## 因子库 (672 个因子)

### Alpha158 基础因子 (158 个)
Qlib 内置经典技术因子：MA、RSI、价格动量、成交量动量、波动率等。

### 扩展因子 (514 个，29 大类)

所有因子之间零重复（名称 + 表达式均唯一验证通过）。

| 类别 | 数量 | 说明 |
|------|------|------|
| **Momentum 动量** | 43 | ROC、对数收益、动量加速度、相对高低点、EMA交叉、SMA偏离 |
| **Technical 技术** | 45 | 布林带、MACD、Williams %R、CCI、ADX、随机指标、唐奇安通道、RSI、Elder Ray、一目均衡、缺口、影线 |
| **Volatility 波动率** | 33 | 已实现波动率、Parkinson、Garman-Klass、波动率比率、偏度/峰度、最大回撤、日内波动 |
| **Volume 成交量** | 32 | 量比、异常成交量、价量相关性、VWAP偏差、AD线、资金流、OBV、Amihud |
| **Trend 趋势** | 23 | 线性回归斜率/R²/残差、收益斜率、成交量斜率、中位数偏离、连涨/跌天数 |
| **Multiscale 多尺度** | 18 | 短长期收益差、相关性差异、斜率差异、波动率差异、多尺度EMA收敛散度 |
| **Volume-Price 量价交互** | 18 | 聪明资金、量价加权收益、上涨量占比、CMF、大量日超额收益 |
| **Cross-sectional 截面** | 17 | 收益排名、量能排名、波动排名、流动性排名、趋势强度排名 |
| **Weighted Momentum 加权动量** | 17 | 量加权动量、波动率调整动量、WMA/EMA动量、信息比率 |
| **Reversal 反转** | 16 | 短期/长期反转、波动率调整反转、隔夜vs日内背离 |
| **Mean Reversion 均值回归** | 16 | Z-Score、自相关系数、EMA偏离、累积偏差 |
| **ATR 真实波幅** | 15 | True Range、ATR、归一化ATR、ATR比率、ATR偏度 |
| **Price Pattern 价格形态** | 15 | 内含/外包日、连涨连跌、新高突破/新低跌破、十字星 |
| **Higher-Order 高阶动力学** | 15 | 价格速度/加速度/曲率/跳跃、R²变化、波动率加速 |
| **Microstructure 微观结构** | 15 | Roll价差、高低价差(Corwin-Schultz)、Kyle Lambda、日内强度、CLV |
| **Open Price 开盘价** | 14 | 日内收益、隔夜波动、缺口回补率、隔夜-日内相关性 |
| **Interaction 因子交互** | 14 | 量价确认、收益效率、市场能量、趋势质量、动量确认 |
| **Tail Risk 尾部风险** | 14 | 下行半方差、尾部比率、暴跌/暴涨频率、CVaR代理 |
| **Relative Strength 相对强度** | 13 | 长周期排名、夏普/Sortino排名、动量价差 |
| **Efficiency 价格效率** | 13 | Kaufman效率比、有向效率、路径长度、效率变化 |
| **Distribution 分布** | 13 | 滚动分位数、上涨频率、最大涨跌幅、方差比 |
| **Dispersion 离散度** | 13 | 收益极差、IQR、MAD、区间不对称、涨跌比 |
| **Conditional 条件统计** | 12 | 量能不对称、涨跌日波幅比、大变动贡献、事件收益 |
| **Detrended 去趋势** | 12 | DPO、PPO、去趋势成交量、周期振幅、去趋势RSI |
| **Log Transform 对数** | 12 | 对数量能Z-Score、对数量能斜率、对数波幅分析 |
| **Serial Dependence 序列依赖** | 12 | 多阶自相关、绝对收益自相关、量价交叉领先 |
| **Volume Shape 量能形态** | 12 | 量能偏度/峰度、量能集中度、变异系数、量能残差 |
| **Pivot 支撑阻力** | 11 | 经典枢轴点、S1/S2/R1/R2距离、滚动枢轴、突破频率 |
| **Sign Change 方向变化** | 11 | 翻转频率、震荡指数、方向偏倚、持续性 |

## 快速开始

### 1. 安装

```bash
pip install -r requirements.txt
```

### 2. 下载数据

```bash
# AKShare 免费数据（推荐，数据到最新）
py main.py download --source akshare

# 指定日期范围（更快）
py main.py download --source akshare --start-date 2015-01-01

# Tushare Pro 付费数据（需要 token）
py main.py download --source tushare

# Qlib 公开数据（数据截止 2020-09）
py main.py download --source qlib
```

### 3. 训练模型

```bash
# 默认 LightGBM + 全部因子
py main.py train

# 指定股票池
py main.py train --universe csi300
py main.py train --universe all

# 指定模型类型
py main.py train --model-type xgboost

# 仅使用部分因子组
py main.py train --factor-groups momentum,volatility,technical
```

### 4. 回测验证

```bash
# 默认 Top-10 等权，每 5 天调仓
py main.py backtest --model-path ./output/models/model_lgbm.pkl

# 自定义参数
py main.py backtest --model-path ./output/models/model_lgbm.pkl --top-k 10 --rebalance-freq 10
```

### 5. 生成信号

```bash
py main.py signal --model-path ./output/models/model_lgbm.pkl

# 指定日期 + 推送到 Redis
py main.py signal --model-path ./output/models/model_lgbm.pkl --date 2025-03-10 --publish
```

### 5. 启动交易

```bash
# 模拟交易 (读取信号文件)
py main.py trade --signal-source ./output/signals/

# Redis 实时信号模式
py main.py trade
```

### 6. 监控面板

```bash
py main.py dashboard
# 或直接: streamlit run stopat30m/web/dashboard.py
```

### 7. 查看因子库

```bash
py main.py info
```

## 配置

所有配置集中在 `config.yaml`，支持通过环境变量覆盖：

```bash
# 格式: STOPAT30M_SECTION__KEY=value
export STOPAT30M_TRADING__ACCOUNT=myaccount
export STOPAT30M_TRADING__PAPER_TRADING=false
```

关键配置项：

| 配置 | 说明 | 默认值 |
|------|------|--------|
| `data.train_start` | 训练集起始 | 2012-01-01 |
| `data.train_end` | 训练集结束 | 2019-06-30 * |
| `model.type` | 模型类型 | lgbm |
| `signal.top_k` | 选股数量 | 10 |
| `signal.method` | 信号方法 | top_k |
| `trading.paper_trading` | 模拟交易 | true |
| `risk.max_drawdown` | 最大回撤限制 | 15% |
| `risk.max_daily_loss` | 日亏损限制 | 3% |

## 风控规则

- **最大回撤**: 超过阈值触发熔断，暂停所有交易
- **日亏损限制**: 当日亏损超限触发熔断
- **单笔止损/止盈**: 8% / 15%
- **持仓集中度**: 单只不超过 10%
- **总仓位上限**: 95%
- **订单级校验**: 每笔下单前经过风控引擎审核

## 信号传递方式

| 方式 | 适用场景 | 延迟 |
|------|----------|------|
| CSV 文件 | 回测 / 手动触发 | 秒级 |
| Redis Pub/Sub | 准实时自动交易 | 毫秒级 |

## 模型评估指标

- **IC** (Information Coefficient): 预测与实际收益的相关性
- **Rank IC**: 排名相关性 (更稳健)
- **ICIR**: IC / IC_std (稳定性)
- **年化收益 / Sharpe / 最大回撤 / 胜率**: 组合回测指标

IC > 0.03 通常表示因子/模型有效。

## 日常运维流程

```
每天（收盘后）:
  1. 更新数据     py main.py download --source akshare --append
  2. 生成信号     py main.py signal --model-path ... --publish
  3. 自动交易     py main.py trade (持续运行)
  4. 监控         py main.py dashboard (持续运行)

每周/每月（定期）:
  5. 重新训练     py main.py train --universe csi300
```

## 技术栈

- **数据**: AKShare (免费) / Tushare Pro (付费) / Qlib 公开数据
- **研究**: Microsoft Qlib (因子计算 + 模型训练 + 回测)
- **模型**: LightGBM / XGBoost / PyTorch (MLP, LSTM, Transformer)
- **交易**: vn.py (gateway: XTP / CTP)
- **消息**: Redis Pub/Sub
- **监控**: Streamlit
- **配置**: YAML + 环境变量
- **日志**: Loguru
