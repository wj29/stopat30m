# StopAt30M 使用手册

> AI驱动的A股量化交易系统 — 完整操作指南

---

## 目录

1. [环境准备](#1-环境准备)
2. [安装部署](#2-安装部署)
3. [数据管理](#3-数据管理)
4. [因子库详解](#4-因子库详解)
5. [模型训练](#5-模型训练)
6. [回测验证](#6-回测验证)
7. [信号生成](#7-信号生成)
8. [交易执行](#8-交易执行)
9. [风控系统](#9-风控系统)
10. [Web 监控面板](#10-web-监控面板)
11. [配置详解](#11-配置详解)
12. [日常运维](#12-日常运维)
13. [常见问题 FAQ](#13-常见问题-faq)
14. [架构说明](#14-架构说明)

---

## 1. 环境准备

### 1.1 系统要求

| 项目 | 最低要求 | 推荐配置 |
|------|----------|----------|
| 操作系统 | macOS / Linux / Windows | Ubuntu 22.04+ |
| Python | 3.10+ | 3.11 |
| 内存 | 8 GB | 16 GB+ |
| 磁盘 | 10 GB（数据+模型） | 50 GB SSD |
| GPU | 不要求 | NVIDIA GPU（PyTorch 模型加速） |

### 1.2 依赖服务（可选）

| 服务 | 用途 | 何时需要 |
|------|------|----------|
| Redis | 实时信号传递 | 自动交易模式 |
| MySQL | 交易记录持久化 | 生产环境 |

### 1.3 券商接入（实盘交易）

| 券商接口 | 适用市场 | 说明 |
|----------|----------|------|
| XTP（中泰证券） | A股 | vn.py 官方支持 |
| CTP | 期货 | 仅用于期货交易 |

> 注意：不配置券商接口也可以正常使用。系统默认运行在**模拟交易**模式。

---

## 2. 安装部署

### 2.1 克隆项目

```bash
cd ~/py
git clone <your-repo-url> stopat30m
cd stopat30m
```

### 2.2 创建虚拟环境（推荐）

```bash
py -m venv .venv
source .venv/bin/activate
```

### 2.3 安装依赖

```bash
pip install -r requirements.txt
```

> 核心依赖安装时间约 3-5 分钟。如需 GPU 加速 PyTorch 模型，请先按 [PyTorch 官网](https://pytorch.org) 安装 CUDA 版本。

### 2.4 验证安装

```bash
py main.py info
```

预期输出：

```
StopAt30M Factor Library
==================================================
Alpha158 base factors:   158
Extended factors:        514
Total factors:           672

Extended factor groups:
--------------------------------------------------
  momentum                  43 factors
  volatility                33 factors
  volume                    33 factors
  technical                 45 factors
  microstructure            15 factors
  trend                     23 factors
  cross_sectional           17 factors
  multiscale                19 factors
  distribution              13 factors
  reversal                  16 factors
  tail_risk                 14 factors
  price_pattern             19 factors
  volume_price              18 factors
  mean_reversion            16 factors
  relative_strength         16 factors
  weighted_momentum         17 factors
  higher_order              15 factors
```

---

## 3. 数据管理

### 3.1 数据源选择

系统支持三种数据源：

| 数据源 | 费用 | 数据范围 | 说明 |
|--------|------|----------|------|
| `qlib` | 免费 | ~2005 至 2020-09 | Qlib 官方静态快照，适合快速验证 |
| `akshare` | 免费 | ~2005 至今 | 东方财富后端，无需注册，**推荐日常使用** |
| `tushare` | 付费 | ~2005 至今 | Tushare Pro，需 token，数据质量更高 |

### 3.2 下载 A 股数据

#### 方式一：Qlib 公开数据（默认，数据到 2020-09）

```bash
py main.py download
# 等同于
py main.py download --source qlib
```

> 注意：Qlib 公开数据是微软托管的静态快照，**数据截止到 2020-09-25**，不再更新。仅适合快速验证流程。

#### 方式二：AKShare 免费数据（推荐，最新数据）

```bash
# 下载 2005 年至今的全 A 股数据
py main.py download --source akshare

# 只下载近几年（更快）
py main.py download --source akshare --start-date 2015-01-01

# 指定结束日期
py main.py download --source akshare --start-date 2020-01-01 --end-date 2025-12-31
```

安装依赖：`pip install akshare`

- 无需注册，开箱即用
- 自动下载 OHLCV + 复权因子
- 自动生成 CSI300 / CSI500 成分股列表
- 约 5000 只股票，预估耗时 **40-60 分钟**

#### 方式三：Tushare Pro 付费数据（高质量）

```bash
py main.py download --source tushare
```

安装依赖：`pip install tushare`

使用前需要在 [tushare.pro](https://tushare.pro/register) 注册获取 API token，然后在 `config.yaml` 中配置：

```yaml
tushare:
  token: "你的tushare_pro_token"
```

- 数据质量更高，字段更丰富
- 基础会员免费（有频率限制），高级会员数据更全
- 自动下载 OHLCV + 复权因子 + 指数成分

### 3.3 推荐方案：Qlib 底 + 增量追加（最快）

先下载 Qlib 公开数据作为历史底仓（2005~2020-09，下载很快），再用 AKShare/Tushare 增量补齐到最新日期。此后每天只需追加当天数据。

#### 第一步：下载 Qlib 基础数据（一次性）

```bash
py main.py download --source qlib
```

#### 第二步：追加 2020-09 之后的数据

```bash
# 自动检测已有数据的最后日期，只下载后续部分
py main.py download --source akshare --append
```

由于 AKShare 免费接口有频率限制，首次追加可能有部分股票被限流跳过。用 `--retry-skipped` 重试：

```bash
# 重试上次被限流跳过的股票
py main.py download --source akshare --retry-skipped

# 也可以和 --append 组合：先追加新日期，再重试跳过的
py main.py download --source akshare --append --retry-skipped
```

系统自动读取现有日历的最后日期（2020-09-25），仅获取 2020-09-26 至今的数据并合并到已有文件中。

#### 第三步：每日增量更新

```bash
# 收盘后运行，只追加当天数据
py main.py download --source akshare --append
```

`--append` 模式的工作原理：
1. 读取现有 `calendars/day.txt` 获取最后日期
2. 仅从 API 获取 *最后日期+1* 到今天的数据
3. 扩展日历文件、追加二进制特征数据、更新股票列表
4. 如果数据已是最新则跳过，不做任何修改

`--retry-skipped` 模式：
1. 扫描每只股票的 binary 文件，找出数据长度不足（未覆盖完整日历）的股票
2. 只对这些不完整的股票重新请求数据
3. 支持断点续传，中断后重跑会跳过已完成的
4. 可与 `--append` 组合使用

> **耗时估算**：初次追加 5 年数据约 40-60 分钟，每日增量更新约 15-20 分钟。重试跳过的股票视数量而定。

同样可以使用 Tushare Pro：

```bash
py main.py download --source tushare --append
```

### 3.4 指定下载目录

```bash
py main.py download --source akshare --target-dir /data/qlib/cn_data
```

同时需要修改 `config.yaml` 中的路径：

```yaml
qlib:
  provider_uri: "/data/qlib/cn_data"
```

### 3.5 数据目录结构

```
~/.qlib/qlib_data/cn_data/
├── calendars/
│   └── day.txt             # 交易日历（每行一个日期）
├── instruments/
│   ├── all.txt             # 全 A 股列表
│   ├── csi300.txt          # 沪深 300 成分
│   └── csi500.txt          # 中证 500 成分
└── features/
    ├── sh600000/           # 每只股票一个目录（小写）
    │   ├── open.day.bin    # float32 二进制：[start_index] + [data...]
    │   ├── close.day.bin
    │   ├── high.day.bin
    │   ├── low.day.bin
    │   ├── volume.day.bin
    │   ├── change.day.bin
    │   └── factor.day.bin  # 复权因子
    ├── sh600519/
    └── ...
```

### 3.6 数据更新

下载最新数据后，**务必更新 `config.yaml` 中的日期范围**以匹配实际数据：

```yaml
data:
  train_start: "2012-01-01"
  train_end: "2024-06-30"
  valid_start: "2024-07-01"
  valid_end: "2025-06-30"
  test_start: "2025-07-01"
  test_end: "2026-03-12"
```

建议每日收盘后增量更新：

```bash
py main.py download --source akshare --append
```

或配合 cron 自动运行：

```cron
30 16 * * 1-5 cd ~/py/stopat30m && .venv/bin/python main.py download --source akshare --append >> output/logs/cron_data.log 2>&1
```

---

## 4. 因子库详解

### 4.1 因子总览

系统包含 **672 个因子**，分为两部分：

| 部分 | 数量 | 来源 |
|------|------|------|
| Alpha158 基础因子 | 158 | Qlib 内置经典技术因子 |
| 扩展因子 | 514 | 29 类市场常用因子 |

### 4.2 Alpha158 基础因子

Qlib 官方经典因子集，覆盖：

- **K线形态**：开收比、影线比、实体占比
- **均线系统**：5/10/20/30/60 日 MA、价格偏离
- **波动率**：滚动标准差
- **动量**：多周期价格动量
- **量价关系**：量价相关性
- **统计特征**：R²、回归残差、分位数

### 4.3 扩展因子分类详解

#### Momentum 动量 (43 个)

捕捉价格惯性效应。

| 子类 | 说明 | 示例 |
|------|------|------|
| ROC | 多周期涨跌幅 (1d~250d) | `$close/Ref($close,20)-1` |
| 对数收益 | 更符合正态分布假设 | `Log($close/Ref($close,5))` |
| 动量加速度 | ROC 的变化率 | ROC(t) - ROC(t-N) |
| 相对高低 | 距 N 日最高/最低点的距离 | `$close/Max($high,60)-1` |
| 区间位置 | 在 N 日振幅中的相对位置 | (C-L_n)/(H_n-L_n) |
| EMA 交叉 | 快慢均线比值 | EMA(12)/EMA(26) |
| SMA 偏离 | 价格偏离均线程度 | `$close/Mean($close,20)-1` |

#### Technical 技术指标 (45 个)

经典技术分析指标的因子化表达。

| 指标 | 因子数 | 说明 |
|------|--------|------|
| 布林带 | 9 | 带宽、位置、Z-Score（10/20/60 日） |
| MACD | 3 | MACD 线、柱状图、归一化 MACD |
| Williams %R | 3 | 10/14/20 日 |
| CCI | 2 | 14/20 日 |
| ADX | 2 | 趋势强度代理 |
| 随机指标 | 4 | %K 和 %D（9/14 日） |
| 唐奇安通道 | 6 | 通道宽度和位置 |
| RSI | 3 | 6/12/24 日 |
| Elder Ray | 4 | 多空力量 |
| 一目均衡 | 2 | 转换线-基准线差、云层位置 |
| 缺口 | 4 | 跳空幅度及均值 |
| K 线影线 | 3 | 上影线、下影线、实体比 |

#### Volatility 波动率 (33 个)

多种波动率估计方法和波动特征。

| 子类 | 说明 |
|------|------|
| 已实现波动率 | 收盘价收益率标准差 (5~120 日) |
| Parkinson | 基于最高最低价，效率更高 |
| Garman-Klass | 使用 OHLC 四价，理论最优 |
| 波动率比率 | 短期/长期波动率，检测波动率体制变化 |
| 偏度 / 峰度 | 收益分布的非对称性和尾部厚度 |
| 最大回撤 | 从区间最高点下跌的幅度 |
| 日内波动 | (最高-最低)/收盘 |
| 波动率的波动率 | 波动率本身的不稳定性 |

#### Volume 成交量 (33 个)

成交量的多维度分析。

| 子类 | 说明 |
|------|------|
| 量比 | 短/长期成交量比 |
| 异常成交量 | Z-Score 标准化 |
| 量价相关 | 价格变动与成交量的滚动相关性 |
| VWAP 偏差 | 价格偏离成交量加权均价 |
| AD 线 | 累积分布线 |
| OBV | 能量潮指标 |
| Amihud | 非流动性指标 |

#### Reversal 反转 (16 个) — **新增**

捕捉短期过度反应和均值回归信号。

| 子类 | 说明 |
|------|------|
| 短期反转 | 1~5 日负收益率 |
| 波动率调整反转 | 收益/波动率 |
| 长期反转 | 60/120/250 日过度反应 |
| 去均值收益 | 当日收益偏离均值 |
| 振荡因子 | 区间最大涨幅与最大跌幅之和 |
| 隔夜 vs 日内 | 隔夜收益与日内收益的背离 |

#### Tail Risk 尾部风险 (14 个) — **新增**

度量极端风险事件的频率和幅度。

| 子类 | 说明 |
|------|------|
| 下行半方差 | 仅考虑负收益的波动 |
| 上行半方差 | 仅考虑正收益的波动 |
| 尾部比率 | 上行/下行半方差比 |
| 暴跌频率 | 收益 < -2σ 的天数占比 |
| 暴涨频率 | 收益 > +2σ 的天数占比 |
| CVaR 代理 | 平均最差收益（条件风险价值） |

#### Price Pattern 价格形态 (19 个) — **新增**

K 线形态和价格结构特征。

| 子类 | 说明 |
|------|------|
| 内含日 | 当日振幅在前日范围内 |
| 外包日 | 当日振幅包住前日 |
| 连涨/连跌 | 多窗口的上涨/下跌天数占比 |
| 新高突破 | 收盘价突破 N 日最高 |
| 新低跌破 | 收盘价跌破 N 日最低 |
| 十字星 | 小实体K线识别 |

#### Volume-Price 量价交互 (18 个) — **新增**

深度分析成交量和价格的联动关系。

| 子类 | 说明 |
|------|------|
| 聪明资金 | 价格/成交量排名相关性 |
| 量价加权收益 | 以成交量为权重的收益 |
| 上涨量占比 | 上涨日的成交量/总成交量 |
| 量价合力 | 累积收益 × 累积成交量 |
| 大量日超额收益 | 放量日 vs 缩量日的收益差 |
| CMF | 蔡金资金流 |
| 成交量异常下的波动 | 异常放量时的收益幅度 |

#### Mean Reversion 均值回归 (16 个) — **新增**

均值回归策略核心因子。

| 子类 | 说明 |
|------|------|
| Z-Score | 价格偏离滚动均值的标准差数 |
| 对数 Z-Score | 对数价格的 Z-Score |
| 自相关 | 收益率自相关系数（回归速度代理） |
| EMA Z-Score | 偏离 EMA 的标准化距离 |
| 累积偏差 | 累积收益偏离趋势的程度 |

#### Relative Strength 相对强度 (16 个) — **新增**

截面比较型因子，衡量个股在全市场中的相对位置。

| 子类 | 说明 |
|------|------|
| 收益排名 | 多周期的截面收益排名 |
| 成交量排名 | 相对成交量活跃度 |
| 动量价差 | 短期排名 - 长期排名 |
| 波动排名 | 相对波动率水平 |
| 夏普排名 | 风险调整后收益的截面排名 |

#### Weighted Momentum 加权动量 (17 个) — **新增**

对简单动量进行各种加权/调整。

| 子类 | 说明 |
|------|------|
| 成交量加权动量 | 放量时的动量贡献更大 |
| 波动率调整动量 | 收益率/波动率 |
| WMA 动量 | 线性衰减加权 |
| EMA 动量 | 指数衰减加权 |
| 信息比率 | 均值收益/均值绝对偏差 |

#### Higher-Order 高阶动力学 (15 个) — **新增**

价格运动的速度、加速度、曲率等高阶特征。

| 子类 | 说明 |
|------|------|
| 速度 | 价格一阶差分 |
| 加速度 | 价格二阶差分 |
| 曲率 | 趋势斜率的变化 |
| 跳跃 | 价格三阶差分 |
| R² 变化 | 趋势拟合优度的变化 |
| 波动率加速 | 波动率的变化速度 |

#### ATR 真实波幅 (15 个) — **新增**

基于 True Range（计入跳空缺口），比 High-Low 更准确的波动度量。

| 子类 | 说明 |
|------|------|
| ATR | 多窗口平均真实波幅 (5~60日) |
| 归一化 ATR | ATR / 价格，跨价格可比 |
| ATR 比率 | 短/长周期比，波动率体制变化 |
| 收益容量 | 日收益 / ATR，实际利用了多少波幅 |
| ATR 扩张 | 当日 TR / ATR，波幅突变检测 |
| ATR 偏度 | 波幅分布的非对称性 |

#### Efficiency 价格效率 (13 个) — **新增**

Kaufman 效率比：衡量价格移动的"直线度"。

| 子类 | 说明 |
|------|------|
| 效率比 | \|净位移\| / 总路径长度 |
| 有向效率 | 保留方向的效率（区分涨跌） |
| 路径长度 | 总波动路径 / 价格，活跃度度量 |
| 效率变化 | 趋势是变得更有效还是更混乱 |

#### Open Price 开盘价 (14 个) — **新增**

$open 是被严重忽视的变量，蕴含隔夜信息流。

| 子类 | 说明 |
|------|------|
| 开盘位置 | 开盘价在当日振幅中的位置 |
| 日内收益 | 开盘→收盘的纯日内回报 |
| 隔夜波动 | 跳空缺口的标准差 |
| 日内波动 | 开→收回报的标准差 |
| 隔夜-日内相关 | 缺口方向与日内方向是否一致 |
| 缺口回补率 | 日内反向填补缺口的频率 |

#### Serial Dependence 序列依赖 (12 个) — **新增**

多阶自相关，揭示均值回归还是趋势延续的微结构。

| 子类 | 说明 |
|------|------|
| Lag-2/3/5 自相关 | 不同时滞的收益自相关 |
| 绝对收益自相关 | 波动率聚类效应的度量 |
| 量价交叉领先 | 昨日成交量能否预测今日收益 |

#### Conditional 条件统计 (12 个) — **新增**

按市场状态分组的条件统计量。

| 子类 | 说明 |
|------|------|
| 量能不对称 | 上涨日均量 / 下跌日均量 |
| 波幅不对称 | 上涨日均波幅 / 下跌日均波幅 |
| 波动不对称 | 上涨波动率 / 下跌波动率 |
| 大变动贡献 | 极端收益对总收益的贡献占比 |
| 事件收益 | 放量异常日的平均收益 |
| 盈亏比 | 上涨日均收益 / 下跌日均收益幅度 |

#### Volume Shape 量能形态 (12 个) — **新增**

成交量自身的分布形态特征。

| 子类 | 说明 |
|------|------|
| 量能偏度 | 成交量分布的非对称性 |
| 量能峰度 | 成交量分布的尾部厚度 |
| 量能集中度 | 最大量日 / 平均量 |
| 变异系数 | 成交量标准差 / 均值 |
| 量能残差 | 成交量偏离自身回归趋势 |

#### Pivot 支撑阻力 (11 个) — **新增**

经典技术分析的枢轴点系统。

| 子类 | 说明 |
|------|------|
| 枢轴点距离 | 价格偏离经典 Pivot 的程度 |
| R1/R2/S1/S2 | 到各阻力/支撑位的标准化距离 |
| 滚动枢轴 | 多日窗口的动态支撑阻力 |
| 枢轴突破频率 | 收盘在枢轴上方的天数占比 |
| 枢轴区间 | R1-S1 范围（预期波动区间） |

#### Detrended 去趋势 (12 个) — **新增**

移除趋势成分后的周期/振荡特征。

| 子类 | 说明 |
|------|------|
| DPO | 去趋势价格振荡器 |
| PPO | 百分比价格振荡器 (快EMA/慢EMA - 1) |
| 去趋势成交量 | 成交量偏离自身趋势 |
| 周期振幅 | 回归残差的振幅 |
| 去趋势RSI | RSI 偏离 50 的距离 |

#### Sign Change 方向变化 (11 个) — **新增**

度量市场是趋势还是震荡。

| 子类 | 说明 |
|------|------|
| 翻转频率 | 收益方向翻转的天数占比 |
| 震荡指数 | 路径长度 / 净位移（choppiness）|
| 方向偏倚 | (上涨天数 - 下跌天数) / 总天数 |
| 持续性 | 同向连续日的收益权重 |

#### Interaction 因子交互 (14 个) — **新增**

有经济学含义的非线性组合。

| 子类 | 说明 |
|------|------|
| 量价确认 | 收益 × 成交量变化方向 |
| 收益效率 | 日收益 / True Range |
| 市场能量 | 波幅 × 成交量的相对水平 |
| 趋势质量 | R² / 波动率（高拟合低波动 = 优质趋势）|
| 动量确认 | 动量 × 量比 |

#### Log Transform 对数 (12 个) — **新增**

对数变换压缩尺度，适合重尾分布的变量。

| 子类 | 说明 |
|------|------|
| 对数量能 Z-Score | 对数成交量偏离均值 |
| 对数量能斜率 | 对数成交量的线性趋势 |
| 对数波幅 Z-Score | 对数 H/L 偏离的标准化 |
| 对数波幅斜率 | 对数波幅的趋势方向 |
| 对数波幅偏度 | 对数波幅分布的非对称性 |

#### Dispersion 离散度 (13 个) — **新增**

收益的分散程度和不对称性。

| 子类 | 说明 |
|------|------|
| 收益极差 | 最大日收益 - 最小日收益 |
| IQR | 四分位距（更稳健的离散度量） |
| MAD | 平均绝对偏差 |
| 区间不对称 | (H-C)/(C-L)，上方空间 vs 下方空间 |
| 涨跌比 | 上涨天数 / 下跌天数 |

*(其余 5 类：Trend、Cross-sectional、Multiscale、Microstructure、Distribution 同上一版本)*

### 4.4 按需选择因子组

训练时可以只使用部分因子组：

```bash
# 仅使用动量+技术+波动率
py main.py train --factor-groups momentum,technical,volatility
```

在 `config.yaml` 中禁用特定组：

```yaml
factors:
  enable_tail_risk: false
  enable_higher_order: false
```

---

## 5. 模型训练

### 5.1 支持的模型

| 模型 | 类型 | 训练速度 | 适合场景 |
|------|------|----------|----------|
| **LightGBM** | 树模型 | 快（分钟级） | 默认首选，因子重要性可解释 |
| **XGBoost** | 树模型 | 较快 | LightGBM 的替代 |
| **MLP** | 神经网络 | 中等 | 捕捉非线性交互 |
| **LSTM** | 循环神经网络 | 慢 | 时序依赖性建模 |
| **Transformer** | 注意力机制 | 最慢 | 长距离依赖，需要 GPU |

### 5.2 基本训练

```bash
# 默认配置：LightGBM，股票池来自 config.yaml
py main.py train

# 指定股票池和选股数量
py main.py train --universe csi300 --top-k 10

# 全市场训练
py main.py train --universe all
```

可选股票池：`csi300`（沪深300）、`csi500`（中证500）、`all`（全 A 股）

训练过程中每 10 秒输出进度和预估剩余时间。

### 5.3 指定模型类型

```bash
py main.py train --model-type xgboost
py main.py train --model-type mlp
```

### 5.4 自定义训练参数

修改 `config.yaml`：

```yaml
model:
  type: lgbm
  params:
    lgbm:
      loss: mse
      num_leaves: 256         # 叶子节点数（越大越复杂）
      learning_rate: 0.05     # 学习率
      n_estimators: 500       # 树的数量
      max_depth: 8            # 最大深度
      subsample: 0.8          # 行采样比例
      colsample_bytree: 0.8   # 列采样比例
      reg_alpha: 0.1          # L1 正则化
      reg_lambda: 1.0         # L2 正则化
      min_child_samples: 50   # 叶子最小样本数
```

### 5.5 训练/验证/测试划分

修改 `config.yaml`：

```yaml
data:
  train_start: "2012-01-01"    # 训练集起始
  train_end: "2024-06-30"      # 训练集结束
  valid_start: "2024-07-01"    # 验证集起始
  valid_end: "2025-06-30"      # 验证集结束
  test_start: "2025-07-01"     # 测试集起始
  # test_end: 留空则自动使用今天的日期
```

> `test_end` 留空（或不写）时自动取当天日期，无需手动更新。

> **重要**：日期范围必须在已下载数据的覆盖范围内。如果使用 Qlib 公开数据（截止 2020-09-25），需要相应缩短所有日期。推荐使用 AKShare + `--append` 下载最新数据后再训练。

**原则**：
- 训练集尽量长（8-12 年），覆盖多轮牛熊
- 验证集 1-2 年，用于调参和 early stopping
- 测试集取最近到今天，模拟真实效果
- **绝不能**用测试集数据调参

### 5.6 训练输出

训练完成后在 `output/` 目录生成：

```
output/
├── models/
│   └── model_lgbm.pkl       # 序列化的模型文件
└── metrics.json              # 评估指标
```

### 5.7 评估指标解读

| 指标 | 含义 | 好的标准 |
|------|------|----------|
| **IC** | 预测与实际收益的相关系数 | > 0.03 有效，> 0.05 优秀 |
| **Rank IC** | 排名相关系数（更稳健） | > 0.05 有效，> 0.08 优秀 |
| **ICIR** | IC / IC标准差（稳定性） | > 0.5 稳定，> 1.0 非常好 |
| **Rank ICIR** | RankIC / RankIC标准差 | > 0.5 |

---

## 6. 回测验证

训练完成后，通过回测验证模型在测试集上的实际组合表现。

### 6.1 基本回测

```bash
py main.py backtest --model-path output/models/model_lgbm.pkl
```

默认行为：
- 在 `config.yaml` 定义的 **test 时间段**上运行
- 每 5 个交易日调仓一次，选 Top-10 股票等权持有
- 扣除交易费用（买入 0.03%，卖出 0.13%）
- 对比沪深 300 基准

### 6.2 自定义参数

```bash
# 持有 Top-10，每 10 天调仓
py main.py backtest --model-path output/models/model_lgbm.pkl --top-k 10 --rebalance-freq 10

# 指定股票池
py main.py backtest --model-path output/models/model_lgbm.pkl --universe csi500

# 仅用部分因子组
py main.py backtest --model-path output/models/model_lgbm.pkl --factor-groups momentum,technical,volatility
```

### 6.3 回测输出

```
output/backtest/
├── report.json     # 绩效指标摘要
├── returns.csv     # 每日收益（策略 + 基准 + 累计净值）
├── trades.csv      # 交易记录（日期、股票、买卖、权重）
└── positions.csv   # 每日持仓明细
```

### 6.4 绩效指标

| 指标 | 含义 | 好的标准 |
|------|------|----------|
| **年化收益** | 年化后的组合回报 | > 15% |
| **夏普比率** | 收益/波动比 | > 1.0 |
| **Sortino** | 收益/下行波动比 | > 1.5 |
| **最大回撤** | 从峰值到谷底的最大跌幅 | < -20% |
| **Calmar** | 年化收益/最大回撤 | > 1.0 |
| **胜率** | 盈利天数占比 | > 50% |
| **盈亏比** | 平均盈利/平均亏损 | > 1.0 |
| **超额收益** | 超过基准的年化收益 | > 0 |

### 6.5 在 Dashboard 查看

```bash
py main.py dashboard
```

「模型评估」页面会展示：
- **净值曲线**：策略 vs 基准
- **回撤曲线**：动态最大回撤
- **月度收益热力图**：年 × 月收益矩阵
- **IC/RankIC/ICIR**：模型预测能力指标

### 6.6 调参建议

| 参数 | 说明 | 建议范围 |
|------|------|----------|
| `top_k` | 持仓股票数 | 10~30，过少集中度高、过多稀释 alpha |
| `rebalance_freq` | 调仓频率（天） | 5~20，过短交易成本高、过长反应慢 |
| `buy_cost` | 买入费率 | 0.0003 (万三) |
| `sell_cost` | 卖出费率 | 0.0013 (万三 + 千一印花税) |

### 6.7 执行价格模型

默认回测使用**开盘价**执行交易（`deal_price: open`），更贴近真实场景：

- 信号在 T-1 收盘后生成，最早只能在 T 开盘时执行
- 卖出的股票：收益计算到 T 开盘价（overnight 部分）
- 买入的股票：收益从 T 开盘价开始算（intraday 部分）
- 继续持有的：按完整 close-to-close 计算

```bash
# 默认：开盘价执行（真实）
py main.py backtest --model-path output/models/model_lgbm.pkl

# 收盘价执行（乐观，用于对比看执行缺口有多大）
py main.py backtest --model-path output/models/model_lgbm.pkl --deal-price close
```

两者差异越大，说明 alpha 越多被隔夜跳空吃掉。如果 `open` 模式仍有显著正收益，策略真实可行。

修改 `config.yaml` 中的 `backtest` 节可以设置默认值：

```yaml
backtest:
  top_k: 10
  rebalance_freq: 5
  buy_cost: 0.0003
  sell_cost: 0.0013
  benchmark: "SH000300"
  deal_price: "open"     # open=开盘价执行(真实), close=收盘价执行(乐观)
```

---

## 7. 信号生成

### 7.1 基本用法

```bash
py main.py signal --model-path ./output/models/model_lgbm.pkl
```

### 7.2 指定日期

```bash
py main.py signal --model-path ./output/models/model_lgbm.pkl --date 2025-03-10
```

### 7.3 信号方法

在 `config.yaml` 中配置：

```yaml
signal:
  top_k: 10            # 选股数量
  method: "top_k"      # 信号方法
  rebalance_freq: 5    # 调仓频率（交易日）
```

| 方法 | 说明 | 适用场景 |
|------|------|----------|
| `top_k` | 选预测收益最高的 K 只，等权 | 简单策略，推荐入门 |
| `long_short` | 做多 Top K，做空 Bottom K | 多空策略（A股做空受限） |
| `quantile` | 选 Top 20% 分位，按分数加权 | 更精细的仓位分配 |

### 7.4 信号输出格式

CSV 示例：

```
instrument,score,signal,weight,date
600519,0.0321,BUY,0.05,2025-03-10
000001,0.0287,BUY,0.05,2025-03-10
300750,0.0265,BUY,0.05,2025-03-10
...
```

| 字段 | 说明 |
|------|------|
| `instrument` | 股票代码 |
| `score` | 模型预测分数（越高预期收益越好） |
| `signal` | BUY / SELL |
| `weight` | 目标持仓权重 |
| `date` | 信号日期 |

### 7.5 发布到 Redis

```bash
py main.py signal --model-path ./output/models/model_lgbm.pkl --publish
```

信号会推送到 Redis 的 `alpha_signals` 频道，交易引擎自动订阅。

---

## 8. 交易执行

### 8.1 运行模式

| 模式 | 说明 | 配置 |
|------|------|------|
| **模拟交易** | 不连接券商，仅模拟执行 | `paper_trading: true`（默认） |
| **实盘交易** | 通过 vn.py 连接券商 | `paper_trading: false` + 配置券商参数 |

### 8.2 模拟交易

```bash
# 读取信号文件执行模拟交易
py main.py trade --signal-source ./output/signals/

# 指定单个信号文件
py main.py trade --signal-source ./output/signals/signal_20250310_latest.csv
```

### 8.3 Redis 实时交易

```bash
# 从 Redis 订阅信号（需要先启动 Redis）
py main.py trade
```

此模式下：
1. 信号生成端执行 `py main.py signal --publish`
2. 交易端自动接收并执行

### 8.4 实盘配置

修改 `config.yaml`：

```yaml
trading:
  gateway: "xtp"           # 券商接口
  account: "your_account"  # 资金账号
  password: "your_password"
  broker_host: "120.x.x.x" # 券商服务器
  broker_port: 6001
  paper_trading: false      # 关闭模拟
```

或通过环境变量（更安全）：

```bash
export STOPAT30M_TRADING__ACCOUNT=your_account
export STOPAT30M_TRADING__PASSWORD=your_password
export STOPAT30M_TRADING__PAPER_TRADING=false
```

### 8.5 交易流程

```
信号输入
  ↓
计算目标持仓 (target positions)
  ↓
对比当前持仓 (diff positions)
  ↓
生成订单列表 (orders)
  ↓
风控检查 (risk check)  ← 每笔订单必须通过
  ↓
执行交易 (execute)
  ↓
更新持仓 (update positions)
```

### 8.6 A 股交易注意事项

- **最小交易单位**：100 股（1 手）
- **T+1 制度**：当日买入次日才能卖出
- **涨跌停**：主板 ±10%，创业板/科创板 ±20%
- **佣金**：买入 ~0.03%，卖出 ~0.13%（含印花税 0.1%）

---

## 9. 风控系统

### 9.1 风控架构

风控引擎在**每笔订单执行前**进行检查，作为不可绕过的安全层。

```
策略生成订单 → 风控引擎检查 → 通过/拒绝 → 执行/记录
```

### 9.2 风控规则一览

| 规则 | 默认阈值 | 触发动作 | 说明 |
|------|----------|----------|------|
| 最大回撤 | 15% | 熔断 | 从峰值回撤超限，暂停所有交易 |
| 日亏损限制 | 3% | 熔断 | 当日亏损超限，当日停止交易 |
| 单笔止损 | 8% | 平仓 | 单只持仓亏损超限自动卖出 |
| 单笔止盈 | 15% | 平仓 | 单只持仓盈利超限自动止盈 |
| 集中度限制 | 10% | 拒绝订单 | 单只股票不超过总资金 10% |
| 单笔上限 | 5% | 拒绝订单 | 单笔下单不超过总资金 5% |
| 总仓位上限 | 95% | 拒绝订单 | 总持仓市值不超过 95% |
| 熔断阈值 | 8% | 熔断 | 快速亏损触发紧急停止 |

### 9.3 修改风控参数

```yaml
risk:
  max_drawdown: 0.15          # 最大回撤 → 触发全局熔断
  max_daily_loss: 0.03        # 日亏损限制 → 触发当日熔断
  max_single_loss: 0.05       # 单只止损阈值
  max_concentration: 0.10     # 单只最大集中度
  circuit_breaker_loss: 0.08  # 快速亏损熔断

trading:
  max_position_pct: 0.05      # 单笔最大占比
  max_total_position_pct: 0.95 # 总仓位上限
  stop_loss_pct: 0.08         # 止损线
  take_profit_pct: 0.15       # 止盈线
```

### 9.4 熔断机制

当触发熔断后：
1. **所有新订单被拒绝**
2. 系统不会自动平仓（避免恐慌卖出）
3. 需要人工确认后手动重置

熔断会在次日自动重置（日级别熔断）或需要重启系统（回撤熔断）。

---

## 10. Web 监控面板

### 10.1 启动

```bash
py main.py dashboard
```

或直接运行：

```bash
streamlit run stopat30m/web/dashboard.py
```

默认访问地址：`http://localhost:8501`

### 10.2 页面说明

#### 概览页

- 账户权益、持仓数量、持仓市值、今日盈亏
- 最新信号列表
- 风控状态（正常/熔断）

#### 调仓操作（一键调仓）

根据最新信号自动生成调仓计划，一键录入交易记录：

1. **加载信号**：自动读取最近一次信号文件，展示目标股票和权重
2. **当前持仓**：从交易记录中计算实际持仓
3. **资金设置**：输入总资金和现金预留比例（默认 2%）
4. **生成计划**：获取实时行情（AKShare），自动计算买卖差额
   - 不在新信号中的持仓 → 清仓（卖出）
   - 新信号中新增的标的 → 建仓（买入）
   - 权重变化的持仓 → 加仓/减仓
   - 按 100 股整手取整，含手续费计算
5. **资金流向**：显示当前现金 → 卖出回笼 → 买入支出 → 调仓后现金
   - 若买入总额超过可用资金，**自动按比例缩减**并警告
6. **一键录入**：选择日期后录入全部交易，或「仅录入卖出」分步操作

> **执行建议**：先完成全部卖出，再执行买入。A股卖出资金 T+0 可用于买入。
> 页面显示的价格为实时行情参考，实际成交以券商委托价为准。

#### 交易记录（手动录入）

手动交易录入页面，用于记录实际买卖操作：

- **录入表单**：日期、股票代码、买/卖方向、数量、价格、手续费、备注
- **交易历史表**：展示所有录入的交易记录，支持删除
- **已实现盈亏**：根据卖出记录自动计算已实现盈亏并绘制累计盈亏曲线

数据存储在 `output/trades/trades.csv`，纯文件存储无需数据库。

#### 持仓管理

- **实时市值**：通过 AKShare 获取最新价格，展示每只持仓的市值和浮动盈亏
- **汇总指标**：总市值、总成本、浮动盈亏（金额 + 百分比）
- **持仓占比图**：按市值的柱状图
- **盈亏分布图**：按浮盈/浮亏的柱状图
- **净值快照**：点击「记录今日净值快照」保存当日估值，用于绘制净值曲线
- **净值曲线**：总资产 vs 总成本的历史走势

#### 信号历史

- 浏览历史信号文件（CSV 格式，每次 `py main.py signal` 自动生成）
- 信号分布图（买入/卖出分布）

#### 模型评估

- **IC 指标**：IC / Rank IC / ICIR，每个指标附带评级和解读（弱/偏弱/及格/良好/优秀）
- **综合评价**：根据 Rank IC 和 ICIR 给出模型是否可用的结论
- **回测绩效**：年化收益、夏普比率、最大回撤、胜率等，每项附带评级
- **回测综合评价**：根据夏普、回撤、收益给出策略是否可用的结论
- 净值曲线（策略 vs 基准）
- 回撤曲线
- 月度收益热力图
- **实盘 vs 回测对比**：将手动录入的实际盈亏与回测模拟盈亏放在同一图表比较

#### 风控监控

- 峰值权益、今日盈亏
- 熔断状态（绿色正常/红色触发）
- 被拒绝订单数
- 当前风控参数一览

#### 因子分析

- 各因子组统计
- 因子名称和表达式浏览

---

## 11. 配置详解

### 11.1 配置文件

所有配置集中在项目根目录的 `config.yaml`。

### 11.2 环境变量覆盖

格式：`STOPAT30M_SECTION__KEY=value`

```bash
# 示例
export STOPAT30M_TRADING__PAPER_TRADING=true
export STOPAT30M_SIGNAL__TOP_K=30
export STOPAT30M_RISK__MAX_DRAWDOWN=0.20
export STOPAT30M_REDIS__HOST=192.168.1.100
```

优先级：环境变量 > config.yaml 默认值

### 11.3 完整配置项参考

#### qlib 配置

| 键 | 说明 | 默认值 |
|------|------|--------|
| `provider_uri` | 数据目录 | `~/.qlib/qlib_data/cn_data` |
| `region` | 地区 | `cn` |

#### data 配置

| 键 | 说明 | 默认值 |
|------|------|--------|
| `download_start` | 数据下载起始日期 | `2005-01-01` |
| `train_start` | 训练集起始 | `2012-01-01` |
| `train_end` | 训练集结束 | `2024-06-30` |
| `valid_start` | 验证集起始 | `2024-07-01` |
| `valid_end` | 验证集结束 | `2025-06-30` |
| `test_start` | 测试集起始 | `2025-07-01` |
| `test_end` | 测试集结束 | 留空 = 今天 |
| `benchmark` | 基准指数 | `SH000300` |
| `universe` | 股票池 | `csi300` |

#### tushare 配置

| 键 | 说明 | 默认值 |
|------|------|--------|
| `token` | Tushare Pro API token | `""` (空=未配置) |

#### factors 配置

| 键 | 说明 | 默认值 |
|------|------|--------|
| `base` | 基础因子集 | `alpha158` |
| `enable_<group>` | 启用/禁用特定因子组 | `true` |
| `label` | 预测标签表达式 | `Ref($close,-5)/$close - 1` |
| `label_name` | 标签名称 | `LABEL0` |

#### model 配置

| 键 | 说明 | 默认值 |
|------|------|--------|
| `type` | 模型类型 | `lgbm` |
| `output_dir` | 模型输出目录 | `./output/models` |
| `params.<type>.*` | 模型超参数 | 见 config.yaml |

#### backtest 配置

| 键 | 说明 | 默认值 |
|------|------|--------|
| `top_k` | 持仓股票数量 | `10` |
| `rebalance_freq` | 调仓周期(天) | `5` |
| `buy_cost` | 买入费率 | `0.0003` |
| `sell_cost` | 卖出费率 | `0.0013` |
| `benchmark` | 基准指数 | `SH000300` |
| `deal_price` | 执行价格模型 | `open`（真实） |

#### signal 配置

| 键 | 说明 | 默认值 |
|------|------|--------|
| `top_k` | 选股数量 | `10` |
| `method` | 信号方法 | `top_k` |
| `rebalance_freq` | 调仓周期(天) | `5` |
| `output_dir` | 信号输出目录 | `./output/signals` |
| `output_format` | 输出格式 | `csv` |

#### trading 配置

| 键 | 说明 | 默认值 |
|------|------|--------|
| `gateway` | 券商接口 | `xtp` |
| `paper_trading` | 模拟交易 | `true` |
| `stop_loss_pct` | 止损线 | `0.08` |
| `take_profit_pct` | 止盈线 | `0.15` |

#### risk 配置

| 键 | 说明 | 默认值 |
|------|------|--------|
| `max_drawdown` | 最大回撤限制 | `0.15` |
| `max_daily_loss` | 日亏损限制 | `0.03` |
| `max_concentration` | 集中度限制 | `0.10` |
| `circuit_breaker_loss` | 熔断阈值 | `0.08` |

---

## 12. 日常运维

### 12.1 每日自动化流程

```
每天（收盘后）:
  16:30  更新数据    py main.py download --source akshare --append
  17:00  生成信号    py main.py signal --model-path output/models/model_lgbm.pkl
  全天    Web 监控   py main.py dashboard (持续运行)

每周（调仓日）:
  17:30  打开面板「调仓操作」页面
         → 输入总资金 → 生成调仓计划 → 确认后一键录入
         → 到券商APP照着计划手动下单

定期（每月初）:
  py main.py train --universe csi300 --top-k 10
  py main.py backtest --model-path output/models/model_lgbm.pkl
```

> **为什么不每天训练？** 模型训练耗时较长，而 12 年训练集多加 1 天数据（<0.01%）对模型参数几乎无影响。日常只需用**已有模型**对最新因子做推理即可。建议每周或每月重新训练，或当 IC 明显衰减时触发训练。

### 12.2 Cron 配置示例

```cron
# ─── 每天（周一到周五）───

# 16:30 增量更新数据
30 16 * * 1-5 cd ~/py/stopat30m && .venv/bin/python main.py download --source akshare --append >> output/logs/cron.log 2>&1

# 17:00 用现有模型生成信号
0 17 * * 1-5 cd ~/py/stopat30m && .venv/bin/python main.py signal --model-path output/models/model_lgbm.pkl --publish >> output/logs/cron.log 2>&1

# ─── 每周六重新训练 ───

0 10 * * 6 cd ~/py/stopat30m && .venv/bin/python main.py train --universe csi300 >> output/logs/cron_train.log 2>&1
```

### 12.3 日志

日志存储在 `output/logs/stopat30m.log`，配置：

```yaml
logging:
  level: "INFO"
  file: "./output/logs/stopat30m.log"
  rotation: "10 MB"     # 单文件最大 10MB
  retention: "30 days"   # 保留 30 天
```

### 12.4 模型版本管理

建议用日期命名保存模型：

```bash
py main.py train --save-name model_20250312
```

保留最近 N 个模型用于回溯比较。

---

## 13. 常见问题 FAQ

### Q1: 下载数据报错

**症状**：`py main.py download` 失败

**解决**：

| 数据源 | 常见问题 | 解决方案 |
|--------|----------|----------|
| qlib | 网络不通 | Qlib 数据托管在 Azure，可能需要代理 |
| akshare | ConnectionError | 东方财富接口偶尔不稳定，重试即可 |
| akshare | 下载中断 | 重新运行即可（会覆盖已有数据） |
| tushare | token 无效 | 检查 config.yaml 中 tushare.token 配置 |
| tushare | 频率限制 | 基础会员 500 次/分钟，脚本已内置延迟 |

如 Qlib 源不可用，**推荐切换到 AKShare**：

```bash
py main.py download --source akshare --start-date 2015-01-01
```

### Q2: 训练时内存不足

**症状**：OOM (Out of Memory) 错误

**解决**：
1. 减少因子数量：`--factor-groups momentum,technical,volatility`
2. 缩短训练时间窗口
3. 减小模型复杂度（如 `num_leaves: 128`）
4. 使用更大内存的机器

### Q3: IC 很低或为负

**症状**：IC < 0.01 或 IC < 0

**诊断**：
1. 检查数据时间范围是否覆盖不同市场环境
2. 检查是否存在未来数据泄露（label 设置）
3. 模型可能过拟合 → 减小 num_leaves，增加正则化
4. 尝试不同因子组合

### Q4: vn.py 连接失败

**症状**：Gateway 连接报错

**解决**：
1. 确认 vnpy 和 gateway 包已安装：`pip install vnpy vnpy-xtp`
2. 确认券商参数正确（账号、密码、服务器）
3. 确认网络可达（券商服务器通常在内网）
4. 先用模拟交易模式验证系统

### Q5: Redis 连接失败

**症状**：`Connection refused`

**解决**：
1. 启动 Redis：`redis-server`
2. 确认 Redis 地址和端口：`redis-cli ping`
3. 不使用 Redis 也可以正常工作，改用 CSV 文件传递信号

### Q6: 如何更换股票池

修改 `config.yaml`：

```yaml
data:
  universe: "csi500"   # 改为沪深500
```

可选值：`csi300`（沪深300）、`csi500`（中证500）、`csi800`、`all`（全市场）

### Q7: 如何修改预测目标

默认预测未来 5 天收益率。修改 `config.yaml`：

```yaml
factors:
  # 预测未来 10 天收益
  label: "Ref($close,-10)/$close - 1"

  # 预测未来 1 天收益
  label: "Ref($close,-1)/$close - 1"

  # 预测未来 20 天收益
  label: "Ref($close,-20)/$close - 1"
```

### Q8: 如何增加自定义因子

在 `stopat30m/factors/expressions.py` 中添加新函数：

```python
def my_custom_factors() -> list[tuple[str, str]]:
    factors = []
    factors.append((
        "your_qlib_expression_here",
        "MY_FACTOR_NAME",
    ))
    return factors
```

然后在文件底部的 `_ALL_GROUPS` 字典中注册：

```python
_ALL_GROUPS: dict[str, callable] = {
    ...
    "my_custom": my_custom_factors,
}
```

---

## 14. 架构说明

### 14.1 系统数据流

```
          ┌──────────┐ ┌──────────┐ ┌──────────┐
          │  AKShare  │ │ Tushare  │ │  Qlib    │
          │  (免费)   │ │  (付费)  │ │ (公开)   │
          └────┬─────┘ └────┬─────┘ └────┬─────┘
               └────────────┼────────────┘
                     ┌──────▼───────────┐
                     │  Qlib 数据格式    │
                     │  OHLCV + 复权    │
                     └────────┬─────────┘
                              │
                     ┌────────▼─────────┐
                     │   因子引擎        │
                     │  530 个特征      │
                     │  Alpha158 + 扩展  │
                     └────────┬─────────┘
                              │
                     ┌────────▼─────────┐
                     │   ML 模型        │
                     │  LightGBM 等     │
                     │  predict: score  │
                     └────────┬─────────┘
                              │
              ┌───────────────┼───────────────┐
              │               │               │
       ┌──────▼──────┐ ┌─────▼──────┐ ┌──────▼──────┐
       │  CSV 文件    │ │   Redis    │ │   MySQL     │
       │  (回测/手动) │ │  (实时)    │ │  (持久化)   │
       └──────┬──────┘ └─────┬──────┘ └──────┬──────┘
              │               │               │
              └───────────────┼───────────────┘
                              │
                     ┌────────▼─────────┐
                     │   风控引擎        │
                     │  订单审核        │
                     │  止损 / 熔断     │
                     └────────┬─────────┘
                              │
                     ┌────────▼─────────┐
                     │   vn.py 交易     │
                     │  XTP / CTP       │
                     │  Portfolio策略   │
                     └────────┬─────────┘
                              │
                     ┌────────▼─────────┐
                     │   券商 / 交易所   │
                     └──────────────────┘
```

### 14.2 模块依赖关系

```
config.py          ← 所有模块依赖
  ↓
data/provider.py   ← 数据下载编排、Qlib 初始化
data/fetcher.py    ← AKShare/Tushare 抓取 + Qlib 格式转换
  ↓
factors/           ← 依赖 config
  ↓
model/             ← 依赖 factors, data
  ↓
backtest/          ← 依赖 model, config, evaluator
  ↓
signal/            ← 依赖 config
  ↓
trading/           ← 依赖 signal, config
  ↓
web/               ← 依赖 config, factors (只读)
```

### 14.3 关键设计决策

| 决策 | 选择 | 理由 |
|------|------|------|
| 因子定义 | Qlib 表达式字符串 | 统一计算引擎，避免手动实现 |
| 模型框架 | Qlib Model Zoo | 标准接口，易切换模型 |
| 信号传递 | CSV + Redis 双通道 | 兼顾回测和实时 |
| 交易引擎 | vn.py | A股生态最成熟 |
| 风控 | 同步前置检查 | 每笔必过，不可绕过 |
| 监控 | Streamlit | 快速开发，Python 原生 |
| 配置 | YAML + 环境变量 | 敏感信息不入文件 |

---

*最后更新：2026-03-13*
