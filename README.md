# StopAt30M

个人 A 股研究与交易工作台。基于 Microsoft Qlib 进行因子研究和模型训练，以 React/FastAPI 提供账户、数据检查与回测页面；保留旧版 Streamlit 和 vn.py 适配器，真实券商执行仍需单独验证。

当前正在按个人研究与交易工作台改进，具体问题、优先级、收益评估口径和实施状态见 [改进方案](docs/IMPROVEMENT_PLAN.md)。策略收益需要独立验证，20% 年化仅是研究目标。

主研究方向为 **A 股个股**：Qlib 格式历史基座、逐股票增量接缝审计、官方 Alpha158 与固定 LightGBM 的时间分离对照，见 [研究工作流](docs/RESEARCH_WORKFLOW.md#当前主线a-股个股与-qlib-历史基座)。`python main.py research --help` 查看 `ashare-base / ashare-collect / ashare-update / ashare-rebuild / ashare-run / ashare-resume`。此前三 ETF 是独立的工程验证，已停用其自动任务，不作为个股策略结果。

新的研究范围收敛为**沪深300与中证1000**，当前成分、历史代码有效日期修复和范围审计见[股票池范围](docs/ASHARE_SCOPE.md)。现有活动模型与已冻结验证只覆盖沪深300子集；中证1000尚不能沿用其收益结论。各行情、成分及规则来源的实际接入和验证状态见[数据来源说明](docs/ASHARE_DATA_SOURCES.md)。

A2阶段增加36只历史修复、原价整数股/现金核算、涨跌停、整手、T+1与最低佣金，并独立验证分红送转和每日模拟数据。实际结果、启动门禁与运行命令见 [A2执行验证](docs/A2_EXECUTION_VALIDATION.md)。`ashare-validate`核对历史对照，`ashare-forward-status`查看独立模拟盘扣费收益、回撤和运行失败；该账本与网页个人账户分开。

收益诊断见 [A3收益、成本与模型诊断](docs/A3_PROFIT_DIAGNOSTICS.md)。随后已补齐2023—2024年执行证据并完成12组含费对照，结果与策略决定见[A4历史验证](docs/A4_HISTORICAL_VALIDATION.md)：按预先门槛推荐M1_E2，当前模拟盘保持原基线；2024用于选优，2025—2026也已查看，新候选仍需独立前瞻冻结与观察。

## 个人工作台

网页默认关闭公开注册。数据库尚无用户时，在服务器本机打开 <http://127.0.0.1:8000>，按首次设置页面自行创建管理员；固定登录签名会安全生成并持久化到被 Git 忽略的 `config.local.yaml`，已有签名不覆盖。首次设置只允许本机、同源访问，已有账户继续使用原密码。远程初始化等场景可回退到交互命令 `.venv/bin/python main.py user create-admin`。

首次进入「持仓与账本」时设置账户起始日期和初始资金，再按时间顺序录入资金变动及成交。这里记录成交，不会向券商下单。旧版 CSV、JSON 及旧交易表保留为遗留记录，不自动并入新账户；录入前先核对归属和初始资金，避免重复记账。

点击「保存当日估值」可记录当时的账户资产，保留行情来源、时间和账本版本；持仓报价不完整时拒绝保存完整估值。该观察估值不是官方每日收盘净值，也不是剔除资金流影响的时间加权收益率（TWR）。券商 CSV 只用于只读核对，不会自动导入或覆盖现金、持仓与成交。

「系统设置」包含系统配置、历史研究数据、量化模型维护和账户安全。系统配置可保存研究下载偏好、新闻搜索、传统回测默认值及新模拟引擎的风险比例；原下载区域仍是固定 ETF 研究入口。旧 Qlib 直接拼接更新已停用，个股研究使用下方新的 `ashare-*` 命令；本轮没有扩展页面。

侧栏「模型配置」（`/model-settings`）独立管理 AI 服务。下拉选择 OpenAI / 兼容服务、Claude、DeepSeek、Gemini、Ollama 或 AIHubMix，只显示对应字段，并标明已配置、未配置与当前启用状态。「仅保存连接配置」保存该服务商的密钥和地址，保持当前使用的服务商与模型；「保存并使用」才切换服务商和模型。切换服务商保留其他家的凭据，密钥不回显、留空保留，环境变量管理的字段只读。

OpenAI 兼容服务可明确选择 Chat Completions 或 Responses 协议。先保存，再点击「检查已保存的模型配置」读取当前服务商模型列表；这个按钮不发送生成请求，列表中存在模型也不代表生成、额度与全部参数已验证。「系统设置 → 系统配置 → 新闻搜索」可配置搜索密钥、自建 SearXNG 地址与新闻时间窗口；公共实例发现默认关闭。余额不足或服务拒绝会提示并冷却，报告明确披露新闻缺失，不能将检索失败解释为没有市场事件。

「回测验证」默认打开「样本外研究」。选择通过校验且匹配锁定协议的数据快照后，可启动固定规则研究，查看净值、回撤、开发/验证/最终样本外表现、观察分段和分页成交明细。命令行保存在默认研究目录的结果也会自动列出；没有完整匹配数据时，页面明确显示尚不可运行，不生成演示收益。数据文件存在不等于最新行情；缺价和不完整估值不会按零元价格计算收益。

网页与复盘环境使用独立依赖清单，包含行情、AI 和报告组件；`requirements-test.txt` 只用于离线测试，不能代替网页运行依赖。新环境先安装，再构建前端，由 FastAPI 同时提供页面和 API：

```bash
.venv/bin/python -m pip install -r requirements-web.txt
cd stopat30m/web/frontend
npm ci
npm run build
cd ../../..
.venv/bin/python main.py serve --host 127.0.0.1 --port 8000
```

浏览器打开 <http://127.0.0.1:8000>。当前工作区的 `.venv` 已准备好时，可直接执行上面的启动命令；研究下载、配置和备份入口只向管理员开放。不使用 `.venv` 的安装环境可改用对应的 Python 解释器。

研究任务状态保存在 `output/research/jobs/`，刷新页面后仍可查询。服务重启后遗留任务会标记为已中断，不会自动重新联网下载或把残留文件当作完成；同日缓存可通过再次提交下载复用，跨日需明确重新采集。

配置按 `config.yaml` → 同目录 `config.local.yaml` → `STOPAT30M_SECTION__KEY` 环境变量覆盖。网页只允许修改白名单，采用文件锁、revision 并发检查、0600 权限的原子本地保存及变更审计；保留其他本地项和凭据。环境覆盖字段只读。密钥不回显，空输入保留原值，显式选择清除才删除；密钥和服务地址变更显示重启提示，普通参数供后续任务或新实例使用。模拟引擎风险参数不是手工成交账本的拦截规则，网页不提供切换实盘网关、关闭模拟交易或任意修改认证/数据库路径的开关。

API 密钥和行情 Token 应放在忽略的本地配置或环境变量里，不要提交到仓库。已进入历史提交的凭据仍需到服务商轮换，清空当前文件不等于从历史中撤回。远程部署可用 `STOPAT30M_AUTH__SECRET_KEY` 提供固定签名，并持久保存数据库、本地配置和所需输出。默认个人模式可在 `auth.single_user` 调整；多人使用还需显式启用 `auth.registration_enabled`，原邀请码校验仍保留。

「运行与备份」检查本地依赖、数据库、个人账户及研究输入，不会自动连接外部服务。管理员可创建并下载经过校验的 SQLite 一致性全库备份，包含用户密码哈希、账户、分析与聊天等数据库内容；不包含 `config.local.yaml`、行情、模型或研究文件。网页没有恢复或覆盖账户入口，下载的备份应妥善保管。

大盘复盘的进度连接与普通接口使用同一登录凭据。若会话过期，重新登录后可查看已在后台运行的任务，不必重复触发。页面显示的 AI 上游鉴权失败属于模型服务配置问题，应到「模型配置」核对对应密钥与服务地址。

此前网页验收覆盖首次设置、登录、配置保存及研究下载默认值生效、开户、观察估值、只读核对、备份创建校验、复盘认证进度与模型列表检查。当前个股研究已经实际下载并审计 Qlib 历史包，完成官方 Alpha158/LightGBM 训练；新的数据、模型和结果显式保存在 `output/research/ashare/`，尚未接入原 ETF 网页结果列表。网页验收和离线测试均不构成策略收益证据，最新研究状态见 [改进方案](docs/IMPROVEMENT_PLAN.md)。

核心回归使用离线合成数据，可独立于真实行情和模型运行：

```bash
python -m pip install -r requirements-test.txt
python -m pytest tests -q
```

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
 │  个人工作台    │  React / FastAPI / SQLite 账户账本
 │              │  保留旧版 Streamlit
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
│   │   ├── portfolio.py           # 个人账户、资金流与成交核算
│   │   ├── engine.py              # vn.py 引擎封装
│   │   ├── strategy.py            # Alpha 组合策略
│   │   ├── risk.py                # 风控引擎
│   │   └── ledger.py              # 手动交易记录 (CSV 存储, 持仓/盈亏计算)
│   ├── api/                      # FastAPI 认证、账户、数据与研究接口
│   └── web/
│       ├── frontend/             # React 个人工作台
│       └── dashboard.py           # 旧版 Streamlit 监控面板
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

### 2. 获取并核验个股历史基座

```bash
.venv/bin/python -m pip install -r requirements-qlib.txt
.venv/bin/python main.py research ashare-base
```

固定来源和整包 SHA 在 `docs/research/a1_qlib_source.json`；目前使用 Qlib 官方推荐的社区格式数据，不能称为微软官方行情包，也不假设每只股票都覆盖到日历末日。成功后命令打印不可变数据目录和清单。旧 `download --source qlib/qlib+baostock` 直接下载/混拼入口已停用。增量采集与逐字段桥接命令见研究工作流；不通过校验就不能推进数据版本。

### 3. 官方 Alpha158 个股训练与样本外对照

```bash
.venv/bin/python main.py research ashare-run \
  --snapshot <ashare-base输出的快照目录> \
  --spec docs/research/a1_ashare_experiment.json \
  --corporate-actions docs/research/a1_ashare_corporate_actions_v2.json \
  --output-dir <新的实验目录>
```

新实验使用包内声明的沪深300历史成员区间，训练2012—2022、验证2023—2024、回测2025至2026-10-16。使用下一交易日开盘的5日标签，按6个真实交易日清除跨边界样本；全部参数、数据版本和费用先固定。模型、动量与等权承担同一费用，结果保存在独立目录；包内历史区间尚不等于公告时点已全面核验的 PIT 数据。公司行为文件引用本机 `output` 中的官方公告提取证据，复制项目时需保留这些证据；缺失时拒绝运行，不能把未下载的原 PDF 当作已校验。命令与核验方式见研究工作流。

原扩展因子与多模型链路仍保留，属于另一实验配置，不能与本轮结果混称：

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

### 6. 启动交易

```bash
# 模拟交易 (读取信号文件)
py main.py trade --signal-source ./output/signals/

# Redis 实时信号模式
py main.py trade
```

### 7. 监控面板

```bash
py main.py dashboard
# 或直接: streamlit run stopat30m/web/dashboard.py
```

### 8. 查看因子库

```bash
py main.py info
```

## 配置

非敏感默认配置在 `config.yaml`；常用白名单参数优先在「系统设置 → 系统配置」保存，AI 服务使用独立的「模型配置」页面，均写入本地覆盖文件。高级部署仍支持环境变量覆盖：

```bash
# 格式: STOPAT30M_SECTION__KEY=value
export STOPAT30M_TRADING__ACCOUNT=myaccount
export STOPAT30M_AUTH__SECRET_KEY="<自己的固定随机签名>"
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

- **最大回撤**: 超过阈值触发熔断，暂停增仓，允许合规减仓
- **日亏损限制**: 当日亏损超限触发熔断
- **单笔止损/止盈**: 8% / 15%
- **持仓集中度**: 单只不超过 10%
- **总仓位上限**: 95%
- **订单级校验**: 每笔下单前经过风控引擎审核

以上规则用于对应模拟/策略引擎实例；个人手工成交账本负责如实记录成交，不把这些参数当作录入拦截规则。

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

IC 衡量预测与结果的相关性，不能单独证明存在可交易收益；还需要包含成本的样本外回测与风险检验。

## 日常运维流程

```
每天（收盘后）:
  1. 更新数据     py main.py download
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
- **工作台**: React / FastAPI；保留旧版 Streamlit
- **配置**: 网页白名单 + 本地 YAML + 环境变量
- **日志**: Loguru
