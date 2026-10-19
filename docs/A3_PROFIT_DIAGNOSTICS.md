# A3：收益、成本与模型诊断

截至2026年10月19日，最明显的问题是**换股频繁、最低佣金和低价股的实际滑点**。减少换股后的历史表现有所改善，但五年滚动训练没有提供稳定的替换依据；本轮未选出新策略，也没有把研究候选切换到每日模拟盘。

本轮代码基线为`116d7fc`。评估区间为2025-01-02至2026-10-16，共431个交易日，每个账户初始资金10万元。**这段历史在A2阶段已经看过，以下全部属于历史复核和敏感性分析，不是新的样本外验证。**20%年化仍是研究目标。

## 先回答：收益被什么拖累

原LightGBM账本的净利润为6,281.25元。保持原来每一笔数量和后续持仓不变，按参考成交价归因，费用及滑点之前的利润为26,321.49元；实际交易摩擦合计20,040.24元，约占该利润的76.14%。

| 项目 | 整段金额（元） | 核对结果 |
| --- | ---: | --- |
| 佣金 | 8,910.00 | 1,782笔全部按每笔最低5元计；逐笔比例佣金分币取整后仅2,395.22元，最低佣金额外增加6,514.78元 |
| 实际滑点 | 9,063.14 | 按参考成交额计为11.35bp；声明的2bp在不利方向按一分钱取整后，实际成本明显增加 |
| 卖出印花税 | 1,986.95 | 只在卖出计提 |
| 过户费 | 80.15 | 买卖分别按成交额计提 |
| 净利润 | 6,281.25 | 滑点已包含在成交价格里，没有再作为现金费用重复扣减 |

这张表是**固定账本的会计归因**：节省的成本没有再投入，也没有重新改变现金、整手约束和后续持仓。后文的成本敏感性实验才是从头重新执行的完整路径，两者不能混用。

换手来自真正换股。原策略每5个交易日选20只股票，平均每次更换12.62只，目标留存率只有36.92%。原账本中884笔属于新目标买入、875笔属于退出目标卖出；同名实际持仓调整只有21笔，相关摩擦126.16元。成交金额低于100元的订单为零，低于1,000元只有16笔，反复微额调仓不是主要损失来源。

原策略累计双边成交额798.39万元，是平均净资产的76.42倍；平均非现金敞口70.40%。低于10元的股票占成交额48.81%，承担了约76.61%的滑点。一分钱对低价股的影响很大：5元以下成交的实际滑点约27.75bp，5—10元约14.56bp。因此本轮保持真实取整规则和费用假设，优先检验减少换股。

## 六组候选完整保留

预先声明的候选为两个模型乘以三种执行方式，没有增加参数搜索：

- **M0**：保留2012—2022年训练的原固定模型，原生权重及预处理统计量不重新拟合。
- **M1**：每年使用此前五个完整自然年的历史训练。2023/2024/2025/2026年分别训练2018—2022、2019—2023、2020—2024、2021—2025，统一剔除截止日前尚未成熟的6个信号交易日。全部拟合实际发生在2026年，未伪造当年的模型冻结时间。
- **E0**：原20只等权目标，每5个交易日重平衡。
- **E1**：仍以20只为目标；实际持仓只要仍位于前40名就保留原股数，退出后再按前一收盘信号补入新股票。
- **E2**：在E1上增加前一收盘时的整手与费用预算筛选。次日开盘的缺价、涨跌停或资金不足只能减少成交，不用后来的行情替换已冻结名单。

减少排名边界附近换股的方向参考了[Qlib官方TopkDropoutStrategy说明](https://qlib.readthedocs.io/en/latest/component/strategy.html#topkdropoutstrategy)。这里实现的是固定前40名保留规则，不能把它当作官方策略收益背书。

| 候选 | 年化收益 | 累计收益 | 最大回撤 | 成交笔数 | 平均非现金敞口 | 不完整估值天数 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| M0_E0：原基线 | 3.63% | 6.28% | -11.63% | 1,782 | 70.40% | 0 |
| M0_E1：排名缓冲 | 6.06% | 10.60% | -12.99% | 1,293 | 73.83% | 0 |
| M0_E2：缓冲＋可负担筛选 | 5.58% | 9.74% | -13.47% | 1,596 | 86.76% | 0 |
| M1_E0：滚动模型，暂估 | -4.91% | -8.25% | -12.26% | 1,450 | 72.37% | 5 |
| M1_E1：滚动＋缓冲，暂估 | -0.15% | -0.26% | -9.32% | 973 | 76.48% | 5 |
| M1_E2：滚动＋筛选，暂估 | -1.88% | -3.20% | -13.73% | 1,160 | 87.17% | 5 |

M0_E1的交易摩擦由20,040.24元降至14,613.91元，净利润增加4,313.87元。不过持仓、平均敞口和回撤同时改变：回撤从11.63%扩大到12.99%。这项结果不能简化为“省掉费用后收益必然增加”，也不足以选出可部署赢家。E2提高了仓位，但其收益低于E1，仍需独立验证资金配置与选股的共同影响。

M1三组都持有100股藏格矿业（SZ000408），在2025-01-10、01-13、01-14、01-15、01-16这5天遇到真实停牌。原始来源明确记录`tradestatus=0`，账本沿用1月9日最后观察到的29.50元收盘价，保持`valuation_complete=false`，停牌期间没有成交；1月17日才重新观察到交易。这5天不是未知数据缺口，但旧价格也不是当天新鲜可执行估值。因此保留上述**暂估指标**，不将M1列为已通过完整估值验证、可与M0精确比较的收益结果。

逐日持仓、来源SHA和原公告交叉核对见[停牌审计（本地产物）](../output/research/ashare/analysis/a3_20261019_rolling_audit.json)。公告包括[1月10日停牌](https://static.cninfo.com.cn/finalpage/2025-01-10/1222286144.PDF)、[1月14日继续停牌](https://static.cninfo.com.cn/finalpage/2025-01-14/1222322233.PDF)及[1月17日复牌](https://static.cninfo.com.cn/finalpage/2025-01-17/1222355440.PDF)。原公告通过网页PDF文本核对，未保存或声明原PDF字节哈希；盘中公开时间未核定，不能利用事后公告删除1月8日信号或1月9日买入。

## 完整重跑的成本敏感性

以下保持E0信号、原模型、整手、涨跌停及现金约束，只改变列出的执行成本。从初始现金重新计算，所以成交数量、资金与后续路径可能变化。它们是诊断实验，不是可以部署的降费方案。

| 成本情景 | 年化收益 | 累计收益 | 最大回撤 | 成交笔数 | 佣金（元） | 滑点（元） |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 原费用 | 3.63% | 6.28% | -11.63% | 1,782 | 8,910.00 | 9,063.14 |
| 去掉最低佣金，仍按3bp计佣 | 8.76% | 15.44% | -10.60% | 1,804 | 2,521.63 | 9,442.64 |
| 去掉滑点，其余费用保留 | 9.96% | 17.63% | -9.33% | 1,803 | 9,015.00 | 0.00 |
| 执行摩擦全部设零 | 14.47% | 26.01% | -8.18% | 1,831 | 0.00 | 0.00 |

最后一组去掉佣金、过户费、交易印花税和滑点，仍保留分红税预留及原来的交易限制。**该情景年化14.47%，仍未达到20%。**这个结果说明，仅降低本轮成本尚不能证明目标可达；它也不是其他模型或策略的收益上限。

## 模型更新有没有帮助

官方Alpha158、固定LightGBM参数和100轮训练保持相同，只检验五年年度滚动。按真实下一开盘所属年份选择对应模型折，信号到下一开盘建仓，再观察5个交易日后的调整价开盘收益。下面是跨股票的预测统计，未扣成本，也不代表整手账户净值。

| 执行年份 | M0 RankIC | M1 RankIC | M0前20名相对样本均值的5日收益 | M1对应收益 |
| --- | ---: | ---: | ---: | ---: |
| 2023 | 0.0544 | 0.0377 | +0.3870% | +0.2736% |
| 2024 | 0.1030 | 0.0978 | +0.6011% | +0.5501% |
| 2025，已看历史 | 0.0499 | 0.0301 | +0.1026% | -0.1890% |
| 2026，已看历史 | 0.0748 | 0.0845 | +0.8379% | +0.5024% |

滚动模型没有表现出稳定的预测提升，其暂估扣费结果也没有提供替换原模型的依据。不要仅因为“训练得更新”就替换模型。逐日统计保留缺失标签数量和重叠收益的Newey-West均值区间，见[预测对照（本地产物）](../output/research/ashare/analysis/a3_20261019_rolling_predictive/predictive_comparison.json)。

四折模型共生成551,018行分数，预测文件SHA256为`44b80dbc1bfab998ee30ee6673891b0a35dff021b33b8ac8cb6d9670abad5cba`。原M0既有129,409行分数全部复现，最大绝对误差为**0.0**，没有放宽`1e-11`容差。原失败尝试与四折模型保留，新推理复用相同模型权重，没有为修复精度而重新挑参数；训练、推理和原生训练器的[源代码归档收据（本地产物）](../output/research/ashare/analysis/a3_20261019/source_provenance/receipt.json)与各阶段SHA匹配。

## 仍然不能选赢家的原因

协议要求先完成2023年开发期与2024年验证期的真实执行、分红送转和当时规则核对，再按2024年扣费表现选择。本轮没有取得这些完整证据，选择状态保持：

`blocked_required_2023_2024_execution_data_and_historical_rules_not_verified`

历史包的特征和价格可用于训练及上述预测诊断，但不能补出当时的`preclose`除权参考、ST、停复牌和完整公司行动。包内`change`也不能代替交易所除权参考；前期规则及2023年印花税调整需按实际生效日核对。[数据就绪审计（本地产物）](../output/research/ashare/analysis/a3_20261019_data_readiness/report.json)保留了350只历史成分股、484个源交易日和90个成分股交易日不可用记录，未将它们伪装成可执行回测。

本轮实际尝试采集开发期数据时，BaoStock登录在8秒、8秒、30秒的三次有限重试中失败，记录了超时及`10002007`网络接收错误，未取得原始行情响应。失败记录与[官方传输接口核对](../output/research/ashare/collections/development_20261019_evidence/official_transport_review.json)保留在`collections/development_20261019_evidence`；所查官方网页接口提供文档内容，不能当作已经取得历史行情的替代证据。

后续优先补齐这些证据，再验证已声明的候选，不继续依据已看过的2025—2026年收益增加参数。所有组的完整指标保存在[机器摘要](research/a3_20261019_summary.json)，没有筛掉亏损组。当前也缺少可靠的同风险市场基准归因，不能据此宣称已经跑赢市场。

## 推理精度修复与每日模拟盘

研究时发现手工推理把原始`float32`特征提升为`float64`，改变了Qlib先减中位数、再除MAD的中间舍入。修复后保留原始dtype及计算顺序，处理后的数值和模型分数均与原生Qlib逐项相同；37项针对性测试通过。

真实2026-10-19的300只股票也已核对。当天旧实现与修复后的**300个分数、全部排名和20只目标名单没有变化**；这不能推断其他日期一定不受精度影响，所以仍新冻结模型包及模拟期，保留旧账本。新模型SHA与旧模型相同：`98bb0def3f432450c91f36333b92f84f7e9a5a192a03f25b91c84c4608089702`。

新期位于`output/research/ashare/forward/latest_20261019_precision_v2`，模型包于19:37:17冻结，训练样本1,049,136行。10月19日收盘已完成，300只股票可用；模型和动量账户各有20只目标、各10万元现金、零成交。**首次计划执行为10月20日09:35**。这次换期用于推理正确性修复，保持原执行策略，没有启用E1、E2或M1研究候选。

旧任务已停止并禁用，旧账本SHA保持不变；新任务为`com.stopat30m.asharepaper.7efe740fa41f`，已加载，首次原生任务退出码为0。已验收重复运行完成的收盘任务不访问数据源、不改变状态，新旧信号逐项相同。证据见[启动验收（本地产物）](../output/research/ashare/forward/latest_20261019_precision_v2/runner/startup_validation.json)及[任务注册收据](../output/research/ashare/forward/latest_20261019_precision_v2/runner/registration.json)。电脑需保持唤醒并登录；目前没有模拟收益可供评估，也不会提交真实券商订单。

```bash
.venv/bin/python main.py research ashare-forward-status \
  --paper-dir output/research/ashare/forward/latest_20261019_precision_v2
```

## 复现命令与产物

三类入口如下。输出目录必须不存在；已有结果保持不变。模型拟合命令可重新生成分数，执行复核可以直接使用本轮已冻结的`predictions_v2`。

```bash
A3_BASELINE=output/research/ashare/experiments/a2_20261019_integer_validation_v2
A3_TRAINING=output/research/ashare/experiments/a2_20261019_repaired_training
A3_SPEC=docs/research/a3_optimization_spec.json
A3_SNAPSHOT=output/research/ashare/updates/rebuild_36_resolved_20261019/versions/snapshots/538d05c141006b73f1297a4fc8ed43d57af21a5b53f9cfc5cbda6e436748c2ac
A3_PREDICTIONS=output/research/ashare/experiments/a3_20261019_predictions_v2

# 1. 原账本成本、换手及预测诊断
.venv/bin/python main.py research ashare-diagnose \
  --experiment-dir "$A3_BASELINE" --training-origin "$A3_TRAINING" \
  --output-dir output/research/ashare/analysis/a3_reproduce_diagnostics

# 2. 可选：按已声明参数重新拟合四折，不改现有模型
.venv/bin/python main.py research ashare-rolling-fit \
  --snapshot "$A3_SNAPSHOT" --baseline-origin "$A3_TRAINING" --spec "$A3_SPEC" \
  --output-dir output/research/ashare/experiments/a3_reproduce_predictions

# 3. 原模型三种执行、成本敏感性、滚动模型三种执行，分别保存
.venv/bin/python main.py research ashare-optimize-review \
  --baseline-dir "$A3_BASELINE" --spec "$A3_SPEC" \
  --output-dir output/research/ashare/experiments/a3_reproduce_execution
.venv/bin/python main.py research ashare-optimize-review \
  --baseline-dir "$A3_BASELINE" --spec "$A3_SPEC" --cost-ablation \
  --output-dir output/research/ashare/experiments/a3_reproduce_costs
.venv/bin/python main.py research ashare-optimize-review \
  --baseline-dir "$A3_BASELINE" --spec "$A3_SPEC" \
  --rolling-predictions-dir "$A3_PREDICTIONS" \
  --output-dir output/research/ashare/experiments/a3_reproduce_rolling
```

`--cost-ablation`与`--rolling-predictions-dir`互斥。每个结果均保存输入、源文件和结果SHA；命令只做研究，不选择赢家、不修改模拟盘。

本轮原始结果目录分别是`a3_20261019_execution_review_v2`、`a3_20261019_cost_ablation`和`a3_20261019_rolling_review`。原`execution_review`为开发过程中的中间产物，复现使用保留了对应实现源文件副本的`execution_review_v2`。另有[净值与回撤图（本地产物）](../output/research/ashare/analysis/a3_20261019/candidate_nav_drawdown.png)。本地`output/`产物不会自动随Git提交，关键数值与校验收据已写入仓库内的机器摘要。

本轮全套验收为990项测试通过、1项跳过、20项子测试通过；后续补充CLI对暂估结果的明确提示，相关CLI合计20项测试通过。`pip check`和差异检查通过。工程测试保证核算和流程可重复，不证明未来盈利。
