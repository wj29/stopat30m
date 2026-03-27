# StopAt30M 命令速查

## 数据管理

```bash
# 增量更新（默认，多源并行：BaoStock 4进程 + AkShare 1进程）
# 自动从 data_meta.json 中每只股票的 data_end 开始拉取到今天
py main.py download

# 全量重建（先下载 Qlib 官方基底数据 ~2020-09，再用 BaoStock 补到今天）
# 等价于 --source qlib+baostock
py main.py download --full

# 禁用多源并行，仅使用 --source 指定的单一数据源
# 默认源为 baostock，可配合 --source 切换
py main.py download --single-source
py main.py download --single-source --source akshare

# 指定数据源
# 可选值: baostock | akshare | tushare | qlib | qlib+baostock
#   baostock      — 免费，无限流，默认源，支持多进程并行
#   akshare       — 免费，有频率限制 (~1req/s)
#   tushare       — 需要在 config.yaml 配置 tushare.token
#   qlib          — 仅下载 Qlib 官方基底数据（截止 ~2020-09）
#   qlib+baostock — 先 qlib 基底再 baostock 补全（等价于 --full）
py main.py download --source akshare

# 控制 BaoStock 子进程数（默认 4，其他源固定为 1）
py main.py download --workers 2
py main.py download --workers 1   # 禁用进程内并行

# 更新到指定日期（默认今天）
py main.py download --end-date 2026-03-01

# 重建 data_meta.json（不下载数据，仅扫描本地文件 + 拉取股票列表）
# 适用场景：meta 文件丢失或损坏、首次从 Qlib 基底开始使用
py main.py download --rebuild-meta

# 数据健康检查（水位线、覆盖范围、缺失诊断）
py main.py check-data

# 数据健康检查 + 清理空目录
py main.py check-data --fix
```

## 模型训练

```bash
# 使用默认配置训练（模型类型从 config.yaml 读取，默认 LightGBM）
py main.py train

# 训练后直接输出 Top N 预测股票
py main.py train --top-k 10

# 指定模型类型
# 可选值: lgbm | xgboost | mlp | lstm | transformer
py main.py train --model-type lgbm
py main.py train --model-type xgboost

# 指定股票池
# 可选值: csi300 | csi500 | all
py main.py train --universe csi300

# 自定义模型保存名称（保存到 output/models/{save_name}_{model_type}.pkl）
py main.py train --save-name model_v2

# 指定因子组（逗号分隔，可用 `py main.py info` 查看可用因子组）
py main.py train --factor-groups momentum,volatility,volume
```

## 回测

```bash
# 基础回测（参数从 config.yaml 读取）
py main.py backtest --model-path output/models/model_lgbm.pkl

# 指定持仓数量和换仓频率（天）
py main.py backtest --model-path output/models/model_lgbm.pkl --top-k 10 --rebalance-freq 5

# 指定成交价格
# 可选值: open | close
#   open  — 次日开盘价成交，更贴近实盘（推荐）
#   close — 当日收盘价成交，乐观估计
py main.py backtest --model-path output/models/model_lgbm.pkl --deal-price open

# 指定股票池
# 可选值: csi300 | csi500 | all
py main.py backtest --model-path output/models/model_lgbm.pkl --universe csi300

# 指定因子组
py main.py backtest --model-path output/models/model_lgbm.pkl --factor-groups momentum,volatility
```

## 信号生成

```bash
# 生成最新交易信号（输出到 output/signals/）
py main.py signal --model-path output/models/model_lgbm.pkl

# 指定日期生成信号
py main.py signal --model-path output/models/model_lgbm.pkl --date 2026-03-12

# 生成并推送到 Redis
py main.py signal --model-path output/models/model_lgbm.pkl --publish
```

## 交易引擎

```bash
# 启动交易引擎（轮询信号目录）
py main.py trade

# 指定信号源目录和轮询间隔（秒）
py main.py trade --signal-source output/signals/ --poll-interval 30
```

## Dashboard

```bash
# 启动 Web 监控面板（Streamlit）
# 包含：概览、调仓操作、交易记录、持仓管理、信号历史、模型评估、风控监控、因子分析
py main.py dashboard
```

## 其他

```bash
# 查看因子库统计（Alpha158 基础因子 + 扩展因子组）
py main.py info

# 使用自定义配置文件（默认读取项目根目录 config.yaml）
py main.py --config path/to/config.yaml download

# 查看帮助
py main.py --help              # 所有命令
py main.py download --help     # download 命令参数
py main.py train --help        # train 命令参数
py main.py backtest --help     # backtest 命令参数
py main.py signal --help       # signal 命令参数
```

## 典型工作流

### 首次使用

```bash
py main.py download --full          # 1. 全量下载数据
py main.py check-data               # 2. 检查数据完整性
py main.py train --top-k 10         # 3. 训练模型
py main.py backtest \               # 4. 回测验证
    --model-path output/models/model_lgbm.pkl \
    --deal-price open
py main.py signal \                 # 5. 生成交易信号
    --model-path output/models/model_lgbm.pkl
py main.py dashboard                # 6. 启动面板查看结果、执行调仓
```

### 日常更新（每个交易日收盘后）

```bash
py main.py download                 # 增量更新行情（多源并行，约 10~15 分钟）
py main.py signal \                 # 生成新信号
    --model-path output/models/model_lgbm.pkl
py main.py dashboard                # 查看信号、执行调仓
```
