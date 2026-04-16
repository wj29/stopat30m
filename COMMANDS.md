# StopAt30M 命令速查

> 所有命令都在项目根目录下执行。首次使用前先跑 `bash setup_env.sh`，
> 之后每次开终端先 `source .venv/bin/activate`。

## 数据管理

```bash
# 增量更新（默认，多源并行：BaoStock 4进程 + AkShare 1进程）
# 自动从 data_meta.json 中每只股票的 data_end 开始拉取到今天
python main.py download

# 全量重建（先下载 Qlib 官方基底数据 ~2020-09，再用 BaoStock 补到今天）
# 等价于 --source qlib+baostock
python main.py download --full

# 禁用多源并行，仅使用 --source 指定的单一数据源
# 默认源为 baostock，可配合 --source 切换
python main.py download --single-source
python main.py download --single-source --source akshare

# 指定数据源
# 可选值: baostock | efinance | akshare | tushare | qlib | qlib+baostock
#   baostock      — 免费，无限流，默认源，支持多进程并行（权重5）
#   efinance      — 免费，东财后端，比akshare更稳定（权重3）
#   akshare       — 免费，有频率限制 (~1req/s)（权重2）
#   tushare       — 需要在 config.yaml 配置 tushare.token（权重2）
#   qlib          — 仅下载 Qlib 官方基底数据（截止 ~2020-09）
#   qlib+baostock — 先 qlib 基底再 baostock 补全（等价于 --full）
python main.py download --source efinance

# 控制 BaoStock 子进程数（默认 4，其他源固定为 1）
python main.py download --workers 2
python main.py download --workers 1   # 禁用进程内并行

# 更新到指定日期（默认今天）
python main.py download --end-date 2026-03-01

# 重建 data_meta.json（不下载数据，仅扫描本地文件 + 拉取股票列表）
# 适用场景：meta 文件丢失或损坏、首次从 Qlib 基底开始使用
python main.py download --rebuild-meta

# 数据健康检查（水位线、覆盖范围、缺失诊断、instruments 一致性）
python main.py check-data

# 数据健康检查 + 自动修复（重建 instruments、清理空目录）
# instruments 修复：从二进制文件的实际数据范围（最后一个非 NaN 值）重建
# instruments/all.txt，修复增量更新中断导致的 instruments 滞后问题
python main.py check-data --fix
```

## 模型训练

```bash
# 使用默认配置训练（模型类型从 config.yaml 读取，默认 LightGBM）
python main.py train

# 训练后直接输出 Top N 预测股票
python main.py train --top-k 10

# 指定模型类型
# 可选值: lgbm | xgboost | mlp | lstm | transformer
python main.py train --model-type lgbm
python main.py train --model-type xgboost

# 指定股票池
# 可选值: csi300 | csi500 | all
python main.py train --universe csi300

# 自定义模型保存名称（保存到 output/models/{save_name}_{model_type}.pkl）
python main.py train --save-name model_v2

# 指定因子组（逗号分隔，可用 `python main.py info` 查看可用因子组）
python main.py train --factor-groups momentum,volatility,volume
```

## 预测缓存

```bash
# 生成模型预测并缓存到磁盘，供后续回测复用（避免重复计算特征）
python main.py cache-predictions --model-path output/models/model_lgbm.pkl --tag v1

# 指定股票池和因子组
python main.py cache-predictions --model-path output/models/model_lgbm.pkl --universe csi300 --factor-groups momentum,volatility
```

## 回测

```bash
# 基础回测（参数从 config.yaml 读取）
python main.py backtest --model-path output/models/model_lgbm.pkl

# 从缓存的预测跑回测（推荐，避免重复计算特征）
python main.py backtest --pred-path output/predictions/predictions_xxx.pkl

# 指定持仓数量和换仓频率（天）
python main.py backtest --pred-path output/predictions/predictions_xxx.pkl --top-k 10 --rebalance-freq 5

# 指定成交价格
# 可选值: open | close
#   open  — 次日开盘价成交，更贴近实盘（推荐）
#   close — 当日收盘价成交，乐观估计
python main.py backtest --model-path output/models/model_lgbm.pkl --deal-price open

# 指定股票池
# 可选值: csi300 | csi500 | all
python main.py backtest --model-path output/models/model_lgbm.pkl --universe csi300

# 指定因子组
python main.py backtest --model-path output/models/model_lgbm.pkl --factor-groups momentum,volatility
```

## 信号回测

```bash
# 从模型直接跑信号回测
python main.py signal-backtest --model-path output/models/model_lgbm.pkl --tag test

# 从缓存的预测跑信号回测（推荐，避免重复计算特征）
python main.py signal-backtest --pred-path output/predictions/predictions_xxx.pkl --tag test

# 指定参数
python main.py signal-backtest --pred-path output/predictions/predictions_xxx.pkl \
    --top-k 10 --method top_k --rebalance-freq 5
```

## 账户回测

```bash
# 从模型直接跑账户回测
python main.py account-backtest --model-path output/models/model_lgbm.pkl --tag test

# 从缓存的预测跑账户回测（推荐）
python main.py account-backtest --pred-path output/predictions/predictions_xxx.pkl --tag test

# 开启真实摩擦模拟（滑点、部分成交、限价单）
python main.py account-backtest --pred-path output/predictions/predictions_xxx.pkl \
    --order-type limit --slippage-bps 5 --allow-partial-fill --participation-rate 0.1
```

## 信号生成

```bash
# 生成最新交易信号（输出到 output/signals/）
python main.py signal --model-path output/models/model_lgbm.pkl

# 指定日期生成信号
python main.py signal --model-path output/models/model_lgbm.pkl --date 2026-03-12

# 生成并推送到 Redis
python main.py signal --model-path output/models/model_lgbm.pkl --publish
```

## 交易引擎

```bash
# 启动交易引擎（轮询信号目录）
python main.py trade

# 指定信号源目录和轮询间隔（秒）
python main.py trade --signal-source output/signals/ --poll-interval 30
```

## Web 前端

```bash
# 一键启动：后端 + 前端开发服务器同时启动（推荐日常使用）
# 访问: http://localhost:5173
python main.py dev

# 只启动 FastAPI 后端（API 文档: http://localhost:8000/docs）
python main.py serve
python main.py serve --port 9000 --reload    # 自定义端口 + 自动重载

# 构建前端生产版本
python main.py build

# 生产部署（build 之后只需要一条命令，一个端口）
python main.py build && python main.py serve     # 访问 http://localhost:8000
```

## 模拟交易

```bash
# 初始化模拟交易账户
python main.py paper init --capital 1000000

# 查看模拟账户状态
python main.py paper status

# 结算（T+1 日结）
python main.py paper settle

# 重置账户
python main.py paper reset

# 导出交易记录
python main.py paper export
```

## 用户管理

```bash
# 创建管理员用户（交互式输入用户名和密码）
python main.py user create-admin

# 列出所有用户
python main.py user list

# 重置用户密码
python main.py user reset-password <username>

# 禁用用户
python main.py user disable <username>

# 生成邀请码（默认 7 天有效）
python main.py invite create
python main.py invite create --expires 30d
python main.py invite create --admin-user admin_name
```

## 数据库管理

```bash
# 初始化数据库 + 从旧版 CSV/JSON 迁移数据
python3 -m stopat30m.storage.migrate
```

## 其他

```bash
# 初始化环境（创建 .venv + 安装 Python/Node 依赖，任何新机器跑一次）
bash setup_env.sh
source .venv/bin/activate

# 首次一键启动（自动建环境 -> 全量下载 -> 训练 -> 产出信号 -> 构建前端）
./first_run.sh

# 查看因子库统计（Alpha158 基础因子 + 扩展因子组）
python main.py info

# 使用自定义配置文件（默认读取项目根目录 config.yaml）
python main.py --config path/to/config.yaml download

# 查看帮助
python main.py --help                  # 所有命令
python main.py download --help         # download 命令参数
python main.py train --help            # train 命令参数
python main.py backtest --help         # backtest 命令参数
python main.py signal --help           # signal 命令参数
python main.py signal-backtest --help  # 信号回测参数
python main.py account-backtest --help # 账户回测参数
python main.py serve --help            # API 服务参数
```

## 流水线说明

```
原始行情 (OHLCV)
     │
     ▼
 特征计算 ──── 将 K 线变成 629 个因子（最耗时，~12 分钟）
     │          例：MA 偏离度、动量、量比、波动率等
     ▼
 模型训练 ──── 学习"什么因子组合 → 未来涨"（~30 秒）
     │          输出: output/models/model_lgbm.pkl
     ▼
 模型预测 ──── 给每只股票每天打一个分（~30 秒）
     │          可用 cache-predictions 缓存到磁盘
     ▼
 ┌───┴────────────────────────┐
 │ 以下步骤可直接用缓存的预测  │
 │ 改参数不需要重算特征        │
 └───┬────────────────────────┘
     ├→ 信号生成 ── Top K 买入名单（瞬间）
     ├→ 回测     ── 模拟逐日换仓（~2 分钟）
     ├→ 信号回测 ── IC/RankIC/分组收益（~2 分钟）
     └→ 账户回测 ── 手续费/滑点/涨跌停（~2 分钟）
```

**什么时候必须重新训练？**
- 修改了 config.yaml 的 `data.train_start/train_end/valid_start/valid_end`
- 修改了因子组 (`factors` 配置)
- 修改了模型超参 (`model.params`)
- 更新了本地数据后想让模型学到新数据

**什么时候只需用缓存？**
- 调 `top_k`、`rebalance_freq`、`deal_price`、`slippage_bps` 等回测参数
- 对比不同持仓策略

## 典型工作流

### 首次使用

```bash
python main.py user create-admin        # 0. 创建管理员账号（首次必须）
python main.py download --full          # 1. 全量下载数据
python main.py check-data               # 2. 检查数据完整性
python main.py train --top-k 10         # 3. 训练模型（含特征计算，~15 分钟）
python main.py cache-predictions \      # 4. 缓存预测（后续回测直接复用）
    --model-path output/models/model_lgbm.pkl --tag v1
python main.py backtest \               # 5. 回测验证（用缓存，~2 分钟）
    --pred-path output/predictions/predictions_v1.pkl
python main.py signal \                 # 6. 生成交易信号（必须用模型，不能用缓存）
    --model-path output/models/model_lgbm.pkl  # 因为需要实时计算今天的因子和预测
python main.py dev                      # 7. 启动后端+前端，打开 http://localhost:5173
```

### 日常更新（每个交易日收盘后）

```bash
python main.py download                 # 增量更新行情（多源并行，约 10~15 分钟）
python main.py signal \                 # 生成新信号
    --model-path output/models/model_lgbm.pkl
python main.py dev                      # 一键启动后端+前端，打开 http://localhost:5173
```

### 改了数据划分后重训（当前配置）

```bash
# config.yaml 当前数据划分:
#   train: 2012-01-01 ~ 2022-12-31  (11年训练)
#   valid: 2023-01-01 ~ 2024-12-31  (2年验证，含2023震荡+2024牛市)
#   test:  2025-01-01 ~ 今天         (实盘评估)

# 1. 训练新模型（特征计算 + 训练，~15 分钟）
python main.py train --save-name model_v2

# 2. 缓存预测（后续所有回测复用，~15 分钟）
python main.py cache-predictions \
    --model-path output/models/model_v2_lgbm.pkl --tag v2

# 3. 用缓存跑各种回测（每次 ~2 分钟，随便调参数）
python main.py backtest \
    --pred-path output/predictions/predictions_v2.pkl --top-k 10
python main.py backtest \
    --pred-path output/predictions/predictions_v2.pkl --top-k 20 --rebalance-freq 10
python main.py signal-backtest \
    --pred-path output/predictions/predictions_v2.pkl --tag v2
python main.py account-backtest \
    --pred-path output/predictions/predictions_v2.pkl --tag v2
```

### 完整回测流程（通用）

```bash
# 1. 缓存预测（一次计算，多次复用）
python main.py cache-predictions --model-path output/models/model_lgbm.pkl --tag v1

# 2. 信号回测（评估信号质量：IC/RankIC/换手率）
python main.py signal-backtest --pred-path output/predictions/predictions_v1.pkl --tag v1

# 3. 账户回测（模拟真实交易：手续费/滑点/涨跌停/T+1）
python main.py account-backtest --pred-path output/predictions/predictions_v1.pkl --tag v1

# 4. 在前端查看回测结果
python main.py serve
```

### 个股分析（新功能）

```bash
# 启动后端 + 前端
# 启动后端 + 前端
python main.py dev

# 打开浏览器访问 http://localhost:5173/analysis
# 输入股票代码 -> 获取技术评分(0-100) + 量化模型预测 + LLM深度分析报告

# 也可通过 API 直接调用
curl -X POST http://localhost:8000/api/v1/analysis/analyze \
    -H "Content-Type: application/json" \
    -d '{"code": "600519"}'
```

## API 端点速查

> 完整交互式文档: http://localhost:8000/docs
>
> 除 `/health`、`/auth/login`、`/auth/register` 外，所有端点均需 `Authorization: Bearer <token>` 头。
> `/system/*` 和 `/admin/*` 端点仅 admin 可访问。

| 方法 | 路径 | 说明 | 权限 |
|------|------|------|------|
| GET | `/api/v1/health` | 系统健康检查 | 公开 |
| POST | `/api/v1/auth/register` | 注册（需邀请码） | 公开 |
| POST | `/api/v1/auth/login` | 登录 | 公开 |
| GET | `/api/v1/auth/me` | 当前用户信息 | 登录 |
| POST | `/api/v1/auth/change-password` | 修改密码 | 登录 |
| POST | `/api/v1/admin/invite` | 生成邀请码 | admin |
| GET | `/api/v1/admin/invites` | 邀请码列表 | admin |
| GET | `/api/v1/admin/users` | 用户列表 | admin |
| PUT | `/api/v1/admin/users/{id}` | 修改角色/禁用 | admin |
| GET | `/api/v1/admin/chat-sessions` | 全部对话记录 | admin |
| POST | `/api/v1/admin/chat-sessions/batch-delete` | 批量删除对话 | admin |
| GET | `/api/v1/admin/analysis-history` | 全部分析记录 | admin |
| POST | `/api/v1/admin/analysis-history/batch-delete` | 批量删除分析 | admin |
| GET | `/api/v1/admin/market-reviews` | 复盘报告列表 | admin |
| POST | `/api/v1/admin/market-reviews/batch-delete` | 批量删除复盘 | admin |
| GET | `/api/v1/admin/logs` | 查看系统日志 | admin |
| POST | `/api/v1/admin/logs/clear` | 清空日志 | admin |
| POST | `/api/v1/analysis/analyze` | 触发个股分析 | 登录 |
| POST | `/api/v1/analysis/analyze-stream` | SSE 流式分析 | 登录 |
| GET | `/api/v1/analysis/history` | 分析历史列表 | 登录 |
| GET | `/api/v1/analysis/{id}` | 单条分析详情 | 登录 |
| DELETE | `/api/v1/analysis/{id}` | 删除分析记录 | 登录 |
| POST | `/api/v1/analysis/batch-delete` | 批量删除分析 | 登录 |
| POST | `/api/v1/analysis/market-review` | 触发大盘复盘 | 登录 |
| GET | `/api/v1/analysis/market-review/status` | 复盘进度 | 登录 |
| GET | `/api/v1/analysis/market-review/stream` | SSE 复盘进度 | 登录 |
| GET | `/api/v1/chat/sessions` | 我的对话列表 | 登录 |
| POST | `/api/v1/chat/sessions` | 创建对话 | 登录 |
| PATCH | `/api/v1/chat/sessions/{id}` | 更新对话 | 登录 |
| DELETE | `/api/v1/chat/sessions/{id}` | 删除对话 | 登录 |
| GET | `/api/v1/chat/sessions/{id}/messages` | 对话消息历史 | 登录 |
| POST | `/api/v1/chat/sessions/{id}/send` | SSE 流式发送消息 | 登录 |
| GET | `/api/v1/trading/positions` | 当前持仓 | 登录 |
| POST | `/api/v1/trading/trade` | 手动买卖 | 登录 |
| GET | `/api/v1/trading/trades` | 交易记录 | 登录 |
| GET | `/api/v1/signals/latest` | 最新信号 | 登录 |
| GET | `/api/v1/signals/history` | 信号历史 | 登录 |
| GET | `/api/v1/backtest/runs` | 回测运行列表 | 登录 |
| GET | `/api/v1/backtest/runs/{id}` | 回测详情 | 登录 |
| GET | `/api/v1/backtest/dirs/{kind}` | 回测目录（文件系统） | 登录 |
| GET | `/api/v1/system/data-status` | 数据层状态 | admin |
| GET | `/api/v1/system/config` | 配置摘要 | admin |
| GET | `/api/v1/system/models` | 可用模型列表 | admin |
