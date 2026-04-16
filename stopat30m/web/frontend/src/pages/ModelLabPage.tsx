import { useEffect, useState } from 'react';
import { getModelConfig, updateModelConfig, generateTrainCommand } from '../api/system';
import { useAuth } from '../contexts/AuthContext';

/* ------------------------------------------------------------------ */
/* Factor toggle keys in config                                        */
/* ------------------------------------------------------------------ */

const FACTOR_GROUPS = [
  'momentum', 'volatility', 'volume', 'technical', 'microstructure', 'trend',
  'cross_sectional', 'multiscale', 'distribution', 'reversal', 'tail_risk',
  'price_pattern', 'volume_price', 'mean_reversion', 'relative_strength',
  'weighted_momentum', 'higher_order', 'atr', 'efficiency', 'open_price',
  'serial_dependence', 'conditional', 'volume_shape', 'pivot', 'detrended',
  'sign_change', 'interaction', 'log_features', 'dispersion',
] as const;

const LABEL_PRESETS: { label: string; expr: string; hint: string }[] = [
  { label: '1日收益', expr: 'Ref($close,-1)/$close - 1', hint: '短线，噪声大' },
  { label: '3日收益', expr: 'Ref($close,-3)/$close - 1', hint: '短线偏中' },
  { label: '5日收益（默认）', expr: 'Ref($close,-5)/$close - 1', hint: '平衡信噪比' },
  { label: '10日收益', expr: 'Ref($close,-10)/$close - 1', hint: '中线，信号更稳' },
  { label: '20日收益', expr: 'Ref($close,-20)/$close - 1', hint: '月线，换手低' },
];

const MODEL_TYPE_HINTS: Record<string, string> = {
  lgbm: '训练快、效果稳、可解释。首选。',
  xgboost: '与 LightGBM 类似，可交叉验证。',
  mlp: '神经网络，能学非线性，需更多数据。',
  lstm: '序列模型，捕捉时序模式，实验性。',
  transformer: '注意力机制，实验性，训练慢。',
};

const UNIVERSE_HINTS: Record<string, string> = {
  csi300: '大盘股300只，数据质量高，训练快。新手推荐。',
  csi500: '中盘股500只，覆盖更广，波动更大。',
  csi1000: '小盘股1000只，机会多但噪声大。',
  all: '全A股4000+只，数据量大，训练慢，噪声高。',
};

const PARAM_HINTS: Record<string, string> = {
  num_boost_round: '最大训练轮数。有 early_stop 兜底，可设大。',
  early_stopping_rounds: '验证集无改善的容忍轮数，越大训练越久。',
  num_leaves: '每棵树叶子数。↑复杂度高易过拟合，↓更保守。核心参数。',
  learning_rate: '学习步长。↓学得细需更多轮，↑学得粗但快。',
  max_depth: '树最大深度。↑学更复杂模式，↓限制复杂度防过拟合。',
  subsample: '每轮训练用多少比例数据。<1 增加随机性防过拟合。',
  bagging_freq: '每隔几轮做一次 subsample。1=每轮都做。',
  colsample_bytree: '每棵树用多少比例特征。↓更随机防过拟合。重要。',
  reg_alpha: 'L1 正则化。↑稀疏化特征选择，防过拟合。',
  reg_lambda: 'L2 正则化。↑平滑权重，防过拟合。核心参数。',
  min_child_samples: '叶子最少样本数。↑更保守防过拟合。',
  path_smooth: '叶子预测平滑度。↑使预测更保守。',
  loss: '损失函数。mse=均方误差（回归标准）。',
  objective: '优化目标。reg:squarederror=回归标准。',
  n_estimators: '树的数量（同 num_boost_round）。',
  min_child_weight: '叶子最小权重和（同 min_child_samples）。',
  hidden_size: '隐藏层神经元数。↑容量大，↓训练快。',
  num_layers: '隐藏层数。2-4层通常够用。',
  dropout: '随机丢弃比例。↑防过拟合，↓保留更多信息。',
  epochs: '训练轮数。',
  batch_size: '每批样本数。↑训练稳定但占内存，↓更随机。',
};

const FACTOR_HINTS: Record<string, string> = {
  momentum: '动量：过去N天涨幅。核心因子。',
  volatility: '波动率：风险度量。',
  volume: '成交量特征：量能信号。',
  technical: '经典技术指标：MACD、RSI等。',
  microstructure: '微观结构：开盘/收盘偏离等短期信号。',
  trend: '趋势：均线方向、突破。',
  cross_sectional: '截面排名：相对强弱。',
  multiscale: '多尺度：不同时间窗口聚合。',
  distribution: '分布特征：偏度、峰度。',
  reversal: '反转因子：均值回归信号。',
  tail_risk: '尾部风险：极端行情特征。',
  price_pattern: '价格形态：K线模式。',
  volume_price: '量价配合：放量/缩量特征。',
  mean_reversion: '均值回归：偏离后回归。',
  relative_strength: '相对强弱：板块内排名。',
  weighted_momentum: '加权动量：成交量加权。',
  higher_order: '高阶衍生：二阶导数等。可关闭防过拟合。',
  atr: 'ATR：平均真实波幅。',
  efficiency: '价格效率：方向性运动。',
  open_price: '开盘价特征：隔夜信息。',
  serial_dependence: '序列依赖：自相关。',
  conditional: '条件因子：条件触发。',
  volume_shape: '量型特征：量能形态。',
  pivot: '枢轴点：支撑阻力位。',
  detrended: '去趋势：去除长期趋势后的波动。',
  sign_change: '符号变化：涨跌转换频率。',
  interaction: '因子交互：组合衍生。特征多，可关闭防过拟合。',
  log_features: '对数特征：对数变换。可关闭减少特征数。',
  dispersion: '离散度：价格分散程度。',
};

/* ------------------------------------------------------------------ */
/* Component                                                           */
/* ------------------------------------------------------------------ */

export default function ModelLabPage() {
  const { isAdmin } = useAuth();

  const [cfg, setCfg] = useState<Record<string, any> | null>(null);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState<string | null>(null);
  const [saveMsg, setSaveMsg] = useState('');

  const [cliCmd, setCliCmd] = useState('');
  const [copied, setCopied] = useState(false);

  const [guideOpen, setGuideOpen] = useState(false);

  /* ---------- load config ---------- */
  useEffect(() => {
    if (!isAdmin) { setLoading(false); return; }
    getModelConfig()
      .then(setCfg)
      .catch((e: unknown) => setErr(e instanceof Error ? e.message : '加载配置失败'))
      .finally(() => setLoading(false));
  }, [isAdmin]);

  /* ---------- helpers ---------- */
  const modelType = cfg?.model?.type ?? 'lgbm';
  const modelParams = cfg?.model?.params?.[modelType] ?? {};
  const factors = cfg?.factors ?? {};
  const dataSection = cfg?.data ?? {};

  function setModelType(t: string) {
    setCfg((prev) => prev ? { ...prev, model: { ...prev.model, type: t } } : prev);
  }

  function setModelParam(key: string, value: number) {
    setCfg((prev) => {
      if (!prev) return prev;
      const mt = prev.model?.type ?? 'lgbm';
      return {
        ...prev,
        model: {
          ...prev.model,
          params: { ...prev.model?.params, [mt]: { ...prev.model?.params?.[mt], [key]: value } },
        },
      };
    });
  }

  function toggleFactor(name: string) {
    const key = `enable_${name}`;
    setCfg((prev) => prev ? { ...prev, factors: { ...prev.factors, [key]: !prev.factors?.[key] } } : prev);
  }

  function setUniverse(u: string) {
    setCfg((prev) => prev ? { ...prev, data: { ...prev.data, universe: u } } : prev);
  }

  function setLabelExpr(expr: string) {
    setCfg((prev) => prev ? { ...prev, factors: { ...prev.factors, label: expr } } : prev);
  }

  function enableAllFactors() {
    setCfg((prev) => {
      if (!prev) return prev;
      const next = { ...prev.factors };
      for (const name of FACTOR_GROUPS) next[`enable_${name}`] = true;
      return { ...prev, factors: next };
    });
  }

  function disableAllFactors() {
    setCfg((prev) => {
      if (!prev) return prev;
      const next = { ...prev.factors };
      for (const name of FACTOR_GROUPS) next[`enable_${name}`] = false;
      return { ...prev, factors: next };
    });
  }

  const enabledCount = FACTOR_GROUPS.filter((n) => factors[`enable_${n}`] !== false).length;

  /* ---------- save config ---------- */
  async function handleSave() {
    if (!cfg) return;
    setSaveMsg('');
    try {
      const res = await updateModelConfig(cfg);
      setSaveMsg(`已保存 (${res.updated_sections})`);
      setTimeout(() => setSaveMsg(''), 3000);
    } catch (e: unknown) {
      setSaveMsg(e instanceof Error ? e.message : '保存失败');
    }
  }

  /* ---------- generate CLI command ---------- */
  async function handleGenerate() {
    try {
      const res = await generateTrainCommand({
        model_type: modelType,
        universe: dataSection.universe ?? '',
      });
      setCliCmd(res.command);
      setCopied(false);
    } catch {
      setCliCmd('python main.py train');
    }
  }

  function handleCopy() {
    navigator.clipboard.writeText(cliCmd).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  }

  /* ---------------------------------------------------------------- */
  /* Render                                                            */
  /* ---------------------------------------------------------------- */

  if (!isAdmin) {
    return (
      <div>
        <h1 className="mb-2 text-2xl font-bold text-gray-900">模型实验室</h1>
        <p className="text-sm text-gray-500">仅管理员可访问。</p>
      </div>
    );
  }

  return (
    <div>
      <h1 className="mb-2 text-2xl font-bold text-gray-900">模型实验室</h1>
      <p className="mb-6 text-sm text-gray-500">
        调整参数 &rarr; 保存配置 &rarr; 生成 CLI 命令 &rarr; 终端训练 &rarr; 回测中心验证
      </p>

      {err && <div className="mb-4 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800">{err}</div>}

      {/* ===== Config Editor (single card) ===== */}
      <div className="rounded-lg border border-gray-200 bg-white p-5 shadow-sm">
        <h2 className="mb-4 text-lg font-semibold text-gray-800">参数编辑器</h2>

        {loading ? (
          <p className="text-sm text-gray-400">加载中...</p>
        ) : (
          <div className="space-y-6">
            {/* Two-column layout for params + factors */}
            <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
              {/* Left: Model & data params */}
              <div className="space-y-5">
                {/* Model type */}
                <div>
                  <label className="mb-1 block text-xs font-medium text-gray-500">模型类型</label>
                  <select
                    value={modelType}
                    onChange={(e) => setModelType(e.target.value)}
                    className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm"
                  >
                    <option value="lgbm">LightGBM</option>
                    <option value="xgboost">XGBoost</option>
                    <option value="mlp">MLP (DNN)</option>
                    <option value="lstm">LSTM</option>
                    <option value="transformer">Transformer</option>
                  </select>
                  <p className="mt-1 text-[11px] text-gray-400">{MODEL_TYPE_HINTS[modelType] ?? ''}</p>
                </div>

                {/* Model params */}
                <div>
                  <label className="mb-1 block text-xs font-medium text-gray-500">
                    {modelType.toUpperCase()} 超参数
                  </label>
                  <div className="space-y-1">
                    {Object.entries(modelParams).map(([k, v]) => (
                      <div key={k} className="rounded-md border border-gray-100 px-2 py-1.5">
                        <div className="flex items-center gap-2">
                          <span className="w-40 truncate text-xs font-medium text-gray-700" title={k}>{k}</span>
                          <input
                            type="number"
                            step="any"
                            value={v as number}
                            onChange={(e) => setModelParam(k, parseFloat(e.target.value) || 0)}
                            className="flex-1 rounded border border-gray-300 px-2 py-1 text-xs"
                          />
                        </div>
                        {PARAM_HINTS[k] && (
                          <p className="mt-0.5 text-[10px] leading-tight text-gray-400">{PARAM_HINTS[k]}</p>
                        )}
                      </div>
                    ))}
                  </div>
                </div>

                {/* Universe */}
                <div>
                  <label className="mb-1 block text-xs font-medium text-gray-500">股票池</label>
                  <select
                    value={dataSection.universe ?? 'all'}
                    onChange={(e) => setUniverse(e.target.value)}
                    className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm"
                  >
                    <option value="csi300">沪深300</option>
                    <option value="csi500">中证500</option>
                    <option value="csi1000">中证1000</option>
                    <option value="all">全A股</option>
                  </select>
                  <p className="mt-1 text-[11px] text-gray-400">{UNIVERSE_HINTS[dataSection.universe ?? 'all'] ?? ''}</p>
                </div>

                {/* Label */}
                <div>
                  <label className="mb-1 block text-xs font-medium text-gray-500">预测标签（收益窗口）</label>
                  <select
                    value={factors.label ?? 'Ref($close,-5)/$close - 1'}
                    onChange={(e) => setLabelExpr(e.target.value)}
                    className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm"
                  >
                    {LABEL_PRESETS.map((p) => (
                      <option key={p.expr} value={p.expr}>{p.label}</option>
                    ))}
                  </select>
                  <p className="mt-1 text-[11px] text-gray-400">
                    {LABEL_PRESETS.find((p) => p.expr === (factors.label ?? 'Ref($close,-5)/$close - 1'))?.hint ?? '模型预测的目标：N天后的收益率'}
                  </p>
                </div>
              </div>

              {/* Right: Factor toggles */}
              <div>
                <div className="mb-2 flex items-center justify-between">
                  <div>
                    <label className="block text-xs font-medium text-gray-500">因子组合</label>
                    <p className="mt-0.5 text-[11px] text-gray-400">
                      已启用 {enabledCount}/{FACTOR_GROUPS.length} 个。越多越丰富但也增加噪声。
                    </p>
                  </div>
                  <div className="flex gap-1.5">
                    <button onClick={enableAllFactors} className="rounded border border-gray-200 px-2 py-1 text-[10px] text-gray-500 hover:bg-gray-50">全选</button>
                    <button onClick={disableAllFactors} className="rounded border border-gray-200 px-2 py-1 text-[10px] text-gray-500 hover:bg-gray-50">全不选</button>
                  </div>
                </div>
                <div className="grid grid-cols-1 gap-0.5">
                  {FACTOR_GROUPS.map((name) => {
                    const key = `enable_${name}`;
                    const enabled = factors[key] !== false;
                    return (
                      <label
                        key={name}
                        className={`flex items-start gap-1.5 rounded px-2 py-1.5 text-xs transition-colors ${
                          enabled ? 'bg-blue-50/50 text-gray-800' : 'text-gray-400'
                        }`}
                      >
                        <input
                          type="checkbox"
                          checked={enabled}
                          onChange={() => toggleFactor(name)}
                          className="mt-0.5 h-3.5 w-3.5 rounded border-gray-300"
                        />
                        <span>
                          <span className="font-medium">{name}</span>
                          {FACTOR_HINTS[name] && (
                            <span className="ml-1 text-[10px] text-gray-400">{FACTOR_HINTS[name]}</span>
                          )}
                        </span>
                      </label>
                    );
                  })}
                </div>
              </div>
            </div>

            {/* Action buttons (full width, below both columns) */}
            <div className="border-t border-gray-100 pt-4">
              <div className="flex flex-wrap items-center gap-2">
                <button
                  onClick={handleSave}
                  className="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700"
                >
                  保存配置
                </button>
                <button
                  onClick={handleGenerate}
                  className="rounded-lg border border-gray-300 bg-white px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-50"
                >
                  生成 CLI 命令
                </button>
                {saveMsg && <span className="text-xs text-green-600">{saveMsg}</span>}
              </div>

              {/* CLI command display */}
              {cliCmd && (
                <div className="mt-3 rounded-md border border-gray-200 bg-gray-50 p-3">
                  <div className="flex items-center justify-between">
                    <span className="text-xs font-medium text-gray-500">终端命令</span>
                    <button
                      onClick={handleCopy}
                      className="text-xs text-blue-600 hover:text-blue-800"
                    >
                      {copied ? '已复制' : '复制'}
                    </button>
                  </div>
                  <code className="mt-1 block break-all text-xs text-gray-800">{cliCmd}</code>
                </div>
              )}
            </div>
          </div>
        )}
      </div>

      {/* ===== Bottom: Tuning Guide (full width) ===== */}
      <div className="mt-6 rounded-lg border border-gray-200 bg-white shadow-sm">
        <button
          onClick={() => setGuideOpen(!guideOpen)}
          className="flex w-full items-center justify-between px-5 py-4 text-left"
        >
          <h2 className="text-lg font-semibold text-gray-800">调参指引</h2>
          <span className="text-gray-400">{guideOpen ? '▲' : '▼'}</span>
        </button>

        {guideOpen && (
          <div className="space-y-5 border-t border-gray-100 px-5 py-4 text-sm text-gray-700">
            {/* Concepts */}
            <div>
              <h3 className="mb-2 text-sm font-semibold text-gray-800">核心概念</h3>
              <div className="grid grid-cols-1 gap-2 text-xs sm:grid-cols-2 xl:grid-cols-4">
                <div className="rounded-md border border-gray-100 bg-gray-50 p-2.5">
                  <span className="font-medium text-gray-700">IC (Information Coefficient)</span>
                  <p className="mt-0.5 text-gray-500">预测值与实际收益的相关性。&gt;0.03 可用，&gt;0.05 优秀，&gt;0.08 顶级。越高说明模型预测越准。</p>
                </div>
                <div className="rounded-md border border-gray-100 bg-gray-50 p-2.5">
                  <span className="font-medium text-gray-700">ICIR</span>
                  <p className="mt-0.5 text-gray-500">IC 的均值/标准差。衡量 IC 的稳定性。&gt;1 为好，说明不只是偶尔准一次。</p>
                </div>
                <div className="rounded-md border border-gray-100 bg-gray-50 p-2.5">
                  <span className="font-medium text-gray-700">Sharpe Ratio</span>
                  <p className="mt-0.5 text-gray-500">风险调整后收益。&gt;1 及格，&gt;2 优秀。越高越值得承担风险。</p>
                </div>
                <div className="rounded-md border border-gray-100 bg-gray-50 p-2.5">
                  <span className="font-medium text-gray-700">过拟合</span>
                  <p className="mt-0.5 text-gray-500">训练指标好但回测差。模型学到的是噪声而非规律。是最常见的问题。</p>
                </div>
              </div>
            </div>

            {/* Diagnosis table */}
            <div>
              <h3 className="mb-2 text-sm font-semibold text-gray-800">问题诊断</h3>
              <div className="overflow-x-auto">
                <table className="w-full text-xs">
                  <thead>
                    <tr className="border-b border-gray-200 text-gray-500">
                      <th className="py-2 text-left font-medium">现象</th>
                      <th className="py-2 text-left font-medium">可能原因</th>
                      <th className="py-2 text-left font-medium">调整建议</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100">
                    <tr>
                      <td className="py-2">IC &lt; 0.03</td>
                      <td className="py-2">因子表达能力不足 / 数据量少</td>
                      <td className="py-2">开启更多因子组、扩大 universe、延长训练时间窗口</td>
                    </tr>
                    <tr>
                      <td className="py-2">IC 高但 ICIR 低</td>
                      <td className="py-2">IC 波动大，模型不稳定</td>
                      <td className="py-2">增大 min_child_samples、降低 num_leaves、加正则化</td>
                    </tr>
                    <tr>
                      <td className="py-2">Sharpe &lt; 1.0</td>
                      <td className="py-2">信号质量不够 / 换手成本高</td>
                      <td className="py-2">尝试不同模型（XGBoost）、调整 learning_rate、降低 top_k</td>
                    </tr>
                    <tr>
                      <td className="py-2">最大回撤 &gt; 15%</td>
                      <td className="py-2">集中持仓 / 极端行情</td>
                      <td className="py-2">降低 top_k、增大 rebalance_freq、启用 risk manager</td>
                    </tr>
                    <tr>
                      <td className="py-2">训练指标好但回测差</td>
                      <td className="py-2">过拟合</td>
                      <td className="py-2">增大 reg_alpha/reg_lambda、减少 num_leaves、缩短标签窗口</td>
                    </tr>
                    <tr>
                      <td className="py-2">胜率 &lt; 50%</td>
                      <td className="py-2">信号准确度低</td>
                      <td className="py-2">换模型、加因子、确认数据完整性</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>

            {/* Tuning roadmap + Key params: side by side on wide screens */}
            <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
              <div className="rounded-md bg-blue-50 p-3 text-xs text-blue-800">
                <strong>推荐调参路线（从上到下依次尝试）：</strong>
                <ol className="mt-1 list-inside list-decimal space-y-1">
                  <li><strong>Baseline</strong> — 用默认参数训练一次，记录 IC / Sharpe / 回撤，作为对比基准</li>
                  <li><strong>股票池</strong> — csi300（快速迭代）→ csi500（更多信号）→ all（最终验证）</li>
                  <li><strong>标签窗口</strong> — 5天 → 3天 → 10天，观察 IC 变化选最优</li>
                  <li><strong>超参微调</strong> — learning_rate（0.005~0.05）、num_leaves（64~256）、reg_lambda（0~10）</li>
                  <li><strong>模型类型</strong> — lgbm → xgboost → mlp，看哪个 IC 最高且稳定</li>
                  <li><strong>因子精简</strong> — 关闭 interaction / log_features / higher_order 等噪声因子，看 IC 是否反升</li>
                  <li><strong>最终验证</strong> — 确定最优参数后，到回测中心跑完整账户回测验证实际可用性</li>
                </ol>
              </div>

              <div className="rounded-md bg-amber-50 p-3 text-xs text-amber-900">
                <strong>LightGBM 关键参数速查：</strong>
                <div className="mt-1 grid grid-cols-1 gap-1 sm:grid-cols-2">
                  <span><code className="rounded bg-amber-100 px-1">num_leaves</code> 64~256，核心复杂度参数</span>
                  <span><code className="rounded bg-amber-100 px-1">learning_rate</code> 0.005~0.05，越小越精细</span>
                  <span><code className="rounded bg-amber-100 px-1">reg_lambda</code> 0~10，防过拟合首选</span>
                  <span><code className="rounded bg-amber-100 px-1">colsample_bytree</code> 0.5~0.9，随机选特征</span>
                  <span><code className="rounded bg-amber-100 px-1">min_child_samples</code> 20~200，叶子最少样本</span>
                  <span><code className="rounded bg-amber-100 px-1">subsample</code> 0.5~0.9，随机选数据行</span>
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
