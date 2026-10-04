# 指标采集与聚合核心

多级时间窗口聚合，支持来源更正（计数器重置 / 样本撤回 / 迟到样本），
并保证同一贡献在重启或补算时不会重复累加。

## 核心语义

- **多级窗口**：`DEFAULT_LEVELS = (60, 300, 3600)` 秒，对齐取整。短窗口
  `seal_due(now)` 封存后自动汇总（rollup）到下一级，同一调用内逐级级联。
- **幂等贡献**：样本按 `sample_id` 持久去重；rollup 贡献键为
  `rollup:{child_window}:v{version}`，重启重放或补算均为 `INSERT OR IGNORE`，
  不会重复累加。父窗口聚合时同一子窗口只取最高版本。
- **计数器重置**：新值 < 上次值时视为新周期，delta 取新值本身，并记录
  `counter_reset` 事件（可查）。
- **撤回边界**：`retract(source, metric, ts_start, ts_end)` 只撤回该来源在
  闭区间内的样本。未封存窗口就地更正；已封存窗口生成新版本并向上级联，
  旧版本统计保留可查。
- **封存窗口修订**：封存窗口绝不覆盖。迟到样本（`apply_late`）或撤回触发
  `version+1`，`revisions()` 返回每次修订的原因，可追溯。
- **资源上限**：`max_contributions_per_window` 超限时样本被拒绝并写入
  拒绝日志，`rejections()` 可查（含 sample_id、原因、时间）。

## 快照报告

在既有入口（`window()` / `revisions()` 等）之外，`MetricsCore` 直接提供
多粒度快照报告，无需新部署组件：

- **一次报告、多个粒度**：`generate_report(metric, start, end)` 在单个
  只读事务中捕获 `levels` 全部层级的窗口，读取过程中不会混入随后到达
  的更正；报告固定观测范围 `[start, end)` 与 `correction_version`。
- **粗细互核**：报告内置 rollup 对账，`consistent` / `discrepancies`
  标明细粒度之和是否与汇总窗口一致。
- **空窗 / 未覆盖 / 零值**：无数据的窗口为 `state="empty"`（数值为
  `null`），仍在累计的窗口为 `state="open"`（`complete=False`），与
  真实零值（如计数器无增长，`sum=0.0`）明确区分；范围边缘截断的窗口
  标记 `coverage="partial"`。
  部分覆盖的窗口只统计落在报告范围内的观测（按样本时间从贡献记录
  重算，而非整窗汇总），并附 `covered_range` 与 `note` 说明实际统计
  口径，范围外的观测不会混入任何粒度。
- **迟到数据与差异**：新迟到数据到达后旧报告原样保留；
  `diff_reports(old_id, new_id)` 列出范围内发生变化的汇总，以及范围未
  覆盖、因而不应影响报告的修订（`revisions_out_of_range`）。
  修订只有在其窗口完全落在报告范围内时才计为范围内；部分覆盖窗口的
  版本号可能因范围外更正而递增，差异比较会忽略这种与报告数值无关的
  变化，只指出真正受影响的结果。
- **采用与引用**：`adopt_report(id)` 正式采用新版，旧版仍可
  `get_report(id)` 引用；报告持久化于 `reports` 表，`purge_before(ts)`
  清理底层窗口与样本时不会删除报告，引用不会失去解释依据。
- **途中重开幂等**：报告 id 由内容哈希决定，处理途中重启后重新生成
  得到同一 id（`INSERT OR IGNORE`），不会重复或漂移。

## 验证

`tests/` 用人工序列（计数器重置、分布样本迟到、部分来源退出、重启重放、
限额拒绝）与独立的逐样本朴素计算对照，检查各级窗口的次数、总和与桶计数
完全一致：

```bash
python3 -m pytest tests/ -q
```
