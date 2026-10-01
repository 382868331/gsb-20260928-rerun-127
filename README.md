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

## 验证

`tests/` 用人工序列（计数器重置、分布样本迟到、部分来源退出、重启重放、
限额拒绝）与独立的逐样本朴素计算对照，检查各级窗口的次数、总和与桶计数
完全一致：

```bash
python3 -m pytest tests/ -q
```
