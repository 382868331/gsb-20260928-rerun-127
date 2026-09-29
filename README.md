# 指标采集与聚合核心

纯 Go、无外部服务依赖的指标采集与窗口聚合库。当前只提供核心聚合结果，后续告警系统可直接消费已关闭窗口；本次不包含统计看板。

## 能力

- 指标定义：注册指标名称、类型和分布桶边界。
- 来源管理：每个样本必须携带来源，来源数量受 `MaxSources` 限制。
- 时间窗口：固定时间窗口，可用 `CloseBefore` 关闭窗口。
- 累计计数器：按 `(来源, 指标)` 保存游标，累计正向增量；值变小视为计数器重置，从新值重新累计，绝不产生负增长。每个来源的首个读数只作为基线。
- 分布聚合：输出 count、sum、min、max、mean、样本方差和固定桶计数；均值/方差使用 Welford 算法。
- 迟到样本：窗口关闭后到达的样本被拒绝累计，并计入 `Stats.DroppedLate`。
- 重复采样：以 `(来源, 指标, 时间戳, 值)` 去重，重复样本计入 `Stats.DroppedDuplicate`。
- 上限可见：来源或去重内存超限时返回 `ErrSourceLimit` / `ErrMemoryLimit`，同时在 `Stats` 中累计拒绝次数。
- 检查点：将定义、来源、窗口、计数器游标、去重集合和统计值保存为 JSON；恢复后继续运行的结果与一次性完整运行一致。

## 快速使用

```go
a, err := metrics.New(metrics.Config{
    WindowSize:       30 * time.Minute,
    MaxSources:       100,
    MaxMemoryEntries: 100000,
}, metrics.Definition{
    Name: "requests",
    Type: metrics.MetricCounter,
}, metrics.Definition{
    Name:    "latency_ms",
    Type:    metrics.MetricDistribution,
    Buckets: []float64{2, 4, 10},
})
if err != nil {
    panic(err)
}
err = a.Add(metrics.Sample{
    Source:    "host-a",
    Metric:    "requests",
    Timestamp: time.Now(),
    Value:     42,
})
a.CloseBefore(time.Now())
for _, window := range a.Results() {
    if window.Closed {
        _ = window.Counters
        _ = window.Distributions
    }
}
var file *os.File
if err := a.SaveCheckpoint(file); err != nil {
    panic(err)
}
restored, err := metrics.LoadCheckpoint(file)
```

## 验证

运行 `go test ./...`。测试使用人工构造序列直接复算窗口结果，并覆盖计数器重置、重复样本、迟到样本、来源/内存上限，以及“保存检查点后继续”与完整运行的结果一致性。
