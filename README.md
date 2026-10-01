# 指标采集与聚合核心

纯 Go 的嵌入式指标注册、采集与时间窗口聚合包，不依赖外部监控服务。后续告警系统可直接读取窗口快照。

## 能力

- 指标定义：支持累计计数器 `counter` 和带显式桶边界的 `distribution`。
- 来源管理：注册来源，限制来源数量，并暴露当前来源数与超限错误。
- 多级时间窗口：`WindowLevels` 配置升序且逐级可整除的窗口；短窗口封存后只向父窗口合并一次，恢复或补算不会重复汇总。
- 封存修订：迟到数据不能直接写入已封存窗口；`AmendWindow` 会复制窗口、递增版本并联动重算父窗口，旧版本继续保留。
- 来源生命周期：`ResetSource` 表示明确的计数器重置边界；`WithdrawSource` 表示来源退出，退出时刻后的样本会被拒绝，历史贡献保留。
- 累计计数：同一来源同一指标的首个样本只建立基线；后续按增量累计，计数器回绕或重置时累计新值，负值不作为负增长。
- 分布聚合：输出次数、总和、最小值、最大值和桶计数。
- 幂等采样：样本 `ID` 全局去重，重复提交不会重复累计。
- 资源上限：可配置最大指标数、最大来源数、估算内存和拒绝记录数，`Stats`/`Rejections()` 中可见超限次数、最近原因与被拒数据。
- 检查点：通过 `Save`/`Load` 保存定义、来源、窗口及修订、事件、重置、去重记录、拒绝记录和统计值；恢复后继续运行与完整运行一致。

## 快速使用

    registry := metriccore.NewRegistry(metriccore.Config{
        WindowLevels:   []time.Duration{time.Minute, 5 * time.Minute},
        MaxSources:     1000,
        MaxMetrics:     100,
        MaxMemoryBytes: 64 << 20,
    })

    _ = registry.AddMetric(metriccore.Definition{
        Name:         "http_latency",
        Type:         metriccore.Distribution,
        BucketBounds: []float64{5, 10, 25, 50, 100},
    })
    _ = registry.RegisterSource(metriccore.Source{ID: "api-1"})

    err := registry.Ingest(metriccore.Sample{
        ID:        "sample-0001",
        Metric:    "http_latency",
        Source:    "api-1",
        Timestamp: time.Now().UTC(),
        Value:     12.5,
    })

    registry.CloseWindows(time.Now())
    windows := registry.SnapshotWindows()
    stats := registry.Stats()
    rejected := registry.Rejections()

    // 已封存窗口收到迟到样本时使用修订，而不是直接覆盖。
    revised, err := registry.AmendWindow(0, sealedWindowStart, lateSamples, "backfill")
    previous, _ := registry.SnapshotWindowRevision(0, sealedWindowStart, revised.Revision-1)

窗口结果中的 key 由 `metriccore.ResultKey(sourceID, metricName)` 生成，也可以用 `metriccore.ParseResultKey` 还原。
