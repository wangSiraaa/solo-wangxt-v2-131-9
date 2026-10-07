# 主要 HTTP 接口

所有时间使用 ISO 8601。原始块编码为 `float32le-interleaved`。

## 清单与上传

### `POST /manifests`

创建不可变分块清单，请求示例：

```json
{
  "name": "line-20261001",
  "nominal_sample_rate": 6000,
  "expected_chunks": [
    {
      "sequence": 0,
      "sha256": "...64 hex...",
      "byte_offset": 0,
      "byte_length": 3600,
      "sample_count": 300,
      "sample_rate": 6000,
      "channels": ["Va", "Vb", "Vc"],
      "start_time": "2026-10-01T00:00:00",
      "end_time": "2026-10-01T00:00:00.049833333",
      "encoding": "float32le-interleaved"
    }
  ]
}
```

创建失败返回 422，`detail.issues` 中列出重叠、空洞、序号、时间或通道集合错误。

### `PUT /manifests/{manifest_id}/chunks/{sequence}/raw`

`multipart/form-data` 字段名：`chunk_file`。可乱序、重传。重复摘要返回已有块；摘要/长度错误返回 422。

### `POST /manifests/{manifest_id}/finalize`

执行最终核对。成功：

```json
{"completed": true, "already_completed": false, "warnings": []}
```

失败返回 422，issues 不做静默合并。并发时只有一个请求能完成首次转换。

### `GET /manifests/{id}/preview?calibration_version_id=...`

返回分段、采样率、降采样波形和缺口/质量 issue，供 Vue/ECharts 展示。后端使用磁盘 memmap 和降采样，避免把数 GB 原始波形全部放入响应内存。

## 标定

### `POST /calibrations`

```json
{
  "channel_set_hash": "manifest.channel_set_hash",
  "change_note": "CT 二次接线复核后修订",
  "coefficients": {
    "Va": {"gain": 1, "offset": 0, "phase_shift_rad": 0, "saturation_low": -450, "saturation_high": 450}
  }
}
```

新 active 版本会让使用旧 active 版本发布的报告进入 `needs_review`。

## 任务与报告

### `POST /analysis-tasks`

```json
{
  "manifest_id": "...",
  "calibration_version_id": "...",
  "params": {"fundamental_hz": 50, "cycles_per_window": 6, "max_harmonic": 15},
  "idempotency_key": "lab-job-123"
}
```

创建时冻结清单、标定和参数。若使用 Celery，提交后自动发送 `run_analysis`；本地测试可调用 `/run`。

### 执行、重试、取消

- `POST /analysis-tasks/{id}/run`
- `POST /analysis-tasks/{id}/retry`
- `POST /analysis-tasks/{id}/cancel`
- `POST /maintenance/recover-stale-tasks`

`GET /analysis-tasks/{id}` 在任务字段之外返回运行轨迹，供页面判断重试是否安全：

| 字段 | 含义 |
| --- | --- |
| `events` | 追加式轨迹：`queued/running/retry_wait/succeeded/failed/cancelled` 状态、租约获取/接管/回收、每次重试、每个阶段完成，均带时间戳 |
| `stage_results` | 按阶段保存的结果（`fixed_snapshot/raw_inventory/raw_bytes/segmentation/spectrum`），每项含 `at` |
| `lease_owner` / `lease_until` / `heartbeat_at` / `lease_generation` | 当前租约持有者、到期时间、最近心跳与 fencing 代际 |
| `lease_expired` | 租约是否已过期（过期租约可被接管） |
| `attempts` / `retry_request_count` | Worker 尝试次数 / 当前尝试已消费的手动重试次数 |
| `error_code` / `error_message` | 最近一次失败原因 |
| `retry_allowed` / `retry_reason` | 服务端给出的受控重试判定与理由 |

受控重试的服务端规则：

- `retry_wait/failed` 且当前尝试仍有未消费的重试名额（`retry_request_count < attempts`）才允许；
- `running` 仅当租约已过期时允许（接管死 Worker），租约有效返回 409；
- `queued/succeeded/cancelled`、已存在 `published/needs_review` 报告时一律 409；
- 转换是带条件的单条 UPDATE（同时自增 `retry_request_count`、自增 `lease_generation`），因此并发/重复点击只有一次成功；
- 重试沿用创建时冻结的同一份 `manifest_snapshot`、标定版本与参数，不创建新任务。

Worker 侧防重边界：

- 每次获取/接管租约自增 `lease_generation`；心跳续约、终态写入与失败落库都是 `WHERE lease_generation = ? AND lease_until > now` 的条件 UPDATE；
- 旧 Worker 在租约被接管后回写影响 0 行，直接停止，不会覆盖新 Owner 状态或重复发布；
- `reports.task_id` 唯一约束是最终防重边界，每任务至多一行报告（质量 error 时为 `diagnostic_failed`，重试成功后该行就地转为 `published`）。

### `GET /reports/{id}`

报告状态：

- `published`：正常发布；
- `needs_review`：标定后来更新，需要复核；
- `diagnostic_failed`：分析有 error 阶段，保留诊断但不是完成报告。
