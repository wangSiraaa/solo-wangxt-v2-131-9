<template>
  <div>
    <header class="header">
      <div>
        <h1>电能质量离线复核平台</h1>
        <small>不可变原始块 · 固定清单/标定/参数 · 谐波 / 对称分量 / 缺口诊断</small>
      </div>
      <span>{{ health.status }} / {{ health.object_store }}</span>
    </header>

    <div class="layout">
      <aside class="panel">
        <h2>录波清单</h2>
        <div
          v-for="item in manifests"
          :key="item.id"
          class="manifest-item"
          :class="{ active: selectedId === item.id }"
          @click="selectManifest(item.id)"
        >
          <h3>{{ item.name }}</h3>
          <div class="meta">
            <span class="badge" :class="item.status">{{ item.status }}</span>
            {{ item.expected_chunks.length }} 块 · {{ item.channel_set.join(', ') }}
          </div>
        </div>
      </aside>

      <main class="panel" v-if="manifest">
        <div style="display:flex;justify-content:space-between;align-items:center">
          <div>
            <h2 style="margin:0 0 4px">{{ manifest.name }}</h2>
            <div class="meta">
              digest {{ short(manifest.manifest_digest) }} · {{ manifest.start_time }} → {{ manifest.end_time }}
            </div>
          </div>
          <div>
            <button @click="finalize" :disabled="manifest.status !== 'open'">核对并完成</button>
            <button class="secondary" @click="refreshAll">刷新</button>
          </div>
        </div>

        <h3>块与缺口</h3>
        <GapChart :chunks="chunks" :manifest="manifest" :issues="issues" />

        <div v-for="issue in issues" :key="issue.id" class="issue" :class="issue.severity">
          <strong>[{{ issue.severity }}] {{ issue.code }}</strong> — {{ issue.message }}
        </div>

        <h3>波形（按固定采样率分段，不做插值拼接）</h3>
        <WaveformChart :preview="preview" />

        <h3>分析任务</h3>
        <div style="margin-bottom:10px">
          <label>固定标定版本：
            <select v-model="selectedCalibrationId" style="max-width:360px">
              <option v-for="c in calibrations" :key="c.id" :value="c.id">
                {{ short(c.id) }} · {{ c.status }} · {{ c.change_note || '初始版本' }}
              </option>
            </select>
          </label>
          <button @click="createTask" :disabled="!selectedCalibrationId">创建/入队</button>
        </div>
        <table>
          <thead><tr><th>任务</th><th>状态</th><th>标定</th><th>尝试</th><th>阶段</th><th>操作</th></tr></thead>
          <tbody>
            <template v-for="task in tasks" :key="task.id">
              <tr>
                <td><a href="#" class="tasklink" @click.prevent="toggleTask(task.id)">{{ short(task.id) }}</a></td>
                <td><span class="badge" :class="task.status">{{ task.status }}</span>
                  <div v-if="task.cancellation_requested" class="meta">取消请求中</div></td>
                <td>{{ short(task.calibration_version_id) }}</td>
                <td>{{ task.attempts }}</td>
                <td class="meta">{{ Object.keys(task.stage_results || {}).join(' → ') }}</td>
                <td>
                  <button @click="run(task.id)" :disabled="!canRun(task)">同步执行</button>
                  <button
                    class="secondary"
                    :disabled="!task.retry_allowed || busyTask === task.id"
                    :title="task.retry_allowed ? '沿用固定快照重新入队' : task.retry_reason"
                    @click="retry(task.id)"
                  >受控重试</button>
                  <button class="danger" @click="cancel(task.id)" :disabled="terminalStatuses.has(task.status)">取消</button>
                </td>
              </tr>
              <tr v-if="expandedTask === task.id" class="trace-row">
                <td colspan="6">
                  <TaskTrace :task="task" />
                </td>
              </tr>
            </template>
          </tbody>
        </table>
        <p v-if="!tasks.length" class="meta">尚无任务。</p>

        <h3>报告</h3>
        <template v-if="reports.length">
          <template v-if="report">
            <div v-if="report.status === 'diagnostic_failed'" class="diagnostic-banner">
              <strong>诊断结果（{{ report.status }}）— 不是已发布的正常报告</strong>
              <p class="meta">{{ report.review_reason }}</p>
              <pre>{{ JSON.stringify(diagnosticSummary, null, 2) }}</pre>
            </div>
            <template v-else>
              <div class="grid" style="margin-bottom:12px">
                <div class="metric"><span>状态</span><strong><span class="badge" :class="report.status">{{ report.status }}</span></strong></div>
                <div class="metric"><span>A 相 RMS</span><strong>{{ metric('Va')?.rms?.toFixed(4) ?? '—' }}</strong></div>
                <div class="metric"><span>A 相 THD</span><strong>{{ metric('Va')?.thd_percent?.toFixed(3) ?? '—' }}%</strong></div>
              </div>
              <p class="meta" v-if="report.review_reason">{{ report.review_reason }}</p>
              <SpectrumChart :report="report" />
              <SequenceTable :report="report" />
              <pre>{{ JSON.stringify(qualitySummary, null, 2) }}</pre>
            </template>
          </template>
        </template>
        <p v-else class="meta">尚无报告。失败/中断任务只保存阶段诊断，不会冒充完成；频谱阶段崩溃时连诊断行也不会发布。</p>
      </main>
    </div>
  </div>
</template>

<script setup>
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import * as echarts from 'echarts'
import { api } from './api'

const health = ref({ status: 'connecting', object_store: '-' })
const manifests = ref([])
const selectedId = ref(null)
const manifest = ref(null)
const chunks = ref([])
const issues = ref([])
const preview = ref({ segments: [] })
const calibrations = ref([])
const selectedCalibrationId = ref('')
const tasks = ref([])
const reports = ref([])
const reportId = ref(null)
const report = ref(null)
const expandedTask = ref(null)
const busyTask = ref(null)
const timer = ref(null)

const terminalStatuses = new Set(['succeeded', 'failed', 'cancelled'])
const statusOrder = ['queued', 'running', 'retry_wait', 'succeeded', 'failed']
const statusLabels = {
  queued: '排队', running: '运行中', retry_wait: '等待重试', succeeded: '成功', failed: '失败', cancelled: '已取消'
}
const canRun = (task) => ['queued', 'retry_wait'].includes(task.status)
function toggleTask(id) { expandedTask.value = expandedTask.value === id ? null : id }

const short = (value) => value ? `${String(value).slice(0, 8)}…` : '—'
const unwrap = async (promise) => {
  try { return await promise } catch (error) { console.warn(error); return null }
}
async function loadHealth() { health.value = await api.health() }
async function loadManifests() {
  manifests.value = await api.manifests()
  if (!selectedId.value && manifests.value.length) selectedId.value = manifests.value[0].id
}
async function refreshAll() {
  await loadHealth(); await loadManifests(); await loadDetail()
}
async function finalize() {
  try { await api.finalize(selectedId.value) } finally { await loadDetail() }
}
async function selectManifest(id) { selectedId.value = id; await loadDetail() }
async function loadDetail() {
  if (!selectedId.value) return
  manifest.value = await api.manifest(selectedId.value)
  const [chunkList, issueList, previewData, taskList, reportList] = await Promise.all([
    unwrap(api.chunks(selectedId.value)),
    unwrap(api.issues(selectedId.value)),
    unwrap(api.preview(selectedId.value, selectedCalibrationId.value)),
    unwrap(api.tasks(selectedId.value)),
    unwrap(api.reports(selectedId.value))
  ])
  chunks.value = chunkList || []
  issues.value = issueList || []
  preview.value = previewData || { segments: [] }
  tasks.value = taskList || []
  reports.value = reportList || []
  const published = reports.value.find((item) => item.status === 'published')
  const chosen = published || reports.value.find((item) => item.status === 'diagnostic_failed') || reports.value[0]
  reportId.value = chosen?.id || null
  report.value = chosen || null
  const calList = await unwrap(api.calibrations(manifest.value.channel_set_hash))
  calibrations.value = calList || []
  if (!selectedCalibrationId.value) {
    selectedCalibrationId.value = calibrations.value.find((item) => item.status === 'active')?.id || calibrations.value[0]?.id || ''
  }
}
async function createTask() {
  await api.createTask(selectedId.value, selectedCalibrationId.value)
  await loadDetail()
}
async function run(id) { busyTask.value = id; try { await api.runTask(id) } finally { busyTask.value = null; await loadDetail() } }
async function retry(id) {
  busyTask.value = id
  try {
    await api.retryTask(id)
  } catch (error) {
    // Server is the final gate (lease expiry, published report, duplicate
    // clicks); a 409 here means the button state was simply stale.
    console.warn('retry rejected', error)
    window.alert(`重试未执行：${error.message}`)
  } finally {
    busyTask.value = null
    await loadDetail()
  }
}
async function cancel(id) { await api.cancelTask(id); await loadDetail() }

const metric = (channel) => {
  const first = report.value?.result?.segments?.[0]?.channels?.[channel]
  return first || null
}
const qualitySummary = computed(() => {
  if (!report.value) return null
  return {
    status: report.value.result.quality_status,
    quality: report.value.result.quality,
    conventions: report.value.result.conventions,
    snapshot_digest: report.value.snapshot_digest
  }
})

// A diagnostic_failed row must never look like a normal report: expose only
// quality codes and messages, never the headline metrics/charts.
const diagnosticSummary = computed(() => {
  if (!report.value) return null
  return {
    report_status: report.value.status,
    quality_status: report.value.result?.quality_status,
    quality: report.value.result?.quality,
    snapshot_digest: report.value.snapshot_digest,
    note: 'diagnostic-only result; no published report exists for this task'
  }
})

const formatTime = (value) => value ? new Date(value).toLocaleTimeString('zh-CN', { hour12: false }) : '—'

const TaskTrace = {
  props: ['task'],
  setup(props) {
    // Collapse the event log into queued/running/retry_wait/succeeded/failed
    // timeline nodes, keeping the first entry and last timestamp per status.
    const nodes = computed(() => {
      const byStatus = new Map()
      for (const event of props.task.events || []) {
        const status = event.status
        if (!status || !statusOrder.includes(status)) continue
        const node = byStatus.get(status) || { status, firstAt: event.at, count: 0 }
        node.firstAt = node.firstAt || event.at
        node.count += 1
        byStatus.set(status, node)
      }
      const currentIndex = statusOrder.indexOf(props.task.status)
      return statusOrder.map((status, index) => {
        const node = byStatus.get(status)
        return {
          status,
          label: statusLabels[status],
          state: node ? 'reached' : (index <= currentIndex ? 'current' : 'pending'),
          at: node?.firstAt || null,
          count: node?.count || 0,
          current: index === currentIndex
        }
      })
    })

    const stageNames = {
      fixed_snapshot: '冻结快照',
      raw_inventory: '原始块清单',
      raw_bytes: '字节装载',
      segmentation: '分段',
      spectrum: '频谱'
    }
    const stageRows = computed(() =>
      Object.entries(props.task.stage_results || {}).map(([stage, detail]) => ({
        stage,
        label: stageNames[stage] || stage,
        at: detail?.at || null,
        detail
      }))
    )

    const takeoverEvents = computed(() =>
      (props.task.events || []).filter((event) =>
        ['lease_acquired', 'lease_taken', 'lease_recovered', 'retry_requested', 'cancel_requested'].includes(event.kind)
      )
    )

    const kindLabels = {
      status: '状态', stage: '阶段完成', lease_acquired: '租约获取', lease_taken: '租约接管',
      lease_recovered: '租约回收', retry_requested: '手动重试', cancel_requested: '取消请求'
    }

    return { nodes, stageRows, takeoverEvents, kindLabels, formatTime, statusLabels }
  },
  template: `
  <div class="trace">
    <ol class="timeline">
      <li v-for="node in nodes" :key="node.status" class="timeline-item" :class="node.state">
        <span class="dot"></span>
        <div>
          <strong>{{ node.label }}</strong>
          <span class="meta"> · {{ formatTime(node.at) }}<span v-if="node.count > 1"> · ×{{ node.count }}</span></span>
        </div>
      </li>
    </ol>

    <div class="trace-grid">
      <div>
        <h4>租约与心跳</h4>
        <table class="trace-table">
          <tbody>
            <tr><th>尝试次数</th><td>{{ task.attempts }}</td></tr>
            <tr><th>租约持有者</th><td>{{ task.lease_owner || '—' }}</td></tr>
            <tr><th>租约到期</th>
              <td>
                {{ formatTime(task.lease_until) }}
                <span v-if="task.status === 'running'" class="badge" :class="task.lease_expired ? 'failed' : 'succeeded'">
                  {{ task.lease_expired ? '已过期，可接管' : '有效' }}
                </span>
              </td>
            </tr>
            <tr><th>最近心跳</th><td>{{ formatTime(task.heartbeat_at) }}</td></tr>
          </tbody>
        </table>
        <ul class="event-list">
          <li v-for="(event, i) in takeoverEvents" :key="i">
            <span class="meta">{{ formatTime(event.at) }}</span>
            {{ kindLabels[event.kind] || event.kind }}
            <span class="meta" v-if="event.previous_owner">← {{ event.previous_owner }}</span>
            <span class="meta" v-if="event.owner">→ {{ event.owner }}</span>
          </li>
        </ul>
      </div>
      <div>
        <h4>阶段结果（固定快照口径）</h4>
        <table class="trace-table">
          <thead><tr><th>阶段</th><th>时间</th><th>关键结果</th></tr></thead>
          <tbody>
            <tr v-for="row in stageRows" :key="row.stage">
              <td>{{ row.label }}</td>
              <td>{{ formatTime(row.at) }}</td>
              <td class="meta">
                <template v-if="row.stage === 'spectrum'">
                  质量 {{ row.detail.quality_status }} · {{ (row.detail.quality_codes || []).join(', ') || '无 issue' }}
                </template>
                <template v-else-if="row.stage === 'segmentation'">
                  {{ row.detail.segments?.length || 0 }} 段
                </template>
                <template v-else-if="row.stage === 'raw_bytes'">{{ row.detail.bytes_loaded }} 字节</template>
                <template v-else-if="row.stage === 'raw_inventory'">{{ row.detail.count }} 块</template>
                <template v-else-if="row.stage === 'fixed_snapshot'">{{ String(row.detail.snapshot_digest).slice(0, 12) }}…</template>
              </td>
            </tr>
          </tbody>
        </table>
        <div v-if="task.error_code" class="trace-error">
          <strong>失败原因：{{ task.error_code }}</strong>
          <div>{{ task.error_message }}</div>
        </div>
        <div class="trace-retry" :class="{ blocked: !task.retry_allowed }">
          <strong>受控重试：</strong>
          <span :class="task.retry_allowed ? 'retry-ok' : 'meta'">
            {{ task.retry_allowed ? '允许' : '不允许' }} — {{ task.retry_reason }}
          </span>
        </div>
      </div>
    </div>
  </div>`
}

const GapChart = {
  props: ['chunks', 'manifest', 'issues'],
  setup(props) {
    const el = ref(null)
    let chart = null
    const render = () => {
      if (!el.value || !props.manifest) return
      chart ||= echarts.init(el.value)
      const total = props.manifest.expected_chunks.length
      const received = new Map(props.chunks.map((chunk) => [chunk.sequence, chunk]))
      const data = props.manifest.expected_chunks.map((item) => {
        const chunk = received.get(item.sequence)
        return {
          value: [item.sequence, item.sequence + 1, chunk ? 1 : -1],
          itemStyle: { color: chunk ? '#1c9b61' : '#d8274f' }
        }
      })
      chart.setOption({
        title: { text: '绿色已到 / 红色缺块', textStyle: { fontSize: 13 } },
        grid: { left: 40, right: 20, top: 35, bottom: 35 },
        xAxis: { type: 'value', name: '块序号', min: 0, max: total },
        yAxis: { type: 'category', data: ['chunk'], show: false },
        tooltip: { formatter: (p) => p.value[2] > 0 ? `块 ${p.value[0]} 已到` : `块 ${p.value[0]} 缺失` },
        series: [{ type: 'custom', renderItem: (_params, api) => {
          const values = api.value(2)
          const start = api.coord([api.value(0), 0]); const end = api.coord([api.value(1), 1])
          return { type: 'rect', shape: { x: start[0], y: start[1], width: Math.max(2, end[0] - start[0] - 1), height: end[1] - start[1] }, style: { fill: values > 0 ? '#1c9b61' : '#d8274f' } }
        }, data }]
      }, true)
    }
    watch(() => [props.chunks, props.manifest, props.issues], render, { deep: true })
    const resize = () => chart?.resize()
    onMounted(() => { render(); window.addEventListener('resize', resize) })
    onUnmounted(() => window.removeEventListener('resize', resize))
    return { el, render, resize }
  },
  template: '<div ref="el" class="chart"></div>'
}

const WaveformChart = {
  props: ['preview'],
  setup(props) {
    const el = ref(null)
    let chart
    const render = () => {
      if (!el.value) return
      chart ||= echarts.init(el.value)
      const series = []
      const axes = []
      ;(props.preview.segments || []).forEach((segment, segmentIndex) => {
        segment.series.forEach((entry) => {
          series.push({
            type: 'line',
            name: `${entry.channel} @${segment.sample_rate}Hz`,
            showSymbol: false,
            sampling: 'lttb',
            data: entry.values.map((value, index) => [segment.start_seconds + segment.times[index], value]),
            xAxisIndex: segmentIndex
          })
        })
        axes.push({ type: 'value', gridIndex: segmentIndex, name: 's', min: segment.start_seconds, max: segment.end_seconds })
      })
      chart.setOption({
        tooltip: { trigger: 'axis' },
        legend: { type: 'scroll', top: 0 },
        grid: (props.preview.segments || []).map((_, i) => ({ left: 55, right: 20, top: 35 + i * 245, height: 210 })),
        xAxis: axes,
        yAxis: (props.preview.segments || []).map(() => ({ type: 'value', name: '标定后' })),
        dataZoom: (props.preview.segments || []).map((_, i) => ({ type: 'inside', xAxisIndex: i })),
        series
      }, true)
    }
    watch(() => props.preview, render, { deep: true })
    const resize = () => chart?.resize()
    onMounted(() => { render(); window.addEventListener('resize', resize) })
    onUnmounted(() => window.removeEventListener('resize', resize))
    return { el, render, resize }
  },
  template: '<div ref="el" :style="{height: `${Math.max(280, (preview.segments||[]).length * 270)}px`}"></div>'
}

const SpectrumChart = {
  props: ['report'],
  setup(props) {
    const el = ref(null)
    let chart
    const render = () => {
      if (!el.value || !props.report) return
      chart ||= echarts.init(el.value)
      const segment = props.report.result.segments?.[0]
      const channels = segment ? Object.keys(segment.channels).slice(0, 3) : []
      const firstHarmonics = segment?.channels[channels[0]]?.windows?.[0]?.harmonics || []
      chart.setOption({
        title: { text: '各次谐波 RMS（固定标定版本）', textStyle: { fontSize: 14 } },
        tooltip: { trigger: 'axis' },
        legend: { data: channels, top: 25 },
        grid: { left: 55, right: 20, top: 70, bottom: 40 },
        xAxis: { type: 'category', name: '次数', data: firstHarmonics.map((h) => h.order) },
        yAxis: { type: 'value', name: 'RMS' },
        series: channels.map((channel) => ({
          name: channel, type: 'bar',
          data: (segment.channels[channel].windows[0].harmonics || []).map((h) => h.rms)
        }))
      }, true)
    }
    watch(() => props.report, render, { deep: true })
    const resize = () => chart?.resize()
    onMounted(() => { render(); window.addEventListener('resize', resize) })
    onUnmounted(() => window.removeEventListener('resize', resize))
    return { el, render, resize }
  },
  template: '<div ref="el" class="chart"></div>'
}

const SequenceTable = {
  props: ['report'],
  setup(props) {
    const rows = computed(() => {
      const segment = props.report?.result?.segments?.[0]
      const voltage = segment?.symmetrical_components?.voltage?.[0]
      if (!voltage || voltage.status !== 'ok') return []
      return ['positive', 'negative', 'zero'].map((name) => ({
        name,
        rms: voltage[`${name}_rms`],
        phase: voltage[`${name}_phase_deg`],
        phasor: `${voltage[`${name}_phasor`].real.toFixed(4)} ${voltage[`${name}_phasor`].imag >= 0 ? '+' : '−'} j${Math.abs(voltage[`${name}_phasor`].imag).toFixed(4)}`
      }))
    })
    return { rows }
  },
  template: `<table v-if="rows.length"><thead><tr><th>分量</th><th>RMS</th><th>相位°</th><th>峰值相量</th></tr></thead>
    <tbody><tr v-for="r in rows" :key="r.name"><td>{{r.name}}</td><td>{{r.rms.toFixed(5)}}</td><td>{{r.phase.toFixed(3)}}</td><td>{{r.phasor}}</td></tr></tbody></table>`
}

onMounted(async () => { await refreshAll(); timer.value = setInterval(loadDetail, 4000) })
onUnmounted(() => clearInterval(timer.value))
watch(selectedCalibrationId, () => loadDetail())
</script>
