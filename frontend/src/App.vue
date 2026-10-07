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
        <div v-if="retryError" class="issue error">
          <strong>重试被拒绝</strong> — {{ retryError }}
          <button class="secondary" style="float:right" @click="retryError = null">知道了</button>
        </div>
        <table>
          <thead><tr><th>任务</th><th>状态</th><th>标定</th><th>尝试</th><th>租约 / 心跳</th><th>失败原因</th><th>操作</th></tr></thead>
          <tbody>
            <tr v-for="task in tasks" :key="task.id" :class="{ active: selectedTaskId === task.id }">
              <td><a href="#" @click.prevent="toggleTimeline(task.id)">{{ short(task.id) }}</a></td>
              <td><span class="badge" :class="task.status">{{ task.status }}</span>
                <div v-if="task.cancellation_requested" class="meta">取消请求中</div></td>
              <td>{{ short(task.calibration_version_id) }}</td>
              <td>{{ task.attempts }}</td>
              <td class="meta">
                <template v-if="task.lease_owner">
                  <div>{{ task.lease_owner }}</div>
                  <div :style="{ color: leaseActive(task) ? '#116c36' : '#a01636' }">
                    租约{{ leaseActive(task) ? '至 ' + fmtTime(task.lease_until) : '已过期，可接管' }}
                  </div>
                </template>
                <span v-else>—</span>
                <div v-if="task.heartbeat_at">心跳 {{ fromNow(task.heartbeat_at) }}</div>
              </td>
              <td class="meta">{{ task.error_code ? `${task.error_code}：${task.error_message}` : '—' }}</td>
              <td>
                <button @click="run(task.id)">同步执行</button>
                <button
                  class="secondary"
                  :disabled="!canRetry(task) || retrying.has(task.id)"
                  :title="retryHint(task)"
                  @click="retry(task)"
                >{{ retrying.has(task.id) ? '重试中…' : '重试' }}</button>
                <button class="danger" @click="cancel(task.id)">取消</button>
              </td>
            </tr>
          </tbody>
        </table>
        <p class="meta">点击任务 id 查看运行轨迹。运行中的任务在租约过期前不可接管重试；重复点击重试最多发布一份报告。</p>

        <TaskTimeline v-if="timeline" :timeline="timeline" />

        <h3>报告</h3>
        <template v-if="report && report.status === 'diagnostic_failed'">
          <div class="issue error">
            <strong>[诊断] 分析未通过质量检查，仅保留诊断结果，未发布正常报告。</strong><br>
            {{ report.review_reason }}
          </div>
          <div v-for="(item, index) in report.result.quality" :key="index" class="issue" :class="item.severity">
            <strong>[{{ item.severity }}] {{ item.code }}</strong> — {{ item.message }}
          </div>
          <p class="meta">快照 {{ short(report.snapshot_digest) }} · 标定版本 {{ short(report.calibration_version_id) }} · 该结果不可作为完成报告使用。</p>
        </template>
        <template v-else-if="report">
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
        <template v-else>
          <div v-if="latestFailedTask" class="issue error">
            <strong>[诊断] 最近任务 {{ latestFailedTask.status }}：{{ latestFailedTask.error_code }}</strong>
            — {{ latestFailedTask.error_message }}<br>
            <span class="meta">已完成阶段：{{ Object.keys(latestFailedTask.stage_results || {}).join(' → ') || '无' }}。失败任务只保存阶段诊断，不会冒充完成报告。</span>
          </div>
          <p v-else class="meta">尚无已发布报告。失败任务只保存阶段诊断，不会冒充完成。</p>
        </template>
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
const selectedTaskId = ref(null)
const timeline = ref(null)
const retrying = ref(new Set())
const retryError = ref(null)
const nowTick = ref(Date.now())
const timer = ref(null)

const short = (value) => value ? `${String(value).slice(0, 8)}…` : '—'
const fmtTime = (iso) => (iso ? new Date(iso).toLocaleTimeString('zh-CN', { hour12: false }) : '—')
const fromNow = (iso) => {
  if (!iso) return '—'
  const seconds = Math.max(0, Math.round((nowTick.value - new Date(iso).getTime()) / 1000))
  if (seconds < 60) return `${seconds}s 前`
  if (seconds < 3600) return `${Math.floor(seconds / 60)}min 前`
  return fmtTime(iso)
}
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
async function selectManifest(id) { selectedId.value = id; selectedTaskId.value = null; timeline.value = null; await loadDetail() }
async function loadDetail() {
  if (!selectedId.value) return
  nowTick.value = Date.now()
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
  // A diagnostic_failed report is never shown as a normal report; prefer a
  // real published/needs_review report and fall back to the diagnostic view.
  const chosen = reports.value.find((item) => item.status !== 'diagnostic_failed') || reports.value[0]
  reportId.value = chosen?.id || null
  report.value = chosen || null
  const calList = await unwrap(api.calibrations(manifest.value.channel_set_hash))
  calibrations.value = calList || []
  if (!selectedCalibrationId.value) {
    selectedCalibrationId.value = calibrations.value.find((item) => item.status === 'active')?.id || calibrations.value[0]?.id || ''
  }
  if (selectedTaskId.value) await loadTimeline()
}
async function createTask() {
  await api.createTask(selectedId.value, selectedCalibrationId.value)
  await loadDetail()
}
async function run(id) { await api.runTask(id); await loadDetail() }
const leaseActive = (task) =>
  task.status === 'running' && !!task.lease_until && new Date(task.lease_until).getTime() > nowTick.value
const canRetry = (task) => {
  if (['failed', 'retry_wait', 'cancelled'].includes(task.status)) return true
  if (task.status === 'running') return !leaseActive(task) // 仅租约过期后可接管
  return false
}
const retryHint = (task) => {
  if (task.status === 'succeeded') return '任务已成功：已发布报告不可被重试替换'
  if (task.status === 'queued') return '任务排队中，直接执行即可'
  if (task.status === 'running') {
    return leaseActive(task)
      ? `租约由 ${task.lease_owner} 持有至 ${fmtTime(task.lease_until)}，到期后方可安全接管`
      : '租约已过期，可安全接管并重试'
  }
  if (task.status === 'retry_wait') return '等待重试：重复点击是幂等的，最多发布一份报告'
  if (task.status === 'failed') return `失败原因 ${task.error_code || '未知'}，可重试`
  return '可重试'
}
async function retry(task) {
  if (!canRetry(task) || retrying.value.has(task.id)) return
  retryError.value = null
  retrying.value = new Set(retrying.value).add(task.id)
  try {
    await api.retryTask(task.id)
  } catch (error) {
    retryError.value = `任务 ${short(task.id)}：${error.message}`
  } finally {
    const next = new Set(retrying.value)
    next.delete(task.id)
    retrying.value = next
    await loadDetail()
  }
}
async function cancel(id) { await api.cancelTask(id); await loadDetail() }
async function loadTimeline() {
  if (!selectedTaskId.value) return
  timeline.value = await unwrap(api.taskTimeline(selectedTaskId.value))
}
async function toggleTimeline(id) {
  selectedTaskId.value = selectedTaskId.value === id ? null : id
  timeline.value = null
  if (selectedTaskId.value) await loadTimeline()
}

const latestFailedTask = computed(() =>
  tasks.value.find((task) => ['failed', 'retry_wait'].includes(task.status) && task.error_code) || null
)

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

const EVENT_LABELS = {
  queued: '入队',
  running: '开始运行',
  stage: '阶段完成',
  lease_recovered: '租约接管',
  retry_wait: '等待重试',
  succeeded: '成功',
  failed: '失败',
  cancelled: '已取消',
  cancel_requested: '取消请求'
}

const TaskTimeline = {
  props: ['timeline'],
  setup() {
    const fmt = (iso) => (iso ? new Date(iso).toLocaleString('zh-CN', { hour12: false }) : '—')
    const stageText = (d) => {
      switch (d.stage) {
        case 'fixed_snapshot': return `冻结快照 ${short(d.snapshot_digest)}`
        case 'raw_inventory': return `清单核对 ${d.count} 块（序号 ${(d.sequences || []).join(', ')}）`
        case 'raw_bytes': return `装载 ${(d.bytes_loaded || 0).toLocaleString()} 字节并校验摘要`
        case 'segmentation': return `按固定采样率分 ${(d.segments || []).length} 段，不插值`
        case 'spectrum': return `频谱完成，质量 ${d.quality_status}${d.quality_codes?.length ? `（${d.quality_codes.join(', ')}）` : ''}`
        default: return d.stage
      }
    }
    const describe = (event) => {
      const d = event.detail || {}
      switch (event.event) {
        case 'queued': return `标定 ${short(d.calibration_version_id)} · 快照 ${short(d.snapshot_digest)}`
        case 'running': return `worker ${d.worker_id} · 第 ${d.attempt} 次尝试 · 租约至 ${fmt(d.lease_until)}`
        case 'lease_recovered':
          return d.recovered_by === 'maintenance'
            ? `原租约（${d.previous_owner || '未知'}）已过期，维护任务回收为待重试`
            : `原租约已过期，${d.worker_id} 接管（第 ${d.attempt} 次尝试，租约至 ${fmt(d.lease_until)}）`
        case 'stage': return stageText(d)
        case 'retry_wait':
          return d.error_code
            ? `${d.error_code}：${d.error_message}`
            : `手动重试（原状态 ${d.previous_status}），已释放租约`
        case 'failed': return `${d.error_code || ''} ${d.error_message || ''}`.trim() || '失败'
        case 'succeeded': return `报告已发布（${d.report_status}）· 快照 ${short(d.snapshot_digest)}`
        case 'cancelled': return d.reason || '已取消'
        case 'cancel_requested': return '已请求取消，等待 worker 到达检查点'
        default: return JSON.stringify(d)
      }
    }
    return { fmt, describe, labels: EVENT_LABELS, short }
  },
  template: `
    <div class="timeline-panel">
      <h4>
        任务轨迹 {{ short(timeline.task.id) }}
        <span class="badge" :class="timeline.task.status">{{ timeline.task.status }}</span>
      </h4>
      <div class="meta">
        尝试 {{ timeline.task.attempts }} 次
        <span v-if="timeline.task.lease_owner"> · 租约 {{ timeline.task.lease_owner }} 至 {{ fmt(timeline.task.lease_until) }}</span>
        <span v-else> · 租约已释放</span>
        <span v-if="timeline.task.heartbeat_at"> · 最近心跳 {{ fmt(timeline.task.heartbeat_at) }}</span>
        <span v-if="timeline.task.error_code"> · 失败原因 {{ timeline.task.error_code }}</span>
        <span v-if="timeline.report"> · 报告 {{ timeline.report.status }}</span>
        <span v-else> · 无报告（仅阶段诊断）</span>
      </div>
      <ol class="timeline">
        <li v-for="event in timeline.events" :key="event.id" :class="event.status">
          <span class="dot"></span>
          <div class="entry">
            <strong>{{ labels[event.event] || event.event }}</strong>
            <span class="meta"> · {{ fmt(event.created_at) }}</span>
            <div class="meta">{{ describe(event) }}</div>
          </div>
        </li>
      </ol>
    </div>`
}

onMounted(async () => { await refreshAll(); timer.value = setInterval(loadDetail, 4000) })
onUnmounted(() => clearInterval(timer.value))
watch(selectedCalibrationId, () => loadDetail())
</script>
