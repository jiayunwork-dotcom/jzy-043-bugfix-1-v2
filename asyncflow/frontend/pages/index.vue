<template>
  <div>
    <h1 class="page-title">总览大盘</h1>
    <p class="page-sub">实时队列深度、吞吐、Worker 利用率与死信积压（每 2 秒刷新）</p>

    <div v-if="m.error" class="error-box">无法连接引擎接口：{{ m.error }}</div>

    <div class="grid cols-5">
      <div v-for="p in priorities" :key="p.key" class="card">
        <div class="flex-between">
          <span class="pill" :class="p.cls">{{ p.label }}</span>
          <span class="muted" style="font-size: 11px">就绪队列</span>
        </div>
        <div class="metric-big mt">{{ m.depth(p.key) }}</div>
        <div class="queue-bar" :style="{ background: 'var(--panel-2)' }">
          <div
            class="bar-fill"
            :style="{
              width: depthPct(m.depth(p.key)) + '%',
              background: p.color,
            }"
          />
        </div>
        <div class="metric-sub" v-if="priorityStat(p.key)">
          成功率 {{ pct(priorityStat(p.key)!.success_rate) }} · 失败率
          {{ pct(priorityStat(p.key)!.failure_rate) }}
        </div>
        <div class="metric-sub" v-else>暂无执行记录</div>
      </div>
    </div>

    <div class="grid cols-4 mt">
      <div class="card">
        <h3>每秒完成数</h3>
        <div class="metric-big">{{ m.data?.completions_per_sec.toFixed(2) ?? '—' }}</div>
        <div class="metric-sub">最近 60 秒平均</div>
      </div>
      <div class="card">
        <h3>Worker 在线 / 离线</h3>
        <div class="metric-big">
          <span style="color: var(--success)">{{ m.data?.workers.online ?? 0 }}</span>
          <span class="muted" style="font-size: 18px"> / </span>
          <span style="color: var(--failed)">{{ m.data?.workers.offline ?? 0 }}</span>
        </div>
        <div class="metric-sub">
          槽位 {{ m.data?.workers.used_slots ?? 0 }} / {{ m.data?.workers.total_slots ?? 0 }}
        </div>
      </div>
      <div class="card">
        <h3>Worker 利用率</h3>
        <div class="metric-big">{{ pct(m.data?.workers.utilization ?? 0) }}</div>
        <div class="bar-track mt">
          <div
            class="bar-fill"
            :style="{
              width: (m.data?.workers.utilization ?? 0) * 100 + '%',
              background: 'linear-gradient(90deg,var(--normal),var(--low))',
            }"
          />
        </div>
      </div>
      <div class="card">
        <h3>死信积压</h3>
        <div class="metric-big" style="color: var(--dead)">{{ m.data?.dlq_count ?? 0 }}</div>
        <div class="metric-sub">
          延迟等待 {{ m.data?.waiting_delay ?? 0 }} · 重试等待 {{ m.data?.waiting_retry ?? 0 }}
        </div>
      </div>
    </div>

    <div class="grid cols-2 mt">
      <div class="card">
        <h3>吞吐趋势（每秒完成 / 失败）</h3>
        <Sparkline :values="completedSeries" />
        <div class="flex-between mt" style="font-size: 12px">
          <span class="muted">{{ oldestLabel }}</span>
          <span><span style="color: var(--accent)">■</span> 完成 <span class="muted" style="margin-left: 8px">峰值 {{ peakCompleted }}/s</span></span>
          <span class="muted">现在</span>
        </div>
      </div>
      <div class="card">
        <h3>死信积压增长趋势</h3>
        <Sparkline :values="dlqSeries" :min-bars="30" />
        <div class="flex-between mt" style="font-size: 12px">
          <span class="muted">最近约 1 小时（每 5 秒采样）</span>
          <span>当前 {{ m.data?.dlq_count ?? 0 }}</span>
        </div>
      </div>
    </div>

    <div class="card mt">
      <h3>各优先级平均执行延迟</h3>
      <div class="grid cols-5">
        <div v-for="p in priorities" :key="p.key">
          <div class="muted" style="font-size: 12px; margin-bottom: 4px">{{ p.label }}</div>
          <div style="font-size: 18px; font-weight: 600">
            {{ m.data?.avg_latency_ms?.[p.key] != null ? Math.round(m.data!.avg_latency_ms[p.key]) + ' ms' : '—' }}
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useMetricsStore } from '~/stores/metrics'
import { usePolling } from '~/composables/usePolling'

const m = useMetricsStore()
usePolling(() => m.load(), 2000)

const priorities = [
  { key: 'critical', label: 'Critical', cls: 'critical', color: 'var(--critical)' },
  { key: 'high', label: 'High', cls: 'high', color: 'var(--high)' },
  { key: 'normal', label: 'Normal', cls: 'normal', color: 'var(--normal)' },
  { key: 'low', label: 'Low', cls: 'low', color: 'var(--low)' },
  { key: 'bulk', label: 'Bulk', cls: 'bulk', color: 'var(--bulk)' },
]

const maxDepth = computed(() =>
  Math.max(1, ...priorities.map((p) => m.depth(p.key))),
)
function depthPct(v: number) {
  return Math.max(v > 0 ? 6 : 0, Math.round((v / maxDepth.value) * 100))
}
function pct(v: number) {
  return (v * 100).toFixed(1) + '%'
}
function priorityStat(key: string) {
  return m.data?.priority_stats?.[key]
}

const completedSeries = computed(
  () => (m.data?.throughput ?? []).map((p) => p.completed) as number[],
)
const peakCompleted = computed(() => Math.max(0, ...completedSeries.value))
const dlqSeries = computed(() => (m.data?.dlq_growth ?? []).map((p) => p.count) as number[])
const oldestLabel = computed(() => {
  const t = m.data?.throughput?.[0]?.t
  return t ? new Date(t).toLocaleTimeString('zh-CN', { hour12: false }) : ''
})
</script>
