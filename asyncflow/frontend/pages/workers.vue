<template>
  <div>
    <h1 class="page-title">Worker 集群</h1>
    <p class="page-sub">每个 Worker 的在线状态、心跳、槽位占用、正在执行的任务与历史统计</p>

    <div class="card">
      <div v-if="store.error" class="error-box">{{ store.error }}</div>
      <div v-if="store.loading" class="empty">加载中…</div>
      <div v-else-if="!store.workers.length" class="empty">尚未注册 Worker</div>
      <table v-else>
        <thead>
          <tr>
            <th>Worker</th>
            <th>状态</th>
            <th>能力（任务类型）</th>
            <th>槽位占用</th>
            <th>最近心跳</th>
            <th>执行中</th>
            <th>完成 / 失败</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="w in store.workers" :key="w.id">
            <td>
              <div style="font-weight: 600">{{ w.name }}</div>
              <div class="mono muted" style="font-size: 11px">{{ w.id }}</div>
            </td>
            <td>
              <span class="st" :class="w.online ? (w.status === 'draining' ? 'paused' : 'succeeded') : 'failed'">
                {{ w.status === 'draining' ? '优雅关闭中' : w.online ? '在线' : '离线' }}
              </span>
            </td>
            <td>
              <span v-for="c in w.capabilities.slice(0, 4)" :key="c" class="pill bulk" style="margin-right: 4px">{{ c }}</span>
            </td>
            <td style="min-width: 130px">
              <div class="bar-track">
                <div
                  class="bar-fill"
                  :style="{
                    width: w.total_slots ? (w.used_slots / w.total_slots) * 100 + '%' : '0%',
                    background: w.used_slots >= w.total_slots ? 'var(--high)' : 'var(--normal)',
                  }"
                />
              </div>
              <div class="muted" style="font-size: 11px; margin-top: 3px">{{ w.used_slots }}/{{ w.total_slots }}</div>
            </td>
            <td>
              <div>{{ fmtDate(w.last_heartbeat) }}</div>
              <div class="muted" style="font-size: 11px">{{ heartbeatAge(w.last_heartbeat) }}</div>
            </td>
            <td>{{ w.current_tasks.length }}</td>
            <td>
              <span style="color: var(--success)">{{ w.completed_count }}</span>
              <span class="muted"> / </span>
              <span :style="{ color: w.failed_count ? 'var(--failed)' : undefined }">{{ w.failed_count }}</span>
            </td>
            <td class="right" style="white-space: nowrap">
              <button class="small ghost" @click="store.open(w.id)">详情</button>
              <button
                v-if="w.online && w.status !== 'draining'"
                class="small ghost"
                style="margin-left: 6px"
                @click="store.drain(w.id)"
              >
                优雅关闭
              </button>
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <Teleport to="body">
      <div v-if="store.detail" class="modal-backdrop" @click.self="store.close()">
        <div class="modal">
          <div class="modal-header">
            <div>
              <div style="font-weight: 700; font-size: 16px">{{ store.detail.worker.name }}</div>
              <div class="mono muted" style="font-size: 12px">{{ store.detail.worker.id }}</div>
            </div>
            <button class="ghost small" @click="store.close()">关闭</button>
          </div>
          <div class="modal-body">
            <h3 style="color: var(--text-dim); font-size: 13px">正在执行的任务</h3>
            <div v-if="!store.detail.tasks.length" class="empty">当前没有执行中的任务</div>
            <table v-else>
              <thead>
                <tr><th>任务 ID</th><th>类型</th><th>优先级</th><th>尝试</th><th>开始时间</th></tr>
              </thead>
              <tbody>
                <tr v-for="t in store.detail.tasks" :key="t.id">
                  <td class="mono id-cell" :title="t.id">{{ t.id }}</td>
                  <td>{{ t.type }}</td>
                  <td><PriorityPill :priority="t.priority" /></td>
                  <td>{{ t.attempts }}</td>
                  <td>{{ fmtDate(t.started_at) }}</td>
                </tr>
              </tbody>
            </table>

            <h3 style="color: var(--text-dim); font-size: 13px; margin-top: 20px">历史统计</h3>
            <div class="grid cols-3">
              <div class="card"><div class="muted" style="font-size: 11px">累计完成</div>
                <div class="metric-big" style="font-size: 24px; color: var(--success)">{{ store.detail.worker.completed_count }}</div>
              </div>
              <div class="card"><div class="muted" style="font-size: 11px">累计失败</div>
                <div class="metric-big" style="font-size: 24px; color: var(--failed)">{{ store.detail.worker.failed_count }}</div>
              </div>
              <div class="card"><div class="muted" style="font-size: 11px">利用率</div>
                <div class="metric-big" style="font-size: 24px">
                  {{ store.detail.worker.total_slots
                    ? Math.round((store.detail.worker.used_slots / store.detail.worker.total_slots) * 100)
                    : 0 }}%
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </Teleport>
  </div>
</template>

<script setup lang="ts">
import { useWorkersStore } from '~/stores/workers'
import { usePolling } from '~/composables/usePolling'
import { useFormat } from '~/composables/useFormat'

const store = useWorkersStore()
const { fmtDate } = useFormat()
usePolling(() => store.load(), 2000)

function heartbeatAge(s: string): string {
  const diff = Date.now() - new Date(s).getTime()
  if (Number.isNaN(diff)) return '—'
  const sec = Math.floor(diff / 1000)
  if (sec < 60) return `${sec} 秒前`
  return `${Math.floor(sec / 60)} 分${sec % 60} 秒前`
}
</script>
