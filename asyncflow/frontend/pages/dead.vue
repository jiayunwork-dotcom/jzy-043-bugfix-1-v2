<template>
  <div>
    <h1 class="page-title">死信管理</h1>
    <p class="page-sub">重试耗尽的任务；支持查看每次执行错误、单条/批量重试、批量丢弃、按错误原因聚合</p>

    <div class="grid cols-3">
      <div class="card" v-for="a in store.aggregates" :key="a.category">
        <div class="flex-between">
          <span class="pill" style="background: rgba(176,107,255,.2); color: var(--dead)">{{ a.category }}</span>
          <span class="metric-big" style="font-size: 24px">{{ a.count }}</span>
        </div>
      </div>
      <div class="card" v-if="!store.aggregates.length">
        <div class="muted">暂无死信，错误原因聚合将显示在这里</div>
      </div>
    </div>

    <div class="card mt">
      <div class="toolbar">
        <button :disabled="!store.selected.size" @click="store.retry([...store.selected])">
          批量重试 ({{ store.selected.size }})
        </button>
        <button class="danger" :disabled="!store.selected.size" @click="confirmDiscard">
          批量丢弃 ({{ store.selected.size }})
        </button>
        <button class="ghost" @click="store.selectAll()">全选</button>
        <button class="ghost" @click="store.clearSelection()">清除选择</button>
        <span class="muted" style="margin-left: auto">共 {{ store.total }} 条死信</span>
      </div>

      <div v-if="store.error" class="error-box">{{ store.error }}</div>
      <div v-if="store.loading" class="empty">加载中…</div>
      <div v-else-if="!store.dead.length" class="empty">🎉 当前没有死信任务</div>
      <table v-else>
        <thead>
          <tr>
            <th style="width: 36px"></th>
            <th>任务 ID</th>
            <th>类型</th>
            <th>优先级</th>
            <th>尝试次数</th>
            <th>错误分类</th>
            <th>最近错误</th>
            <th>死信时间</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="d in store.dead" :key="d.task.id">
            <td>
              <input
                type="checkbox"
                style="padding: 0"
                :checked="store.selected.has(d.task.id)"
                @change="store.toggle(d.task.id)"
              />
            </td>
            <td class="mono id-cell" :title="d.task.id">{{ d.task.id }}</td>
            <td>{{ d.task.type }}</td>
            <td><PriorityPill :priority="d.task.priority" /></td>
            <td>{{ d.attempts.length }}</td>
            <td>
              <span class="pill" style="background: rgba(176,107,255,.2); color: var(--dead)">
                {{ d.task.error_category || 'unknown' }}
              </span>
            </td>
            <td class="id-cell" :title="d.task.last_error" style="max-width: 240px; color: #ffb3bb">
              {{ d.task.last_error }}
            </td>
            <td>{{ fmtDate(d.dead_at) }}</td>
            <td class="right" style="white-space: nowrap">
              <button class="small ghost" @click="store.open(d)">详情</button>
              <button class="small" style="margin-left: 6px" @click="store.retry([d.task.id])">重试</button>
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
              <div style="font-weight: 700; font-size: 16px">死信任务详情</div>
              <div class="mono muted" style="font-size: 12px">{{ store.detail.task.id }}</div>
            </div>
            <div class="flex gap">
              <button @click="store.retry([store.detail.task.id]).then(() => store.close())">重新入队</button>
              <button class="ghost small" @click="store.close()">关闭</button>
            </div>
          </div>
          <div class="modal-body">
            <div class="grid cols-2" style="gap: 10px">
              <div><div class="muted" style="font-size: 11px">类型</div>{{ store.detail.task.type }}</div>
              <div><div class="muted" style="font-size: 11px">优先级</div>
                <PriorityPill :priority="store.detail.task.priority" />
              </div>
              <div><div class="muted" style="font-size: 11px">负载</div>
                <pre class="mono" style="white-space: pre-wrap; font-size: 12px">{{ JSON.stringify(store.detail.task.payload, null, 2) }}</pre>
              </div>
              <div><div class="muted" style="font-size: 11px">回调地址</div>
                <span class="mono" style="font-size: 12px">{{ store.detail.task.callback_url || '—' }}</span>
              </div>
            </div>

            <h3 style="margin: 18px 0 12px; color: var(--text-dim); font-size: 13px">
              每次执行的错误信息（{{ store.detail.attempts.length }}）
            </h3>
            <div v-for="a in store.detail.attempts" :key="a.attempt_no" class="timeline-item">
              <span class="timeline-dot" :class="a.status" />
              <div class="flex-between">
                <strong>第 {{ a.attempt_no }} 次 · {{ a.status }}</strong>
                <span class="muted mono" style="font-size: 11px">
                  {{ fmtDate(a.started_at) }} → {{ fmtDate(a.ended_at) }}
                </span>
              </div>
              <div class="muted" style="font-size: 12px">Worker: {{ a.worker_id || '—' }}</div>
              <div v-if="a.error" style="margin-top: 6px; color: #ffb3bb; font-size: 12px">
                <span class="pill" style="background: rgba(176,107,255,.2); color: var(--dead)">{{ a.error_category }}</span>
                {{ a.error }}
              </div>
            </div>
          </div>
        </div>
      </div>
    </Teleport>
  </div>
</template>

<script setup lang="ts">
import { useDeadStore } from '~/stores/dead'
import { usePolling } from '~/composables/usePolling'
import { useFormat } from '~/composables/useFormat'

const store = useDeadStore()
const { fmtDate } = useFormat()
usePolling(() => store.load(), 3000)

function confirmDiscard() {
  if (confirm(`确认丢弃选中的 ${store.selected.size} 条死信任务？该操作不可撤销。`)) {
    store.discard([...store.selected])
  }
}
</script>
