<template>
  <div>
    <h1 class="page-title">任务列表</h1>
    <p class="page-sub">按状态 / 优先级 / 类型 / 时间范围筛选，点开查看每次尝试的执行时间线</p>

    <div class="card">
      <div class="toolbar">
        <div class="field">
          <label>状态</label>
          <select v-model="store.filters.status">
            <option value="">全部</option>
            <option value="pending">等待</option>
            <option value="ready">就绪</option>
            <option value="running">执行中</option>
            <option value="succeeded">成功</option>
            <option value="failed">失败</option>
            <option value="dead">死信</option>
            <option value="canceled">已取消</option>
          </select>
        </div>
        <div class="field">
          <label>优先级</label>
          <select v-model="store.filters.priority">
            <option value="">全部</option>
            <option value="critical">Critical</option>
            <option value="high">High</option>
            <option value="normal">Normal</option>
            <option value="low">Low</option>
            <option value="bulk">Bulk</option>
          </select>
        </div>
        <div class="field">
          <label>类型</label>
          <input v-model="store.filters.type" placeholder="如 sample.echo" />
        </div>
        <div class="field">
          <label>起始时间</label>
          <input v-model="store.filters.from" type="datetime-local" />
        </div>
        <div class="field">
          <label>结束时间</label>
          <input v-model="store.filters.to" type="datetime-local" />
        </div>
        <div class="field">
          <label>&nbsp;</label>
          <button @click="reload">筛选</button>
        </div>
        <div class="field">
          <label>&nbsp;</label>
          <button class="ghost" @click="store.resetFilters(); reload()">重置</button>
        </div>
        <div class="field" style="margin-left: auto">
          <label>&nbsp;</label>
          <span class="muted">共 {{ store.total }} 条</span>
        </div>
      </div>

      <div v-if="store.error" class="error-box">{{ store.error }}</div>

      <div v-if="store.loading" class="empty">加载中…</div>
      <div v-else-if="!store.tasks.length" class="empty">暂无任务</div>
      <table v-else>
        <thead>
          <tr>
            <th>任务 ID</th>
            <th>类型</th>
            <th>优先级</th>
            <th>状态</th>
            <th>尝试</th>
            <th>Worker</th>
            <th>创建时间</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="t in store.tasks" :key="t.id">
            <td class="mono id-cell" :title="t.id">{{ t.id }}</td>
            <td>{{ t.type }}</td>
            <td><PriorityPill :priority="t.priority" /></td>
            <td><StatusBadge :status="t.status" /></td>
            <td>{{ t.attempts }}/{{ t.max_retries }}</td>
            <td class="mono" style="max-width: 110px; overflow: hidden; text-overflow: ellipsis">
              {{ t.worker_id || '—' }}
            </td>
            <td>{{ fmtDate(t.created_at) }}</td>
            <td class="right"><button class="small ghost" @click="store.open(t.id)">时间线</button></td>
          </tr>
        </tbody>
      </table>

      <div class="flex-between mt">
        <span class="muted" style="font-size: 12px">
          显示 {{ store.tasks.length }} 条
        </span>
        <div class="flex gap">
          <button
            class="small ghost"
            :disabled="store.filters.offset === 0"
            @click="store.filters.offset = Math.max(0, store.filters.offset - store.filters.limit); reload()"
          >
            上一页
          </button>
          <button
            class="small ghost"
            :disabled="store.filters.offset + store.filters.limit >= store.total"
            @click="store.filters.offset += store.filters.limit; reload()"
          >
            下一页
          </button>
        </div>
      </div>
    </div>

    <Teleport to="body">
      <div v-if="store.selected" class="modal-backdrop" @click.self="store.close()">
        <div class="modal">
          <div class="modal-header">
            <div>
              <div style="font-weight: 700; font-size: 16px">任务执行时间线</div>
              <div class="mono muted" style="font-size: 12px">{{ store.selected.task.id }}</div>
            </div>
            <button class="ghost small" @click="store.close()">关闭</button>
          </div>
          <div class="modal-body">
            <div class="grid cols-3" style="gap: 10px; margin-bottom: 18px">
              <div><div class="muted" style="font-size: 11px">类型</div>{{ store.selected.task.type }}</div>
              <div>
                <div class="muted" style="font-size: 11px">优先级</div>
                <PriorityPill :priority="store.selected.task.priority" />
              </div>
              <div>
                <div class="muted" style="font-size: 11px">状态</div>
                <StatusBadge :status="store.selected.task.status" />
              </div>
              <div><div class="muted" style="font-size: 11px">负载</div>
                <pre class="mono" style="margin: 0; white-space: pre-wrap; font-size: 12px">{{ JSON.stringify(store.selected.task.payload, null, 2) }}</pre>
              </div>
              <div v-if="store.selected.task.callback_url">
                <div class="muted" style="font-size: 11px">回调</div>
                <span class="mono" style="font-size: 12px">{{ store.selected.task.callback_url }}</span>
              </div>
              <div v-if="store.selected.task.last_error">
                <div class="muted" style="font-size: 11px">最近错误</div>
                <span style="color: var(--failed); font-size: 12px">
                  [{{ store.selected.task.error_category }}] {{ store.selected.task.last_error }}
                </span>
              </div>
            </div>

            <h3 style="margin: 8px 0 14px; color: var(--text-dim); font-size: 13px">
              尝试记录（{{ store.selected.attempts.length }}）
            </h3>
            <div v-if="!store.selected.attempts.length" class="empty">暂无尝试</div>
            <div v-for="a in store.selected.attempts" :key="a.attempt_no" class="timeline-item">
              <span class="timeline-dot" :class="a.status" />
              <div class="flex-between">
                <strong>第 {{ a.attempt_no }} 次尝试 · {{ a.status }}</strong>
                <span class="muted mono" style="font-size: 11px">
                  {{ fmtDate(a.started_at) }} → {{ fmtDate(a.ended_at) }}
                  ({{ duration(a.started_at, a.ended_at) }})
                </span>
              </div>
              <div class="muted" style="font-size: 12px; margin-top: 2px">
                Worker: <span class="mono">{{ a.worker_id || '—' }}</span>
              </div>
              <div v-if="a.error" style="margin-top: 6px; color: #ffb3bb; font-size: 12px">
                <span class="pill" style="background: rgba(176,107,255,.2); color: var(--dead)">{{ a.error_category }}</span>
                {{ a.error }}
              </div>
            </div>

            <h3 style="margin: 18px 0 10px; color: var(--text-dim); font-size: 13px">审计轨迹</h3>
            <table>
              <thead>
                <tr><th>动作</th><th>from</th><th>to</th><th>执行者</th><th>时间</th></tr>
              </thead>
              <tbody>
                <tr v-for="(row, i) in store.selected.audit" :key="i">
                  <td>{{ row.action }}</td>
                  <td>{{ row.from_state || '—' }}</td>
                  <td>{{ row.to_state || '—' }}</td>
                  <td>{{ row.actor }}</td>
                  <td class="mono" style="font-size: 11px">{{ fmtDate(row.created_at as string) }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </Teleport>
  </div>
</template>

<script setup lang="ts">
import { useTasksStore } from '~/stores/tasks'
import { usePolling } from '~/composables/usePolling'
import { useFormat } from '~/composables/useFormat'

const store = useTasksStore()
const { fmtDate, duration } = useFormat()

function reload() {
  return store.load()
}
usePolling(reload, 3000)
</script>
