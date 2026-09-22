import type { Task } from '~/lib/api'

const PRIORITIES = [
  { key: 'critical', label: 'Critical', cls: 'critical' },
  { key: 'high', label: 'High', cls: 'high' },
  { key: 'normal', label: 'Normal', cls: 'normal' },
  { key: 'low', label: 'Low', cls: 'low' },
  { key: 'bulk', label: 'Bulk', cls: 'bulk' },
]

const STATUS_LABELS: Record<string, string> = {
  pending: '等待',
  ready: '就绪',
  running: '执行中',
  succeeded: '成功',
  failed: '失败',
  dead: '死信',
  canceled: '已取消',
  paused: '已打断',
}

function fmtDate(s?: string | null): string {
  if (!s) return '—'
  const d = new Date(s)
  if (Number.isNaN(d.getTime())) return s
  return d.toLocaleString('zh-CN', { hour12: false })
}

function duration(a?: string | null, b?: string | null): string {
  if (!a || !b) return '—'
  const ms = new Date(b).getTime() - new Date(a).getTime()
  if (ms < 1000) return `${ms}ms`
  return `${(ms / 1000).toFixed(2)}s`
}

export function useFormat() {
  return { PRIORITIES, STATUS_LABELS, fmtDate, duration }
}

export type { Task }
