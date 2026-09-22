// Thin HTTP client around the engine REST API. Every data point in the panel
// comes through here; nothing is hardcoded in components.

export interface PriorityMetric {
  completed: number
  failed: number
  success_rate: number
  failure_rate: number
}

export interface Metrics {
  taken_at: string
  queue_depths: Record<string, number>
  waiting_delay: number
  waiting_retry: number
  throughput: Array<{
    t: string
    completed: number
    failed: number
    avg_latency_ms: number
  }>
  completions_per_sec: number
  priority_stats: Record<string, PriorityMetric>
  avg_latency_ms: Record<string, number>
  workers: {
    online: number
    offline: number
    total_slots: number
    used_slots: number
    utilization: number
  }
  dlq_count: number
  dlq_growth: Array<{ t: string; count: number }>
}

export interface Task {
  id: string
  type: string
  priority: string
  status: string
  attempts: number
  max_retries: number
  timeout_seconds: number
  payload: unknown
  callback_url?: string
  worker_id?: string
  last_error?: string
  error_category?: string
  dag_id?: string
  dag_node_id?: string
  created_at: string
  updated_at: string
  started_at?: string
  finished_at?: string
}

export interface Attempt {
  attempt_no: number
  worker_id: string
  started_at: string
  ended_at?: string
  status: string
  error?: string
  error_category?: string
}

export interface TaskTimeline {
  task: Task
  attempts: Attempt[]
  audit: Array<Record<string, unknown>>
}

export interface Worker {
  id: string
  name: string
  capabilities: string[]
  total_slots: number
  used_slots: number
  status: string
  online: boolean
  last_heartbeat: string
  current_tasks: string[]
  completed_count: number
  failed_count: number
}

export interface DeadLetter {
  task: Task
  attempts: Attempt[]
  dead_at: string
}

export interface DAGNode {
  id: string
  type: string
  priority: string
  state: string
  task_id: string
  dependencies: string[]
  successors: string[]
  attempts: number
}

export interface DAGDetail {
  dag: {
    id: string
    name: string
    failure_policy: string
    status: string
    created_at: string
    finished_at?: string
    node_count: number
  }
  layers: string[][]
  nodes: DAGNode[]
}

export interface DAGSummary {
  id: string
  name: string
  failure_policy: string
  status: string
  created_at: string
  finished_at?: string
  node_count: number
}

export interface SubmitTaskPayload {
  type: string
  payload: unknown
  priority: string
  idempotency_key?: string
  max_retries?: number
  timeout_seconds?: number
  delay_seconds?: number
  callback_url?: string
  retry_policy?: {
    kind: string
    base_interval_seconds?: number
    max_retries?: number
    cron_expression?: string
  }
}

export interface DAGNodePayload {
  id: string
  type: string
  payload: unknown
  priority: string
  timeout_seconds: number
  max_retries: number
  dependencies: string[]
}

export interface SubmitDAGPayload {
  name: string
  failure_policy: string
  nodes: DAGNodePayload[]
}

function base(): string {
  return useRuntimeConfig().public.apiBase as string
}

async function request<T>(path: string, opts: any = {}): Promise<T> {
  const res = await $fetch.raw<T>(`${base()}${path}`, {
    ...opts,
    headers: { 'Content-Type': 'application/json', ...(opts.headers || {}) },
  })
  return res._data as T
}

export const api = {
  health: () => request<{ status: string }>('/api/health'),
  metrics: () => request<Metrics>('/api/metrics'),

  submitTask: (body: SubmitTaskPayload) =>
    request<{ task: Task; duplicated?: boolean }>('/api/tasks', { method: 'POST', body }),
  listTasks: (params: Record<string, string | number | undefined>) => {
    const q = new URLSearchParams()
    Object.entries(params).forEach(([k, v]) => {
      if (v !== undefined && v !== '' && v !== null) q.set(k, String(v))
    })
    return request<{ tasks: Task[]; total: number }>(`/api/tasks?${q.toString()}`)
  },
  getTask: (id: string) => request<{ task: Task }>(`/api/tasks/${id}`),
  timeline: (id: string) => request<TaskTimeline>(`/api/tasks/${id}/timeline`),
  cancelTask: (id: string) => request(`/api/tasks/${id}/cancel`, { method: 'POST' }),

  listDead: (limit = 100, offset = 0) =>
    request<{ dead: DeadLetter[]; total: number }>(`/api/dead?limit=${limit}&offset=${offset}`),
  aggregateDead: () =>
    request<{ aggregates: Array<{ category: string; count: number }> }>('/api/dead/aggregate'),
  retryDead: (ids: string[]) => request<{ retried: number }>('/api/dead/retry', { method: 'POST', body: { ids } }),
  discardDead: (ids: string[]) => request<{ discarded: number }>('/api/dead/discard', { method: 'POST', body: { ids } }),

  listWorkers: () => request<{ workers: Worker[] }>('/api/workers'),
  getWorker: (id: string) => request<{ worker: Worker; tasks: Task[] }>(`/api/workers/${id}`),
  drainWorker: (id: string) => request(`/api/workers/${id}/drain`, { method: 'POST' }),

  listDAGs: () => request<{ dags: DAGSummary[] }>('/api/dags'),
  getDAG: (id: string) => request<DAGDetail>(`/api/dags/${id}`),
  validateDAG: (body: SubmitDAGPayload) =>
    request<{ valid: boolean; layers?: string[][]; cycle?: string[]; error?: string }>('/api/dags/validate', {
      method: 'POST',
      body,
    }),
  submitDAG: (body: SubmitDAGPayload) =>
    request<{ dag_id: string; status: string }>('/api/dags', { method: 'POST', body }),

  listAudit: (params: Record<string, string> = {}) => {
    const q = new URLSearchParams(params)
    return request<{ audit: Array<Record<string, unknown>> }>(`/api/audit?${q.toString()}`)
  },
}
