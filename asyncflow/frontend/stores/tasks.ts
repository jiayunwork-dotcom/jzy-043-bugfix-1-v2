import { defineStore } from 'pinia'
import { api, type Task, type TaskTimeline } from '~/lib/api'

export interface TaskFilters {
  status: string
  priority: string
  type: string
  from: string
  to: string
  limit: number
  offset: number
}

const emptyFilters = (): TaskFilters => ({
  status: '',
  priority: '',
  type: '',
  from: '',
  to: '',
  limit: 50,
  offset: 0,
})

export const useTasksStore = defineStore('tasks', {
  state: () => ({
    tasks: [] as Task[],
    total: 0,
    filters: emptyFilters(),
    selected: null as TaskTimeline | null,
    loading: false,
    error: '' as string,
  }),
  actions: {
    async load() {
      this.loading = true
      try {
        const params: Record<string, string | number> = {
          limit: this.filters.limit,
          offset: this.filters.offset,
        }
        if (this.filters.status) params.status = this.filters.status
        if (this.filters.priority) params.priority = this.filters.priority
        if (this.filters.type) params.type = this.filters.type
        if (this.filters.from) params.from = this.filters.from
        if (this.filters.to) params.to = this.filters.to
        const res = await api.listTasks(params)
        this.tasks = res.tasks
        this.total = res.total
        this.error = ''
      } catch (e: any) {
        this.error = e?.message || 'failed to load tasks'
      } finally {
        this.loading = false
      }
    },
    async open(id: string) {
      try {
        this.selected = await api.timeline(id)
      } catch (e: any) {
        this.error = e?.message || 'failed to load task'
      }
    },
    close() {
      this.selected = null
    },
    resetFilters() {
      this.filters = emptyFilters()
    },
  },
})
