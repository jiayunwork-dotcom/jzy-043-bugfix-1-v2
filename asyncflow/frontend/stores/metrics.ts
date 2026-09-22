import { defineStore } from 'pinia'
import { api, type Metrics } from '~/lib/api'

// Overview/dashboard metrics store, refreshed on a 2s poll.
export const useMetricsStore = defineStore('metrics', {
  state: () => ({
    data: null as Metrics | null,
    loading: false,
    error: '' as string,
  }),
  actions: {
    async load() {
      this.loading = true
      try {
        this.data = await api.metrics()
        this.error = ''
      } catch (e: any) {
        this.error = e?.message || 'metrics unavailable'
      } finally {
        this.loading = false
      }
    },
    depth(priority: string): number {
      return this.data?.queue_depths?.[priority] ?? 0
    },
  },
})
