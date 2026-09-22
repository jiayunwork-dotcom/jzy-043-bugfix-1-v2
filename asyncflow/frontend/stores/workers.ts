import { defineStore } from 'pinia'
import { api, type Worker, type Task } from '~/lib/api'

export const useWorkersStore = defineStore('workers', {
  state: () => ({
    workers: [] as Worker[],
    detail: null as { worker: Worker; tasks: Task[] } | null,
    loading: false,
    error: '' as string,
  }),
  actions: {
    async load() {
      this.loading = true
      try {
        const res = await api.listWorkers()
        this.workers = res.workers
        this.error = ''
      } catch (e: any) {
        this.error = e?.message || 'failed to load workers'
      } finally {
        this.loading = false
      }
    },
    async open(id: string) {
      try {
        this.detail = await api.getWorker(id)
      } catch (e: any) {
        this.error = e?.message || 'failed to load worker'
      }
    },
    close() {
      this.detail = null
    },
    async drain(id: string) {
      await api.drainWorker(id)
      await this.load()
    },
  },
})
