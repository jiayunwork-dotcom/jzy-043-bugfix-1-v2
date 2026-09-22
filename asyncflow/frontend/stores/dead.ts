import { defineStore } from 'pinia'
import { api, type DeadLetter } from '~/lib/api'

export const useDeadStore = defineStore('dead', {
  state: () => ({
    dead: [] as DeadLetter[],
    total: 0,
    aggregates: [] as Array<{ category: string; count: number }>,
    selected: new Set<string>(),
    detail: null as DeadLetter | null,
    loading: false,
    error: '' as string,
  }),
  actions: {
    async load() {
      this.loading = true
      try {
        const [list, agg] = await Promise.all([api.listDead(200, 0), api.aggregateDead()])
        this.dead = list.dead
        this.total = list.total
        this.aggregates = agg.aggregates
        this.error = ''
      } catch (e: any) {
        this.error = e?.message || 'failed to load dead letters'
      } finally {
        this.loading = false
      }
    },
    toggle(id: string) {
      if (this.selected.has(id)) this.selected.delete(id)
      else this.selected.add(id)
    },
    selectAll() {
      this.dead.forEach((d) => this.selected.add(d.task.id))
    },
    clearSelection() {
      this.selected.clear()
    },
    async retry(ids: string[]) {
      await api.retryDead(ids)
      this.selected.clear()
      await this.load()
    },
    async discard(ids: string[]) {
      await api.discardDead(ids)
      this.selected.clear()
      await this.load()
    },
    open(d: DeadLetter) {
      this.detail = d
    },
    close() {
      this.detail = null
    },
  },
})
