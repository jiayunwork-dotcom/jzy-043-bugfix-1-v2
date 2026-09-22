import { defineStore } from 'pinia'
import { api, type DAGSummary, type DAGDetail, type SubmitDAGPayload } from '~/lib/api'

export const useDagsStore = defineStore('dags', {
  state: () => ({
    dags: [] as DAGSummary[],
    detail: null as DAGDetail | null,
    loading: false,
    error: '' as string,
    validationMessage: '' as string,
    cycleNodes: [] as string[],
  }),
  actions: {
    async load() {
      this.loading = true
      try {
        const res = await api.listDAGs()
        this.dags = res.dags
        this.error = ''
      } catch (e: any) {
        this.error = e?.message || 'failed to load DAGs'
      } finally {
        this.loading = false
      }
    },
    async open(id: string) {
      try {
        this.detail = await api.getDAG(id)
      } catch (e: any) {
        this.error = e?.message || 'failed to load DAG'
      }
    },
    close() {
      this.detail = null
    },
    async validate(def: SubmitDAGPayload) {
      this.validationMessage = ''
      this.cycleNodes = []
      try {
        const res = await api.validateDAG(def)
        if (res.valid) {
          this.validationMessage = 'DAG 定义有效，无环，可提交。'
          return true
        }
        this.validationMessage = res.error || '无效定义'
        return false
      } catch (e: any) {
        const data = e?.data
        if (data?.cycle) {
          this.cycleNodes = data.cycle
          this.validationMessage = `检测到环: ${data.cycle.join(' -> ')}`
        } else {
          this.validationMessage = data?.error || e?.message || '校验失败'
        }
        return false
      }
    },
    async submit(def: SubmitDAGPayload) {
      const res = await api.submitDAG(def)
      await this.load()
      return res.dag_id
    },
  },
})
