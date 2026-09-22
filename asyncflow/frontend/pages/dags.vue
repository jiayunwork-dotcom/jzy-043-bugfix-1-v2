<template>
  <div>
    <h1 class="page-title">DAG 编排</h1>
    <p class="page-sub">按层级查看节点依赖与执行状态；提交前做环检测。依赖顺序与节点状态是核心，不做自由画布。</p>

    <div class="grid cols-2">
      <div class="card">
        <h3>DAG 实例</h3>
        <div v-if="store.loading" class="empty">加载中…</div>
        <div v-else-if="!store.dags.length" class="empty">暂无 DAG</div>
        <table v-else>
          <thead>
            <tr><th>名称</th><th>节点数</th><th>失败策略</th><th>状态</th><th>创建时间</th><th></th></tr>
          </thead>
          <tbody>
            <tr v-for="d in store.dags" :key="d.id">
              <td>{{ d.name }}</td>
              <td>{{ d.node_count }}</td>
              <td>{{ policyLabel(d.failure_policy) }}</td>
              <td><StatusBadge :status="d.status" /></td>
              <td>{{ fmtDate(d.created_at) }}</td>
              <td class="right"><button class="small ghost" @click="store.open(d.id)">查看图</button></td>
            </tr>
          </tbody>
        </table>
      </div>

      <div class="card">
        <h3>定义并提交新的 DAG</h3>
        <div class="form-row">
          <label>DAG 名称</label>
          <input v-model="form.name" placeholder="例如 order-pipeline" />
        </div>
        <div class="form-row">
          <label>节点失败处置</label>
          <select v-model="form.failure_policy">
            <option value="terminate">终止整个 DAG</option>
            <option value="skip">跳过失败节点继续后续</option>
            <option value="retry">重试失败节点</option>
          </select>
        </div>
        <div class="form-row">
          <label>节点定义（JSON：id / type / priority / dependencies / payload …）</label>
          <textarea v-model="form.nodesJson" rows="10" style="width: 100%; font-family: monospace; font-size: 12px"
            :placeholder="exampleJson"></textarea>
        </div>
        <div v-if="store.validationMessage" :class="store.cycleNodes.length ? 'error-box' : 'success-box'">
          {{ store.validationMessage }}
        </div>
        <div class="flex gap">
          <button class="ghost" @click="onValidate">环检测 / 校验</button>
          <button @click="onSubmit">提交 DAG</button>
          <button class="ghost" @click="loadExample">填入示例</button>
        </div>
      </div>
    </div>

    <Teleport to="body">
      <div v-if="store.detail" class="modal-backdrop" @click.self="store.close()">
        <div class="modal">
          <div class="modal-header">
            <div>
              <div style="font-weight: 700; font-size: 16px">
                {{ store.detail.dag.name }}
                <StatusBadge :status="store.detail.dag.status" style="margin-left: 8px" />
              </div>
              <div class="muted" style="font-size: 12px">
                {{ store.detail.dag.id }} · 失败策略：{{ policyLabel(store.detail.dag.failure_policy) }}
              </div>
            </div>
            <button class="ghost small" @click="store.close()">关闭</button>
          </div>
          <div class="modal-body">
            <div v-for="(layer, i) in store.detail.layers" :key="i" class="dag-layer">
              <div class="layer-label">第 {{ i }} 层</div>
              <div
                v-for="nid in layer"
                :key="nid"
                class="dag-node"
                :class="nodeMap[nid]?.state"
              >
                <div class="flex-between">
                  <strong>{{ nid }}</strong>
                  <StatusBadge :status="nodeMap[nid]?.state || 'waiting'" />
                </div>
                <div class="muted" style="font-size: 12px; margin-top: 4px">
                  {{ nodeMap[nid]?.type }} · <PriorityPill :priority="nodeMap[nid]?.priority || 'normal'" />
                </div>
                <div style="font-size: 11px; margin-top: 6px">
                  <span class="muted">前驱: </span>{{ nodeMap[nid]?.dependencies.join(', ') || '∅' }}
                </div>
                <div style="font-size: 11px">
                  <span class="muted">后继: </span>{{ nodeMap[nid]?.successors.join(', ') || '∅' }}
                </div>
                <div v-if="nodeMap[nid]?.task_id" class="mono muted" style="font-size: 10px; margin-top: 4px" :title="nodeMap[nid]?.task_id">
                  task: {{ nodeMap[nid]?.task_id }}
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
import { computed, reactive } from 'vue'
import { useDagsStore } from '~/stores/dags'
import type { DAGNodePayload, SubmitDAGPayload } from '~/lib/api'
import { usePolling } from '~/composables/usePolling'
import { useFormat } from '~/composables/useFormat'

const store = useDagsStore()
const { fmtDate } = useFormat()

const exampleJson = JSON.stringify(
  [
    { id: 'A', type: 'sample.echo', priority: 'high', max_retries: 1, timeout_seconds: 30, dependencies: [], payload: { step: 'start' } },
    { id: 'B', type: 'sample.compute', priority: 'normal', max_retries: 2, timeout_seconds: 30, dependencies: ['A'], payload: {} },
    { id: 'C', type: 'sample.compute', priority: 'normal', max_retries: 2, timeout_seconds: 30, dependencies: ['A'], payload: {} },
    { id: 'D', type: 'sample.flaky', priority: 'low', max_retries: 3, timeout_seconds: 30, dependencies: ['B', 'C'], payload: {} },
  ],
  null,
  2,
)

const form = reactive({
  name: 'sample-pipeline',
  failure_policy: 'terminate',
  nodesJson: '',
})

function loadExample() {
  form.name = 'sample-pipeline'
  form.nodesJson = exampleJson
}
loadExample()

function parseNodes(): DAGNodePayload[] {
  const raw = JSON.parse(form.nodesJson)
  if (!Array.isArray(raw)) throw new Error('节点定义必须是数组')
  return raw.map((n: any) => ({
    id: String(n.id),
    type: String(n.type || 'sample.echo'),
    payload: n.payload ?? {},
    priority: n.priority || 'normal',
    timeout_seconds: Number(n.timeout_seconds || 30),
    max_retries: Number(n.max_retries ?? 2),
    dependencies: Array.isArray(n.dependencies) ? n.dependencies.map(String) : [],
  }))
}

function buildDef(): SubmitDAGPayload {
  return { name: form.name, failure_policy: form.failure_policy, nodes: parseNodes() }
}

async function onValidate() {
  try {
    await store.validate(buildDef())
  } catch (e: any) {
    store.validationMessage = e?.message || 'JSON 解析失败'
  }
}

async function onSubmit() {
  try {
    const def = buildDef()
    const ok = await store.validate(def)
    if (!ok) return
    const id = await store.submit(def)
    await store.open(id)
  } catch (e: any) {
    store.validationMessage = '提交失败: ' + (e?.data?.error || e?.message || 'JSON 解析失败')
  }
}

function policyLabel(p: string) {
  return { terminate: '终止', skip: '跳过', retry: '重试' }[p] || p
}

const nodeMap = computed(() => {
  const m: Record<string, any> = {}
  store.detail?.nodes.forEach((n) => (m[n.id] = n))
  return m
})

usePolling(() => store.load(), 4000)
</script>
