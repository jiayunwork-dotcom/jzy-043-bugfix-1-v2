<template>
  <span class="st" :class="status">{{ label }}</span>
</template>

<script setup lang="ts">
import { computed } from 'vue'
const props = defineProps<{ status: string }>()
const labels: Record<string, string> = {
  pending: '等待',
  ready: '就绪',
  running: '执行中',
  succeeded: '成功',
  failed: '失败',
  dead: '死信',
  canceled: '已取消',
  paused: '已打断',
  waiting: '等待',
  skipped: '已跳过',
}
const label = computed(() => labels[props.status] || props.status)
</script>
