<template>
  <div class="spark">
    <div
      v-for="(v, i) in normalized"
      :key="i"
      class="col"
      :style="{ height: barHeight(v) + '%' }"
      :title="`${v}`"
    />
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
const props = defineProps<{ values: number[]; minBars?: number }>()

const normalized = computed(() => {
  const vals = props.values.length ? props.values : [0]
  const min = props.minBars || 40
  const padded = [...vals]
  while (padded.length < min) padded.unshift(0)
  return padded.slice(-min)
})

const max = computed(() => Math.max(1, ...normalized.value))
function barHeight(v: number): number {
  return Math.max(4, Math.round((v / max.value) * 100))
}
</script>
