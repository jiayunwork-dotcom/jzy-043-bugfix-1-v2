<template>
  <span class="pill" :class="priority">{{ label }}</span>
</template>

<script setup lang="ts">
import { computed } from 'vue'
const props = defineProps<{ priority: string }>()
const labels: Record<string, string> = {
  critical: 'Critical',
  high: 'High',
  normal: 'Normal',
  low: 'Low',
  bulk: 'Bulk',
}
const label = computed(() => labels[props.priority] || props.priority)
</script>
