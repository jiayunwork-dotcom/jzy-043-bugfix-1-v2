// Polling helper: invokes loader immediately and then on an interval; stops
// automatically when the component unmounts.
import { onBeforeUnmount, onMounted } from 'vue'

export function usePolling(loader: () => unknown | Promise<unknown>, intervalMs = 2000) {
  let timer: ReturnType<typeof setInterval> | null = null
  let stopped = false

  async function tick() {
    if (stopped) return
    try {
      await loader()
    } catch (e) {
      // Surface nothing noisy; components read store error flags.
    }
  }

  function start() {
    if (timer) return
    tick()
    timer = setInterval(tick, intervalMs)
  }

  function stop() {
    stopped = true
    if (timer) {
      clearInterval(timer)
      timer = null
    }
  }

  function refresh() {
    return tick()
  }

  onMounted(start)
  onBeforeUnmount(stop)

  return { start, stop, refresh }
}
