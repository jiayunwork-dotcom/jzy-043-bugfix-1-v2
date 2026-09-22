<template>
  <div class="layout">
    <aside class="sidebar">
      <div class="brand"><span class="dot" />AsyncFlow</div>
      <nav class="nav">
        <NuxtLink to="/">📊 总览大盘</NuxtLink>
        <NuxtLink to="/tasks">📋 任务列表</NuxtLink>
        <NuxtLink to="/dead">🪦 死信管理</NuxtLink>
        <NuxtLink to="/dags">🔀 DAG 编排</NuxtLink>
        <NuxtLink to="/workers">⚙️ Worker 集群</NuxtLink>
      </nav>
    </aside>
    <main class="main">
      <NuxtPage />
    </main>
  </div>
</template>
