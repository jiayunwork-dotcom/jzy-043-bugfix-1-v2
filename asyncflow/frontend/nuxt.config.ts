// https://nuxt.com/docs/api/configuration/nuxt-config
export default defineNuxtConfig({
  compatibilityDate: '2024-11-01',
  ssr: false, // browser-only management panel; all data fetched from the API
  modules: ['@pinia/nuxt'],
  devtools: { enabled: false },
  runtimeConfig: {
    public: {
      // Browser-facing base URL of the engine API. Override in deployment:
      // NUXT_PUBLIC_API_BASE=http://host:8080
      apiBase: process.env.NUXT_PUBLIC_API_BASE || 'http://localhost:8080',
    },
  },
  app: {
    head: {
      title: 'AsyncFlow · 异步任务引擎',
      meta: [
        { charset: 'utf-8' },
        { name: 'viewport', content: 'width=device-width, initial-scale=1' },
      ],
      link: [{ rel: 'icon', type: 'image/svg+xml', href: '/favicon.svg' }],
    },
  },
  css: ['~/assets/css/main.css'],
})
