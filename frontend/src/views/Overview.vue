<template>
  <section class="page">
    <header class="page-head">
      <div>
        <h2>运营概览</h2>
        <p class="page-desc">汇总各业务模块的关键指标，先看总量再看异常。</p>
      </div>
    </header>
    <div class="stat-row">
      <article v-for="card in cards" :key="card.label" class="stat-card">
        <span class="stat-label">{{ card.label }}</span>
        <strong class="stat-value">{{ card.value }}</strong>
      </article>
    </div>
    <table class="data-table">
      <thead>
        <tr><th>业务模块</th><th>今日新增</th><th>待处理</th><th>异常量</th></tr>
      </thead>
      <tbody>
        <tr v-for="row in moduleRows" :key="row.name">
          <td>{{ row.name }}</td>
          <td>{{ row.created }}</td>
          <td>{{ row.pending }}</td>
          <td>{{ row.abnormal }}</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { fetchJson } from '@/api/client'

type Overview = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
}

const cards = ref<Overview['cards']>([])
const moduleRows = ref<Overview['modules']>([])

onMounted(async () => {
  try {
    const payload = await fetchJson<Overview>('/api/overview')
    cards.value = payload.cards
    moduleRows.value = payload.modules
  } catch {
    cards.value = [{"label": "业务模块", "value": 0}, {"label": "今日新增", "value": 0}]
    moduleRows.value = [{"name": "API端点注册", "created": 0, "pending": 0, "abnormal": 0}, {"name": "Webhook通道", "created": 0, "pending": 0, "abnormal": 0}, {"name": "事件结构定义", "created": 0, "pending": 0, "abnormal": 0}, {"name": "事件结构归属", "created": 0, "pending": 0, "abnormal": 0}, {"name": "接入清单", "created": 0, "pending": 0, "abnormal": 0}, {"name": "待补验记录", "created": 0, "pending": 0, "abnormal": 0}, {"name": "消息队列消费", "created": 0, "pending": 0, "abnormal": 0}, {"name": "死信队列管理", "created": 0, "pending": 0, "abnormal": 0}, {"name": "认证凭证管理", "created": 0, "pending": 0, "abnormal": 0}, {"name": "流量控制", "created": 0, "pending": 0, "abnormal": 0}, {"name": "数据转换编排", "created": 0, "pending": 0, "abnormal": 0}, {"name": "事件分发路由", "created": 0, "pending": 0, "abnormal": 0}, {"name": "事件去重", "created": 0, "pending": 0, "abnormal": 0}, {"name": "批量接入调度", "created": 0, "pending": 0, "abnormal": 0}, {"name": "数据落湖", "created": 0, "pending": 0, "abnormal": 0}, {"name": "接入质量校验", "created": 0, "pending": 0, "abnormal": 0}, {"name": "调用链路追踪", "created": 0, "pending": 0, "abnormal": 0}, {"name": "增量水位管理", "created": 0, "pending": 0, "abnormal": 0}, {"name": "历史回补任务", "created": 0, "pending": 0, "abnormal": 0}, {"name": "事件归档清理", "created": 0, "pending": 0, "abnormal": 0}, {"name": "集成监控看板", "created": 0, "pending": 0, "abnormal": 0}]
  }
})
</script>
