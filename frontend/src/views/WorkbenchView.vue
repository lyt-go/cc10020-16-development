<template>
  <section class="page" :data-module="meta.key">
    <header class="page-head">
      <div>
        <h2>{{ meta.name }}</h2>
        <p class="page-desc">{{ meta.desc }}</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">
          登记{{ meta.entity }}
        </button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">
          导出{{ meta.name }}清单
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <p class="status-legend">
      <span v-for="item in statusSummary" :key="item.status" class="legend-item">
        {{ item.status }}：{{ item.count }}
      </span>
    </p>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in meta.fields" :key="column">{{ column }}</th>
          <th>当前状态</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in meta.fields" :key="column">{{ row[column] ?? '—' }}</td>
          <td>{{ row.status }}</td>
          <td class="row-actions">
            <button
              v-for="action in meta.actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
            <button
              v-if="meta.key === 'ingest_manifest'"
              class="link"
              type="button"
              :data-testid="`trace-${row.id}`"
              @click="openTrace(Number(row.id))"
            >
              回查结构
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="meta.fields.length + 2" class="empty-state">
            暂无{{ meta.name }}数据，可先登记{{ meta.entity }}
          </td>
        </tr>
      </tbody>
    </table>

    <section v-if="meta.key === 'event_schema' || meta.key === 'event_schema_ownership'" class="ownership-panel">
      <header class="ownership-head">
        <h3>事件结构归属明细</h3>
        <label class="filter-item">
          <span>按查看通道过滤</span>
          <input v-model="viewerChannel" data-testid="viewer-channel" placeholder="通道ID，如 3" @change="loadOwnership" />
        </label>
      </header>
      <p class="page-desc">
        未列进共享范围的结构只读查看：可以核对归属与字段快照，但不能用于接入放行。
      </p>
      <table class="data-table">
        <thead>
          <tr>
            <th>归属编号</th><th>结构编号</th><th>事件类型</th><th>归属通道</th>
            <th>发布范围</th><th>授权通道</th><th>Schema版本</th><th>使用权</th><th>结论</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in ownershipItems" :key="item.id" :data-testid="`ownership-${item.id}`">
            <td>{{ item['归属编号'] }}</td>
            <td>{{ item['结构编号'] }}</td>
            <td>{{ item['事件类型'] }}</td>
            <td>{{ item['归属通道'] }}</td>
            <td>{{ item['发布范围'] }}</td>
            <td>{{ item['授权通道'] || '—' }}</td>
            <td>{{ item.Schema版本 }}</td>
            <td :class="item.可使用 ? 'access-ok' : 'access-readonly'">
              {{ item.可使用 ? '可使用' : '只读' }}
            </td>
            <td>{{ item.授权结论 }}</td>
          </tr>
          <tr v-if="!ownershipItems.length">
            <td colspan="9" class="empty-state">暂无已发布的结构归属明细</td>
          </tr>
        </tbody>
      </table>
    </section>

    <div v-if="trace" class="modal-backdrop" data-testid="trace-panel">
      <section class="modal trace-modal">
        <h3>接入回查 · {{ trace.conclusion['实际使用结构'] || '未使用归属结构' }}</h3>
        <dl class="dag-detail">
          <template v-for="(value, key) in trace.conclusion" :key="String(key)">
            <dt>{{ key }}</dt>
            <dd>{{ value === '' || value === null ? '—' : value }}</dd>
          </template>
        </dl>
        <p v-if="historicalCheck" class="page-desc">
          原校验结论：{{ historicalCheck['原校验结论'] }}；
          与原结论一致：{{ historicalCheck['与原结论一致'] ? '是' : '否' }}；
          当前授权仍可使用：{{ historicalCheck['当前授权仍可使用'] === false ? '否（授权已变更）' : '是' }}
        </p>
        <h4>待补验记录</h4>
        <ul class="trace-list">
          <li v-for="item in trace.pending_revalidations" :key="String(item.id)">
            {{ item['补验编号'] }} · 缺失字段：{{ item['缺失字段'] || '无' }} · {{ item['status'] }}
          </li>
          <li v-if="!trace.pending_revalidations.length">无</li>
        </ul>
        <h4>正式分区落湖记录</h4>
        <ul class="trace-list">
          <li v-for="(item, index) in trace.lake_records" :key="index">
            {{ item.event_id }} · 归属：{{ (item.attribution as Record<string, unknown>)?.['归属编号'] || '—' }}
          </li>
          <li v-if="!trace.lake_records.length">无</li>
        </ul>
        <div class="page-actions">
          <button class="btn" type="button" @click="trace = null">关闭</button>
          <button class="btn primary" type="button" @click="recheckHistorical">核对原校验结果</button>
        </div>
      </section>
    </div>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记{{ meta.entity }}</h3>
        <label v-for="field in meta.fields" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条{{ meta.name }}记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  createEntry,
  downloadEntries,
  listEntries,
  listOwnership,
  moduleMeta,
  runAction as applyAction,
  traceIntake,
  verifyIntake,
} from '@/api/backend-service'
import type { IntakeTrace, OwnershipItem } from '@/api/backend-service'
import type { EntryRow } from '@/data/types'

const props = defineProps<{ moduleKey: string }>()
const meta = moduleMeta(props.moduleKey)
const rows = ref<EntryRow[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = meta.fields.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const viewerChannel = ref('')
const ownershipItems = ref<OwnershipItem[]>([])
const trace = ref<IntakeTrace | null>(null)
const historicalCheck = ref<Record<string, unknown> | null>(null)
const showsOwnership = meta.key === 'event_schema' || meta.key === 'event_schema_ownership'
const stats = computed(() => [
  { label: meta.metrics[0], value: total.value },
  { label: meta.metrics[1], value: rows.value.filter((row) => row.status === meta.statuses[1]).length },
  {
    label: meta.metrics[2],
    value: rows.value.filter((row) => row.status === meta.statuses[meta.statuses.length - 1]).length,
  },
])
const statusSummary = computed(() =>
  meta.statuses.map((status) => ({
    status,
    count: rows.value.filter((row) => String(row.status) === status).length,
  })),
)

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  try {
    await downloadEntries(meta.key, filters.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(meta.fields.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const result = await createEntry(meta.key, createValues.value)
    if (!result.ok) throw new Error(result.message)
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: EntryRow) {
  errorMessage.value = ''
  try {
    const result = await applyAction(meta.key, Number(row.id), action)
    if (!result.ok) throw new Error(result.message)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await listEntries(meta.key, filters.value)
    rows.value = payload.items
    total.value = payload.total
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '业务列表读取失败'
  }
}

async function loadOwnership() {
  if (!showsOwnership && !viewerChannel.value) return
  errorMessage.value = ''
  try {
    ownershipItems.value = await listOwnership(viewerChannel.value.trim() || undefined)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '结构归属明细读取失败'
  }
}

async function openTrace(manifestId: number) {
  errorMessage.value = ''
  historicalCheck.value = null
  try {
    trace.value = await traceIntake(manifestId)
    historicalCheck.value = await verifyIntake(manifestId)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '接入回查失败'
  }
}

async function recheckHistorical() {
  const manifest = trace.value?.manifest
  if (!manifest) return
  try {
    historicalCheck.value = await verifyIntake(Number(manifest.id))
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '历史校验结果核对失败'
  }
}

onMounted(async () => {
  await reload()
  await loadOwnership()
})
</script>
