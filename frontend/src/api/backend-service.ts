import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

export type OwnershipItem = {
  id: number
  '归属编号': string
  '结构编号': string
  '事件类型': string
  '归属通道': string
  '发布范围': string
  '授权通道': string
  Schema版本: string
  '适用通道版本': string
  可使用: boolean
  只读: boolean
  授权结论: string
  字段快照: { field_name: string; field_type: string; required: boolean; default: unknown }[]
}

export async function listOwnership(channelId?: string): Promise<OwnershipItem[]> {
  const query = channelId ? `?channel_id=${encodeURIComponent(channelId)}` : ''
  const response = await request(`/api/pipeline/schema-ownership${query}`)
  if (!response.ok) throw new Error('结构归属明细读取失败')
  const payload = await response.json()
  return payload.items ?? []
}

export type IntakeTrace = {
  manifest: Record<string, unknown>
  ownership: OwnershipItem | null
  pending_revalidations: Record<string, unknown>[]
  lake_records: Record<string, unknown>[]
  conclusion: Record<string, unknown>
}

export async function traceIntake(manifestId: number): Promise<IntakeTrace> {
  const response = await request(`/api/pipeline/ingest-manifest/${manifestId}/trace`)
  if (!response.ok) throw new Error('接入清单回查失败')
  return await response.json()
}

export async function verifyIntake(manifestId: number): Promise<Record<string, unknown>> {
  const response = await request(`/api/pipeline/ingest-manifest/${manifestId}/verify`)
  if (!response.ok) throw new Error('历史校验结果核对失败')
  return await response.json()
}
