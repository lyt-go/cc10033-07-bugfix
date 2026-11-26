import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type {
  ActionResult,
  ClaimResult,
  EntryRow,
  MissionAttemptList,
  OverviewResult,
  PageResult,
  QueueSnapshot,
} from '@/data/types'

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

// ── 调度计划手工领取 ───────────────────────────────────

async function postJson(path: string, body: Record<string, unknown>): Promise<unknown> {
  const response = await request(path, { method: 'POST', body: JSON.stringify(body) })
  if (!response.ok) throw new Error(`接口返回 ${response.status}，操作未生效`)
  return await response.json()
}

export async function loadSchedulerQueues(): Promise<QueueSnapshot[]> {
  const response = await request('/api/scheduler/queues')
  if (!response.ok) throw new Error('调度待派队列读取失败')
  const payload = (await response.json()) as { items: QueueSnapshot[] }
  return payload.items
}

export async function loadSchedulerQueue(planId: number): Promise<QueueSnapshot> {
  const response = await request(`/api/scheduler/${planId}/queue`)
  if (!response.ok) throw new Error('调度待派队列读取失败')
  return (await response.json()) as QueueSnapshot
}

export async function enqueueMission(planId: number, missionId: number, priority: number): Promise<unknown> {
  return postJson(`/api/scheduler/${planId}/enqueue`, { mission_id: missionId, priority })
}

export async function claimNext(planId: number, clientId?: string): Promise<ClaimResult> {
  return (await postJson(`/api/scheduler/${planId}/claim`, { client_id: clientId ?? null })) as ClaimResult
}

export async function resumeClaim(planId: number, clientId?: string): Promise<ClaimResult> {
  return (await postJson(`/api/scheduler/${planId}/resume`, { client_id: clientId ?? null })) as ClaimResult
}

export async function reportCompletion(
  planId: number,
  missionId: number,
  success: boolean,
  clientId?: string,
): Promise<Record<string, unknown>> {
  return (await postJson(`/api/scheduler/${planId}/report`, {
    mission_id: missionId,
    success,
    client_id: clientId ?? null,
  })) as Record<string, unknown>
}

// ── 采集任务活动尝试 ───────────────────────────────────

export async function loadMissionAttempts(
  missionId: number,
  planId?: number,
): Promise<MissionAttemptList> {
  const query = planId ? `?plan_id=${planId}` : ''
  const response = await request(`/api/crawl_mission/${missionId}/attempts${query}`)
  if (!response.ok) throw new Error('采集任务活动尝试读取失败')
  return (await response.json()) as MissionAttemptList
}
