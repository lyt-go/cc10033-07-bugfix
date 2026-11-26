/** 纯前端数据层的公共类型：与全栈版后端返回的结构保持一致，换回后端时页面不用改。 */

export type EntryRow = {
  id: number
  status: string
  pending: boolean
  abnormal: boolean
  [field: string]: string | number | boolean
}

export type ModuleMeta = {
  key: string
  name: string
  entity: string
  desc: string
  fields: string[]
  statuses: string[]
  actions: string[]
  actionTargets: Record<string, string>
  metrics: string[]
}

export type PageResult = {
  items: EntryRow[]
  total: number
  page: number
  size: number
}

export type ActionResult = {
  ok: boolean
  message: string
}

export type OverviewResult = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
}

// ── 调度计划手工领取面板 ───────────────────────────────

export type QueueItem = {
  mission_id: number
  priority: number
  enqueued_at: number
  seq: number | null
  queue_position: number
}

export type AttemptReceipt = {
  attempt_id: string
  mission_id: number
  plan_id: number
  priority: number
  status: string
  client_id: string | null
  dispatched_at: number
  completed_at: number | null
}

export type QueueSnapshot = {
  plan_id: number
  plan_code: string
  status: string
  limit: number
  active: number
  available: number
  queue_depth: number
  total_dispatched: number
  failed_dispatch: number
  queue: QueueItem[]
  active_attempts: AttemptReceipt[]
  history: AttemptReceipt[]
  last_deny: ClaimResult | null
}

export type ClaimResult = {
  accepted: boolean
  reason_code?: string
  message: string
  retryable?: boolean
  qualification?: 'new_claim' | 'continue' | 'not_qualified'
  resumed?: boolean
  attempt_id?: string
  mission_id?: number
  plan_id?: number
  priority?: number
  status?: string
  client_id?: string | null
  dispatched_at?: number
  completed_at?: number | null
  active?: number
  limit?: number
  available?: number
  head?: QueueItem | null
}

export type MissionAttempt = {
  attempt_id: string
  plan_id: number
  mission_id: number
  priority: number
  status: string
  client_id: string | null
  dispatched_at: number
  completed_at: number | null
  acknowledged_at: number | null
}

export type MissionAttemptList = {
  items: MissionAttempt[]
  total: number
  mission_id: number
  mission_code: string
  status: string
}
