<template>
  <section class="claim-panel" data-testid="scheduler-claim-panel">
    <header class="claim-head">
      <div>
        <h3>手工领取队列</h3>
        <p class="claim-desc">
          先检查并发上限再占槽：满载时队头与选择位置原样保留，只在拿到服务端受理回执后移出待派行。
        </p>
      </div>
      <div class="claim-actions">
        <label class="client-field">
          <span>执行端标识</span>
          <input v-model="clientId" data-testid="claim-client-id" placeholder="如 worker-01" />
        </label>
        <button class="btn" type="button" data-testid="claim-refresh" @click="reload">刷新面板</button>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="claim-error">{{ errorMessage }}</p>
    <p v-if="!loading && !snapshots.length" class="empty-state">暂无调度计划，先登记并启动一个计划。</p>

    <article v-for="snapshot in snapshots" :key="snapshot.plan_id" class="plan-card" :data-testid="`plan-card-${snapshot.plan_id}`">
      <div class="plan-card-head">
        <div>
          <strong>{{ snapshot.plan_code || `计划 ${snapshot.plan_id}` }}</strong>
          <span class="plan-state">{{ snapshot.status }}</span>
        </div>
        <div class="capacity-line">
          <span>并发 {{ snapshot.active }} / {{ snapshot.limit }}</span>
          <span :class="snapshot.available > 0 ? 'cap-ok' : 'cap-full'">
            可再领 {{ snapshot.available }}
          </span>
          <span>待派 {{ snapshot.queue_depth }}</span>
          <span>已受理 {{ snapshot.total_dispatched }}</span>
          <span>失败 {{ snapshot.failed_dispatch }}</span>
        </div>
      </div>

      <div v-if="snapshot.last_deny" class="deny-banner" :data-testid="`deny-${snapshot.plan_id}`">
        <strong>上次领取未受理（可重试）：</strong>{{ snapshot.last_deny.message }}
      </div>

      <div class="claim-grid">
        <div>
          <h4>待派队列（队头在最上）</h4>
          <table class="data-table compact">
            <thead>
              <tr><th>位置</th><th>采集任务</th><th>优先级</th><th>入队时间</th><th>核对</th></tr>
            </thead>
            <tbody>
              <tr
                v-for="item in snapshot.queue"
                :key="`${item.mission_id}-${item.seq}`"
                :class="{ head: item.queue_position === 0 }"
                :data-testid="`queue-row-${snapshot.plan_id}-${item.mission_id}`"
              >
                <td>#{{ item.queue_position + 1 }}</td>
                <td>
                  <button class="link" type="button" @click="openMission(item.mission_id, snapshot.plan_id)">
                    任务 {{ item.mission_id }}
                  </button>
                </td>
                <td>{{ item.priority }}</td>
                <td>{{ formatTime(item.enqueued_at) }}</td>
                <td>
                  <button class="link" type="button" @click="openMission(item.mission_id, snapshot.plan_id)">
                    查看活动尝试
                  </button>
                </td>
              </tr>
              <tr v-if="!snapshot.queue.length">
                <td colspan="5" class="empty-state">待派队列已空</td>
              </tr>
            </tbody>
          </table>

          <form class="enqueue-bar" @submit.prevent="submitEnqueue(snapshot.plan_id)">
            <input
              :value="enqueueInputs[snapshot.plan_id] ?? ''"
              :data-testid="`enqueue-mission-${snapshot.plan_id}`"
              type="number"
              placeholder="采集任务ID"
              required
              @input="enqueueInputs[snapshot.plan_id] = ($event.target as HTMLInputElement).valueAsNumber"
            />
            <input
              :value="enqueuePriority[snapshot.plan_id] ?? 1"
              :data-testid="`enqueue-priority-${snapshot.plan_id}`"
              type="number"
              title="优先级"
              @input="enqueuePriority[snapshot.plan_id] = ($event.target as HTMLInputElement).valueAsNumber"
            />
            <button class="btn" type="button" :data-testid="`claim-${snapshot.plan_id}`" @click="submitClaim(snapshot.plan_id)">
              领取队头
            </button>
            <button class="btn primary" type="submit" :data-testid="`enqueue-submit-${snapshot.plan_id}`">
              加入待派
            </button>
          </form>
        </div>

        <div>
          <h4>在途活动尝试（{{ snapshot.active_attempts.length }}）</h4>
          <table class="data-table compact">
            <thead>
              <tr><th>受理回执</th><th>采集任务</th><th>执行端</th><th>受理时间</th><th>操作</th></tr>
            </thead>
            <tbody>
              <tr v-for="receipt in snapshot.active_attempts" :key="receipt.attempt_id">
                <td class="mono">{{ receipt.attempt_id }}</td>
                <td>
                  <button class="link" type="button" @click="openMission(receipt.mission_id, snapshot.plan_id)">
                    任务 {{ receipt.mission_id }}
                  </button>
                </td>
                <td>{{ receipt.client_id || '—' }}</td>
                <td>{{ formatTime(receipt.dispatched_at) }}</td>
                <td class="row-actions">
                  <button
                    class="link"
                    type="button"
                    :data-testid="`resume-${snapshot.plan_id}-${receipt.mission_id}`"
                    @click="submitResume(snapshot.plan_id)"
                  >
                    中断续领
                  </button>
                  <button
                    class="link ok"
                    type="button"
                    :data-testid="`report-ok-${snapshot.plan_id}-${receipt.mission_id}`"
                    @click="submitReport(snapshot.plan_id, receipt.mission_id, true)"
                  >
                    成功回报
                  </button>
                  <button
                    class="link fail"
                    type="button"
                    :data-testid="`report-fail-${snapshot.plan_id}-${receipt.mission_id}`"
                    @click="submitReport(snapshot.plan_id, receipt.mission_id, false)"
                  >
                    失败回报
                  </button>
                </td>
              </tr>
              <tr v-if="!snapshot.active_attempts.length">
                <td colspan="5" class="empty-state">暂无在途尝试</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      <details class="history-box">
        <summary>受理历史（{{ snapshot.history.length }}，与采集任务活动尝试核对）</summary>
        <table class="data-table compact">
          <thead>
            <tr><th>回执</th><th>任务</th><th>状态</th><th>受理时间</th><th>回报时间</th></tr>
          </thead>
          <tbody>
            <tr v-for="receipt in snapshot.history" :key="receipt.attempt_id">
              <td class="mono">{{ receipt.attempt_id }}</td>
              <td>
                <button class="link" type="button" @click="openMission(receipt.mission_id, snapshot.plan_id)">
                  任务 {{ receipt.mission_id }}
                </button>
              </td>
              <td>{{ receipt.status }}</td>
              <td>{{ formatTime(receipt.dispatched_at) }}</td>
              <td>{{ receipt.completed_at ? formatTime(receipt.completed_at) : '—' }}</td>
            </tr>
          </tbody>
        </table>
      </details>
    </article>
  </section>
</template>

<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'
import { useRouter } from 'vue-router'

import {
  claimNext,
  enqueueMission,
  loadSchedulerQueues,
  reportCompletion,
  resumeClaim,
} from '@/api/backend-service'
import type { ClaimResult, QueueSnapshot } from '@/data/types'

const emit = defineEmits<{ (event: 'notice', message: string, kind: 'ok' | 'err'): void }>()

const router = useRouter()
const snapshots = ref<QueueSnapshot[]>([])
const loading = ref(false)
const errorMessage = ref('')
const clientId = ref('')
const enqueueInputs = reactive<Record<number, number | null>>({})
const enqueuePriority = reactive<Record<number, number>>({})

function formatTime(ts: number | null | undefined): string {
  if (!ts) return '—'
  return new Date(ts * 1000).toLocaleString('zh-CN', { hour12: false })
}

async function reload(): Promise<void> {
  loading.value = true
  errorMessage.value = ''
  try {
    snapshots.value = await loadSchedulerQueues()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '调度待派队列读取失败'
  } finally {
    loading.value = false
  }
}

function describe(result: ClaimResult): string {
  if (result.accepted) {
    if (result.qualification === 'continue') {
      return `已续上原尝试 ${result.attempt_id}（任务 ${result.mission_id}），未重复占槽`
    }
    return `领取受理成功：任务 ${result.mission_id}，回执 ${result.attempt_id}，剩余可领 ${result.available ?? 0}`
  }
  return `领取未受理：${result.message}（原因 ${result.reason_code}，${result.retryable ? '可重试' : '不可重试'}）`
}

async function submitClaim(planId: number): Promise<void> {
  try {
    const result = await claimNext(planId, clientId.value || undefined)
    emit('notice', describe(result), result.accepted ? 'ok' : 'err')
    await reload()
  } catch (error) {
    emit('notice', error instanceof Error ? error.message : '领取请求失败', 'err')
  }
}

async function submitResume(planId: number): Promise<void> {
  try {
    const result = await resumeClaim(planId, clientId.value || undefined)
    emit('notice', describe(result), result.accepted ? 'ok' : 'err')
    await reload()
  } catch (error) {
    emit('notice', error instanceof Error ? error.message : '中断续领请求失败', 'err')
  }
}

async function submitReport(planId: number, missionId: number, success: boolean): Promise<void> {
  try {
    await reportCompletion(planId, missionId, success, clientId.value || undefined)
    emit('notice', `任务 ${missionId} ${success ? '成功' : '失败'}回报已落账，两侧容量已同步`, 'ok')
    await reload()
  } catch (error) {
    emit('notice', error instanceof Error ? error.message : '回报请求失败', 'err')
  }
}

async function submitEnqueue(planId: number): Promise<void> {
  const missionId = enqueueInputs[planId]
  if (!missionId) return
  try {
    await enqueueMission(planId, missionId, enqueuePriority[planId] ?? 1)
    enqueueInputs[planId] = null
    emit('notice', `任务 ${missionId} 已加入计划 ${planId} 的待派队列`, 'ok')
    await reload()
  } catch (error) {
    emit('notice', error instanceof Error ? error.message : '入队请求失败', 'err')
  }
}

function openMission(missionId: number, planId: number): void {
  void router.push({ path: '/crawl_mission', query: { mission: String(missionId), plan: String(planId) } })
}

onMounted(reload)
defineExpose({ reload })
</script>
