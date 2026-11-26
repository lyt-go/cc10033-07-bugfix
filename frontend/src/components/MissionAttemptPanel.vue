<template>
  <section class="claim-panel" data-testid="mission-attempt-panel">
    <header class="claim-head">
      <div>
        <h3>采集任务活动尝试</h3>
        <p class="claim-desc">
          每次受理回执对应一条活动尝试，可跳回调度计划核对；回报到达后两侧同时落账并释放可再领容量。
        </p>
      </div>
      <div class="claim-actions">
        <label class="client-field">
          <span>采集任务ID</span>
          <input v-model.number="missionId" data-testid="attempt-mission-id" type="number" min="1" />
        </label>
        <label class="client-field">
          <span>计划ID（可选）</span>
          <input v-model.number="planFilter" data-testid="attempt-plan-id" type="number" min="1" />
        </label>
        <button class="btn primary" type="button" data-testid="attempt-query" @click="loadAttempts">查询尝试</button>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="attempt-error">{{ errorMessage }}</p>

    <template v-if="detail">
      <div class="plan-card">
        <div class="plan-card-head">
          <div>
            <strong>{{ detail.mission_code || `任务 ${detail.mission_id}` }}</strong>
            <span class="plan-state">{{ detail.status }}</span>
          </div>
          <div class="capacity-line">
            <span>活动尝试 {{ detail.total }}</span>
          </div>
        </div>

        <table class="data-table compact">
          <thead>
            <tr>
              <th>受理回执</th><th>调度计划</th><th>优先级</th><th>尝试状态</th>
              <th>执行端</th><th>受理时间</th><th>接管时间</th><th>回报时间</th><th>核对</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="attempt in detail.items" :key="attempt.attempt_id" :data-testid="`attempt-row-${attempt.attempt_id}`">
              <td class="mono">{{ attempt.attempt_id }}</td>
              <td>
                <button class="link" type="button" @click="openPlan(attempt.plan_id)">
                  计划 {{ attempt.plan_id }}
                </button>
              </td>
              <td>{{ attempt.priority }}</td>
              <td>
                <span :class="['attempt-state', `state-${attempt.status}`]">{{ attempt.status }}</span>
              </td>
              <td>{{ attempt.client_id || '—' }}</td>
              <td>{{ formatTime(attempt.dispatched_at) }}</td>
              <td>{{ formatTime(attempt.acknowledged_at) }}</td>
              <td>{{ formatTime(attempt.completed_at) }}</td>
              <td>
                <button class="link" type="button" @click="openPlan(attempt.plan_id)">
                  调度计划核对
                </button>
              </td>
            </tr>
            <tr v-if="!detail.items.length">
              <td colspan="9" class="empty-state">该任务暂无活动尝试</td>
            </tr>
          </tbody>
        </table>
      </div>
    </template>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'

import { loadMissionAttempts } from '@/api/backend-service'
import type { MissionAttemptList } from '@/data/types'

const route = useRoute()
const router = useRouter()
const missionId = ref<number | null>(
  route.query.mission ? Number(route.query.mission) : null,
)
const planFilter = ref<number | null>(
  route.query.plan ? Number(route.query.plan) : null,
)
const detail = ref<MissionAttemptList | null>(null)
const errorMessage = ref('')

function formatTime(ts: number | null | undefined): string {
  if (!ts) return '—'
  return new Date(ts * 1000).toLocaleString('zh-CN', { hour12: false })
}

async function loadAttempts(): Promise<void> {
  errorMessage.value = ''
  if (!missionId.value) {
    errorMessage.value = '请先填写采集任务ID'
    detail.value = null
    return
  }
  try {
    detail.value = await loadMissionAttempts(
      missionId.value,
      planFilter.value ?? undefined,
    )
  } catch (error) {
    detail.value = null
    errorMessage.value = error instanceof Error ? error.message : '活动尝试读取失败'
  }
}

function openPlan(planId: number): void {
  void router.push({ path: '/scheduler', query: { plan: String(planId) } })
}

watch(
  () => route.query,
  (query) => {
    if (query.mission) {
      missionId.value = Number(query.mission)
      planFilter.value = query.plan ? Number(query.plan) : null
      void loadAttempts()
    }
  },
)

onMounted(() => {
  if (missionId.value) void loadAttempts()
})
</script>
