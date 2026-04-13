import type { DipySnapshot } from 'dipy/store';
import { describe, expect, it } from 'vitest';

import { PrismaThreadStore } from './prisma';

function createDelegate(rows: Array<Record<string, unknown>>) {
  return {
    async create(args: { data: Record<string, unknown> }) {
      rows.push({ ...args.data });
      return { ...args.data };
    },
    async deleteMany(args: { where?: Record<string, unknown> }) {
      if (!args.where) return;
      for (let index = rows.length - 1; index >= 0; index -= 1) {
        const row = rows[index]!;
        const matches = Object.entries(args.where).every(([key, value]) => row[key] === value);
        if (matches) {
          rows.splice(index, 1);
        }
      }
    },
    async findFirst(args: {
      where?: Record<string, unknown>;
      orderBy?: Record<string, 'asc' | 'desc'>;
    }) {
      const found = await this.findMany(args);
      return found[0] ?? null;
    },
    async findMany(args: {
      where?: Record<string, unknown>;
      orderBy?: Record<string, 'asc' | 'desc'>;
    }) {
      const filtered = rows.filter((row) => {
        if (!args.where) return true;
        return Object.entries(args.where).every(([key, value]) => {
          if (value && typeof value === 'object' && 'gt' in (value as Record<string, unknown>)) {
            return (
              new Date(String(row[key])).getTime() >
              new Date(String((value as Record<string, unknown>)['gt'])).getTime()
            );
          }
          return row[key] === value;
        });
      });
      const [orderKey, orderDirection] = Object.entries(args.orderBy ?? {})[0] ?? [];
      if (!orderKey) {
        return filtered.map((row) => ({ ...row }));
      }
      return filtered
        .slice()
        .sort((left, right) => {
          const leftValue = left[orderKey];
          const rightValue = right[orderKey];
          let comparison: number;
          if (leftValue instanceof Date || rightValue instanceof Date) {
            comparison =
              new Date(leftValue as string | Date).getTime() -
              new Date(rightValue as string | Date).getTime();
          } else {
            comparison = String(leftValue).localeCompare(String(rightValue));
          }
          return orderDirection === 'desc' ? -comparison : comparison;
        })
        .map((row) => ({ ...row }));
    },
    async findUnique(args: { where: Record<string, unknown> }) {
      return (
        rows.find((row) =>
          Object.entries(args.where).every(([key, value]) => row[key] === value),
        ) ?? null
      );
    },
    async update(args: { where: Record<string, unknown>; data: Record<string, unknown> }) {
      const row = rows.find((entry) =>
        Object.entries(args.where).every(([key, value]) => entry[key] === value),
      );
      if (!row) {
        throw new Error('Row not found');
      }
      Object.assign(row, args.data);
      return { ...row };
    },
    async upsert(args: {
      create: Record<string, unknown>;
      update: Record<string, unknown>;
      where: Record<string, unknown>;
    }) {
      const existing = await this.findUnique({ where: flattenWhere(args.where) });
      if (existing) {
        Object.assign(existing, args.update);
        return { ...existing };
      }
      rows.push({ ...args.create });
      return { ...args.create };
    },
  };
}

function flattenWhere(where: Record<string, unknown>) {
  if ('runId_stepKey' in where && typeof where['runId_stepKey'] === 'object') {
    return where['runId_stepKey'] as Record<string, unknown>;
  }
  return where;
}

describe('PrismaThreadStore', () => {
  it('persists threads, messages, snapshots, events, and memoized results', async () => {
    const threadRows: Array<Record<string, unknown>> = [];
    const messageRows: Array<Record<string, unknown>> = [];
    const runRows: Array<Record<string, unknown>> = [];
    const runEventRows: Array<Record<string, unknown>> = [];
    const runSnapshotRows: Array<Record<string, unknown>> = [];
    const memoizedRows: Array<Record<string, unknown>> = [];

    const store = new PrismaThreadStore<{ id: string; role: string; parts?: unknown[] }>({
      prisma: {
        memoizedResult: createDelegate(memoizedRows),
        message: createDelegate(messageRows),
        run: createDelegate(runRows),
        runEvent: createDelegate(runEventRows),
        runSnapshot: createDelegate(runSnapshotRows),
        thread: createDelegate(threadRows),
      },
    });

    await store.createThread({
      projectId: 'proj-123',
      metadata: { reviewers: new Set(['alice']), topic: 'research' },
      threadId: 'thread-1',
    });
    expect(await store.getThread('thread-1')).toMatchObject({
      metadata: { reviewers: new Set(['alice']), topic: 'research' },
      projectId: 'proj-123',
      threadId: 'thread-1',
    });

    await store.saveMessages('thread-1', [
      { id: 'm1', role: 'user', parts: [{ submittedAt: new Date('2026-03-21T10:00:00.000Z') }] },
      { id: 'm2', role: 'assistant', parts: [{ tags: new Set(['ready']) }] },
    ]);
    expect(await store.loadMessages('thread-1')).toEqual([
      { id: 'm1', parts: [{ submittedAt: new Date('2026-03-21T10:00:00.000Z') }], role: 'user' },
      { id: 'm2', parts: [{ tags: new Set(['ready']) }], role: 'assistant' },
    ]);

    const snapshot: DipySnapshot = {
      agentStates: { root: { reviewedAt: new Date('2026-03-21T12:10:00.000Z'), step: 1 } },
      createdAt: new Date('2026-03-21T12:00:00.000Z'),
      delegationStack: { lineage: ['root'], reviewers: new Set(['alice']) },
      middlewareState: { audit: { enabled: true, totals: new Map([['steps', 1]]) } },
      nodeTree: { id: 'root', lastCompactedAt: new Date('2026-03-21T12:01:00.000Z') },
      reason: 'periodic',
      runId: 'run-1',
      stepNumber: 1,
      threadId: 'thread-1',
    };
    await store.saveSnapshot('run-1', snapshot);
    expect(await store.loadSnapshot('run-1')).toMatchObject({
      runId: 'run-1',
      threadId: 'thread-1',
    });

    await store.appendEvents('run-1', [
      {
        data: { startedAt: new Date('2026-03-21T12:00:10.000Z'), step: 1 },
        timestamp: 10,
        type: 'agent:mount',
      },
      { data: { labels: new Set(['analysis']), step: 2 }, timestamp: 20, type: 'agent:step' },
    ]);
    expect(await store.loadEvents('run-1')).toEqual([
      {
        data: { startedAt: new Date('2026-03-21T12:00:10.000Z'), step: 1 },
        timestamp: 10,
        type: 'agent:mount',
      },
      { data: { labels: new Set(['analysis']), step: 2 }, timestamp: 20, type: 'agent:step' },
    ]);

    await store.saveMemoizedResult('run-1', 'step-1', {
      completedAt: new Date('2026-03-21T12:00:20.000Z'),
      ok: true,
    });
    const memoized = await store.loadMemoizedResults('run-1');
    expect(memoized.get('step-1')).toEqual({
      completedAt: new Date('2026-03-21T12:00:20.000Z'),
      ok: true,
    });
  });
});
