import type { Persistable } from 'dipy';
import { deserializePersisted, serializePersisted } from 'dipy/store';
import type { DipyEventRecord, DipySnapshot, DipyThread, SnapshotMeta, ThreadStore } from 'dipy/store';

export interface PrismaLikeDelegate {
  create(args: Record<string, unknown>): Promise<Record<string, unknown>>;
  deleteMany?(args: Record<string, unknown>): Promise<unknown>;
  findFirst?(args: Record<string, unknown>): Promise<Record<string, unknown> | null>;
  findMany(args: Record<string, unknown>): Promise<Array<Record<string, unknown>>>;
  findUnique?(args: Record<string, unknown>): Promise<Record<string, unknown> | null>;
  update?(args: Record<string, unknown>): Promise<Record<string, unknown>>;
  updateMany?(args: Record<string, unknown>): Promise<unknown>;
  upsert?(args: Record<string, unknown>): Promise<Record<string, unknown>>;
}

export interface PrismaLikeClient {
  memoizedResult: PrismaLikeDelegate;
  message: PrismaLikeDelegate;
  run: PrismaLikeDelegate;
  runEvent: PrismaLikeDelegate;
  runSnapshot: PrismaLikeDelegate;
  thread: PrismaLikeDelegate;
}

export interface PrismaThreadStoreOptions {
  prisma: PrismaLikeClient;
}

function randomUUID(options?: { disableEntropyCache?: boolean }): string {
  const cryptoImpl = globalThis.crypto;
  if (cryptoImpl && typeof cryptoImpl.randomUUID === 'function') {
    void options;
    return cryptoImpl.randomUUID();
  }

  if (!cryptoImpl || typeof cryptoImpl.getRandomValues !== 'function') {
    throw new Error('randomUUID requires globalThis.crypto.randomUUID or getRandomValues support');
  }

  const bytes = cryptoImpl.getRandomValues(new Uint8Array(16));
  bytes[6] = (bytes[6]! & 0x0f) | 0x40;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  const hex = Array.from(bytes, (value: number) => value.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function asDate(value: unknown): Date {
  return value instanceof Date ? value : new Date(String(value));
}

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' ? (value as Record<string, unknown>) : {};
}

export class PrismaThreadStore<
  UIMessage extends Persistable = Persistable,
> implements ThreadStore<UIMessage> {
  constructor(private readonly options: PrismaThreadStoreOptions) {}

  private get prisma() {
    return this.options.prisma;
  }

  async createThread(opts: {
    projectId: string;
    metadata?: Persistable;
    threadId: string;
  }): Promise<DipyThread> {
    const created = await this.prisma.thread.create({
      data: {
        projectId: opts.projectId,
        id: opts.threadId,
        ...(opts.metadata ? { metadata: serializePersisted(opts.metadata) } : {}),
      },
    });
    return {
      projectId: String(created['projectId']),
      createdAt: asDate(created['createdAt'] ?? new Date()),
      ...(created['metadata'] !== undefined
        ? { metadata: deserializePersisted<Persistable>(created['metadata']) }
        : {}),
      threadId: String(created['id']),
      updatedAt: asDate(created['updatedAt'] ?? new Date()),
    };
  }

  async getThread(threadId: string, opts?: { projectId?: string }): Promise<DipyThread | null> {
    let thread: Record<string, unknown> | null;
    if (opts?.projectId) {
      const where: Record<string, unknown> = { id: threadId, projectId: opts.projectId };
      thread = this.prisma.thread.findFirst
        ? await this.prisma.thread.findFirst({ where })
        : this.prisma.thread.findUnique
          ? await this.prisma.thread.findUnique({ where })
          : null;
    } else {
      thread = this.prisma.thread.findUnique
        ? await this.prisma.thread.findUnique({ where: { id: threadId } })
        : ((await this.prisma.thread.findFirst?.({ where: { id: threadId } })) ?? null);
    }
    if (!thread || thread['deletedAt'] != null) {
      return null;
    }
    return {
      projectId: String(thread['projectId']),
      createdAt: asDate(thread['createdAt'] ?? new Date()),
      ...(thread['metadata'] !== undefined
        ? { metadata: deserializePersisted<Persistable>(thread['metadata']) }
        : {}),
      threadId: String(thread['id']),
      updatedAt: asDate(thread['updatedAt'] ?? new Date()),
    };
  }

  async listThreads(projectId: string): Promise<DipyThread[]> {
    const rows = await this.prisma.thread.findMany({
      orderBy: { updatedAt: 'desc' },
      where: { projectId, deletedAt: null },
    });
    return rows.map((thread) => ({
      projectId: String(thread['projectId']),
      createdAt: asDate(thread['createdAt'] ?? new Date()),
      ...(thread['metadata'] !== undefined
        ? { metadata: deserializePersisted<Persistable>(thread['metadata']) }
        : {}),
      threadId: String(thread['id']),
      updatedAt: asDate(thread['updatedAt'] ?? new Date()),
    }));
  }

  async deleteThread(threadId: string, opts?: { projectId?: string }): Promise<void> {
    const where: Record<string, unknown> = { id: threadId };
    if (opts?.projectId) {
      where['projectId'] = opts.projectId;
    }

    if (this.prisma.thread.update) {
      await this.prisma.thread.update({
        data: { deletedAt: new Date() },
        where,
      });
      return;
    }

    await this.prisma.thread.deleteMany?.({ where });
  }

  async saveMessages(
    threadId: string,
    messages: UIMessage[],
    opts?: { projectId?: string },
  ): Promise<void> {
    if (opts?.projectId) {
      const thread = await this.getThread(threadId, { projectId: opts.projectId });
      if (!thread) {
        throw new Error(
          `Thread "${threadId}" not found or does not belong to project "${opts.projectId}"`,
        );
      }
    }
    for (const message of messages) {
      const record = asRecord(message);
      const id = typeof record['id'] === 'string' ? record['id'] : `${threadId}-${randomUUID()}`;
      const createPayload = {
        createdAt: new Date(),
        id,
        parts: serializePersisted(record['parts'] ?? []),
        role: String(record['role'] ?? 'assistant'),
        threadId,
      };
      const updatePayload = {
        id,
        parts: serializePersisted(record['parts'] ?? []),
        role: String(record['role'] ?? 'assistant'),
        threadId,
      };

      if (this.prisma.message.upsert) {
        await this.prisma.message.upsert({
          create: createPayload,
          update: updatePayload,
          where: { id },
        });
        continue;
      }

      try {
        await this.prisma.message.create({ data: createPayload });
      } catch (error) {
        const isPrismaUniqueViolation =
          error &&
          typeof error === 'object' &&
          'code' in error &&
          (error as { code: string }).code === 'P2002';
        if (isPrismaUniqueViolation) {
          await this.prisma.message.updateMany?.({ where: { id }, data: updatePayload });
        } else {
          throw error;
        }
      }
    }
  }

  async loadMessages(threadId: string, opts?: { projectId?: string }): Promise<UIMessage[]> {
    if (opts?.projectId) {
      const thread = await this.getThread(threadId, { projectId: opts.projectId });
      if (!thread) {
        return [];
      }
    }
    const rows = await this.prisma.message.findMany({
      orderBy: { createdAt: 'asc' },
      where: { threadId },
    });
    return rows.map((row) => ({
      id: row['id'],
      parts: deserializePersisted(row['parts']),
      role: row['role'],
    })) as UIMessage[];
  }

  async deleteMessage(messageId: string, opts?: { projectId?: string }): Promise<void> {
    const where: Record<string, unknown> = { id: messageId };
    if (opts?.projectId) {
      where['id'] = messageId;
    }
    await this.prisma.message.deleteMany?.({ where });
  }

  async saveSnapshot(runId: string, snapshot: DipySnapshot): Promise<void> {
    if (snapshot.runId && snapshot.runId !== runId) {
      throw new Error(
        `saveSnapshot: runId mismatch — argument "${runId}" vs snapshot.runId "${snapshot.runId}"`,
      );
    }
    const data = {
      createdAt: snapshot.createdAt,
      data: serializePersisted(snapshot),
      reason: snapshot.reason,
      runId,
      step: snapshot.stepNumber,
    };

    await this.prisma.runSnapshot.create({ data });
  }

  async loadSnapshot(runId: string): Promise<DipySnapshot | null> {
    const row = await (this.prisma.runSnapshot.findFirst
      ? this.prisma.runSnapshot.findFirst({
          orderBy: { createdAt: 'desc' },
          where: { runId },
        })
      : this.prisma.runSnapshot
          .findMany({
            orderBy: { createdAt: 'desc' },
            where: { runId },
            take: 1,
          })
          .then((results) => results[0] ?? null));
    if (!row) {
      return null;
    }
    let data: DipySnapshot;
    try {
      data = deserializePersisted<DipySnapshot>(row['data']);
    } catch (error) {
      console.error('Failed to deserialize DipySnapshot for row:', row['id'], error);
      return null;
    }
    return {
      ...data,
      createdAt: asDate(data.createdAt ?? row['createdAt'] ?? new Date()),
    };
  }

  async listSnapshots(runId: string): Promise<SnapshotMeta[]> {
    const rows = await this.prisma.runSnapshot.findMany({
      orderBy: { createdAt: 'asc' },
      where: { runId },
    });
    return rows.map((row) => ({
      createdAt: asDate(row['createdAt'] ?? new Date()),
      reason: String(row['reason']) as DipySnapshot['reason'],
      stepNumber: Number(row['step'] ?? 0),
    }));
  }

  async appendEvents(runId: string, events: DipyEventRecord[]): Promise<void> {
    await Promise.all(
      events.map((event) =>
        this.prisma.runEvent.create({
          data: {
            data: serializePersisted(event.data),
            runId,
            timestamp: new Date(event.timestamp),
            type: event.type,
          },
        }),
      ),
    );
  }

  async loadEvents(
    runId: string,
    opts?: { after?: { id: string; timestamp: number } },
  ): Promise<DipyEventRecord[]> {
    const rows = await this.prisma.runEvent.findMany({
      orderBy: { timestamp: 'asc' },
      where: {
        runId,
        ...(opts?.after ? { timestamp: { gt: new Date(opts.after.timestamp) } } : {}),
      },
    });
    return rows.map((row) => ({
      data: deserializePersisted<Persistable>(row['data']),
      timestamp: asDate(row['timestamp'] ?? new Date()).getTime(),
      type: String(row['type']),
    }));
  }

  async saveMemoizedResult(runId: string, stepKey: string, result: Persistable): Promise<void> {
    const data = {
      result: serializePersisted(result),
      runId,
      stepKey,
    };

    if (this.prisma.memoizedResult.upsert) {
      await this.prisma.memoizedResult.upsert({
        create: data,
        update: data,
        where: { runId_stepKey: { runId, stepKey } },
      });
      return;
    }

    try {
      await this.prisma.memoizedResult.create({ data });
    } catch (error) {
      const isPrismaUniqueViolation =
        error &&
        typeof error === 'object' &&
        'code' in error &&
        (error as { code: string }).code === 'P2002';
      if (isPrismaUniqueViolation) {
        await this.prisma.memoizedResult.updateMany?.({
          where: { runId, stepKey },
          data: { result: serializePersisted(result) },
        });
      } else {
        throw error;
      }
    }
  }

  async loadMemoizedResults(runId: string): Promise<Map<string, Persistable>> {
    const rows = await this.prisma.memoizedResult.findMany({
      where: { runId },
    });
    return new Map(
      rows.map((row) => [String(row['stepKey']), deserializePersisted<Persistable>(row['result'])]),
    );
  }
}
