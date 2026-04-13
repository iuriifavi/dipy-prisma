import { describe, expect, it } from 'vitest';

import * as publicApi from './index';
import { PrismaThreadStore } from './prisma';

describe('public entrypoint barrel', () => {
  it('re-exports the Prisma thread store', () => {
    expect(publicApi.PrismaThreadStore).toBe(PrismaThreadStore);
  });
});
