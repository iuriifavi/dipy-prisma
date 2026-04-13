import path from 'node:path';

import { defineConfig } from 'vitest/config';

export default defineConfig({
  resolve: {
    alias: [
      {
        find: /^dipy$/,
        replacement: path.resolve(__dirname, '../dipy/src/index.ts'),
      },
      {
        find: /^dipy\/store$/,
        replacement: path.resolve(__dirname, '../dipy/src/store/index.ts'),
      },
    ],
  },
  test: {
    environment: 'node',
    globals: true,
    include: ['src/**/*.test.ts'],
  },
});
