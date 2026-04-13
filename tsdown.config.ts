import { defineConfig } from 'tsdown';

export default defineConfig({
  clean: true,
  dts: false,
  entry: {
    index: 'src/index.ts',
  },
  format: ['esm', 'cjs'],
  platform: 'node',
  sourcemap: true,
});
