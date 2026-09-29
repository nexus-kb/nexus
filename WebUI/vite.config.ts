import { loadEnv } from "vite";
import { defineConfig } from "vitest/config";
import solid from "vite-plugin-solid";

export default defineConfig(({ mode }) => {
  const apiOrigin =
    loadEnv(mode, ".", "").NEXUS_API_ORIGIN ?? "http://127.0.0.1:8080";

  return {
    plugins: [solid()],
    base: "/",
    publicDir: false,
    server: {
      proxy: {
        "/api": apiOrigin,
      },
    },
    preview: {
      proxy: {
        "/api": apiOrigin,
      },
    },
    build: {
      outDir: "dist",
      assetsDir: "assets",
      emptyOutDir: true,
      sourcemap: false,
    },
    test: {
      environment: "jsdom",
      setupFiles: "./src/test/setup.ts",
      css: true,
      restoreMocks: true,
    },
  };
});
