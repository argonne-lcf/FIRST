import { defineConfig } from "vite";
import path from "path";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

export default defineConfig({
  base: "/admin-console",
  plugins: [react(), tailwindcss()],
  resolve: { alias: { "@": path.resolve(import.meta.dirname, "./src") } },
  server: {
    port: 4040,
    strictPort: true,
    proxy: {
      // The admin API: controller-manager's loopback-only control server.
      "/catalog": "http://localhost:9100",
      "/control": "http://localhost:9100",
      "/whoami": "http://localhost:9100",
    },
  },
});
