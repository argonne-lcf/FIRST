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
      "/catalog": "http://localhost:8000",
      "/control": "http://localhost:8000",
      "/whoami": "http://localhost:8000",
    },
  },
});
