import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  base: "/admin-console",
  plugins: [react()],
  server: { port: 4040, strictPort: true },
});
