import { defineConfig } from "@hey-api/openapi-ts";

export default defineConfig({
  // Exported from the control server app by `npm run gen-sdk`; no stack needed.
  input: "./openapi.json",
  output: "./src/lib/client",
  plugins: ["@hey-api/client-fetch", "@tanstack/react-query"],
});
