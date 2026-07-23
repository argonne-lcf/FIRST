import { defineConfig } from "@hey-api/openapi-ts";

export default defineConfig({
  input: "http://localhost:8000/openapi.json",
  output: "./src/lib/client",
  plugins: ["@hey-api/client-fetch", "@tanstack/react-query"],
});
