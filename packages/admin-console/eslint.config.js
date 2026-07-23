import js from "@eslint/js";
import tseslint from "typescript-eslint";
import reactHooks from "eslint-plugin-react-hooks";
import reactRefresh from "eslint-plugin-react-refresh";

export default tseslint.config(
  { ignores: ["dist", "src/lib/client"] },
  js.configs.recommended,
  tseslint.configs.recommendedTypeChecked,
  {
    files: ["**/*.{ts,tsx}"],
    languageOptions: {
      parserOptions: {
        projectService: true,
        tsconfigRootDir: import.meta.dirname,
      },
    },
    plugins: {
      "react-hooks": reactHooks,
      "react-refresh": reactRefresh,
    },
    rules: {
      ...reactHooks.configs.recommended.rules,
      "react-refresh/only-export-components": "warn",
    },
  },
  {
    // Layer boundary: pages/components must go through src/queries/,
    // never the generated query options or SDK directly.
    // (src/queries/ and src/lib/ are exempt — they ARE the seam.)
    files: ["src/**/*.{ts,tsx}"],
    ignores: ["src/queries/**", "src/lib/**"],
    rules: {
      "@typescript-eslint/no-restricted-imports": [
        "error",
        {
          patterns: [
            {
              group: ["**/lib/client/@tanstack/*", "@/lib/client/@tanstack/*"],
              message:
                "Import query options from '@/queries/' instead of the generated TanStack layer.",
            },
            {
              group: [
                "**/lib/client",
                "**/lib/client/**",
                "@/lib/client",
                "@/lib/client/**",
              ],
              allowTypeImports: true,
              message:
                "Runtime SDK imports belong in src/queries/ or src/lib/api.ts. Types are allowed: use `import { type Foo }`.",
            },
          ],
        },
      ],
    },
  },
  {
    files: ["**/*.{js,mjs,cjs}"],
    extends: [tseslint.configs.disableTypeChecked],
  },
  {
    // shadcn components intentionally export cva variants alongside
    // components; full-reload HMR on these files is an acceptable cost.
    files: ["src/components/ui/**/*.tsx"],
    rules: {
      "react-refresh/only-export-components": "off",
    },
  },
);
