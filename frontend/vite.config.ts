import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

// In dev (npm run dev) API calls are proxied to FastAPI on :8000.
// In production FastAPI serves the built files from dist/.
export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: { host: true, proxy: { "/api": "http://localhost:8000" } },
});
