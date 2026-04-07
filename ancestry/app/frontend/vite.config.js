import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  base: "/ancestry/",
  server: {
    proxy: {
      "/api": "http://localhost:8700",
    },
  },
});
