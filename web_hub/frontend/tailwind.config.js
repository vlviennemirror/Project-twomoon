/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        "tm-bg": "#0a0a0f",
        "tm-surface": "#12121a",
        "tm-border": "rgba(255, 255, 255, 0.06)",
        "tm-accent": "#6366f1",
        "tm-accent-dim": "#4f46e5",
        "tm-success": "#22c55e",
        "tm-warning": "#eab308",
        "tm-danger": "#ef4444",
        "tm-text": "#e2e8f0",
        "tm-muted": "#64748b",
      },
      backdropBlur: {
        glass: "16px",
      },
      backgroundImage: {
        "glass-gradient":
          "linear-gradient(135deg, rgba(255,255,255,0.05) 0%, rgba(255,255,255,0.02) 100%)",
      },
      boxShadow: {
        glass: "0 8px 32px rgba(0, 0, 0, 0.4)",
        "glass-inset": "inset 0 1px 0 rgba(255, 255, 255, 0.05)",
      },
      animation: {
        "pulse-slow": "pulse 3s cubic-bezier(0.4, 0, 0.6, 1) infinite",
        "fade-in": "fadeIn 0.3s ease-out",
      },
      keyframes: {
        fadeIn: {
          "0%": { opacity: "0", transform: "translateY(8px)" },
          "100%": { opacity: "1", transform: "translateY(0)" },
        },
      },
    },
  },
  plugins: [],
};
