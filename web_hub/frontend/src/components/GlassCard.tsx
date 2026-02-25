import type { ReactNode } from "react";

interface GlassCardProps {
  children: ReactNode;
  className?: string;
  padding?: boolean;
}

export default function GlassCard({
  children,
  className = "",
  padding = true,
}: GlassCardProps) {
  return (
    <div
      className={`bg-glass-gradient backdrop-blur-glass border border-tm-border shadow-glass rounded-2xl ${
        padding ? "p-6" : ""
      } ${className}`}
    >
      {children}
    </div>
  );
}
