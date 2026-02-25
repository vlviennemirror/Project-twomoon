interface StatusBadgeProps {
  status: string;
  dotColor?: string;
  bgColor?: string;
  size?: "sm" | "md";
}

export default function StatusBadge({
  status,
  dotColor = "bg-tm-muted",
  bgColor = "bg-white/[0.03] border-tm-border",
  size = "sm",
}: StatusBadgeProps) {
  const sizeClasses =
    size === "md"
      ? "px-3 py-1.5 text-xs gap-2"
      : "px-2 py-0.5 text-[11px] gap-1.5";

  return (
    <span
      className={`inline-flex items-center font-medium border rounded-full ${bgColor} ${sizeClasses}`}
    >
      <span
        className={`rounded-full flex-shrink-0 ${dotColor} ${
          size === "md" ? "w-2.5 h-2.5" : "w-2 h-2"
        }`}
      />
      {status}
    </span>
  );
}
