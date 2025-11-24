import { cn } from "../../lib/utils";

export function Badge({ className, variant = "default", ...props }) {
  const variants = {
    default: "bg-primary/10 text-primary border border-primary/10",
    secondary: "bg-secondary text-secondary-foreground border border-secondary",
    outline: "border border-dashed border-slate-300 text-muted-foreground",
    success: "bg-emerald-50 text-emerald-700 border border-emerald-100",
    warning: "bg-amber-50 text-amber-700 border border-amber-100",
  };

  return (
    <span
      className={cn(
        "inline-flex items-center rounded-full px-3 py-1 text-xs font-semibold tracking-wide",
        variants[variant] || variants.default,
        className
      )}
      {...props}
    />
  );
}

