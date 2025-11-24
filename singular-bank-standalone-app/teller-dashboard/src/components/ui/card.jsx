import { cn } from "../../lib/utils";

export function Card({ className, ...props }) {
  return (
    <div
      className={cn(
        "rounded-2xl border border-slate-200/90 bg-card text-card-foreground shadow-soft-card",
        className
      )}
      {...props}
    />
  );
}

export function CardHeader({ className, ...props }) {
  return (
    <div className={cn("flex flex-col gap-1 border-b border-slate-100 px-6 py-4", className)} {...props} />
  );
}

export function CardTitle({ className, ...props }) {
  return (
    <h3 className={cn("text-lg font-semibold leading-none tracking-tight", className)} {...props} />
  );
}

export function CardDescription({ className, ...props }) {
  return (
    <p className={cn("text-sm text-muted-foreground", className)} {...props} />
  );
}

export function CardContent({ className, ...props }) {
  return <div className={cn("px-6 py-4", className)} {...props} />;
}

