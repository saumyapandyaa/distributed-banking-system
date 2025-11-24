import { cn } from "../../lib/utils";

export function Label({ className, ...props }) {
  return (
    <label
      className={cn(
        "text-sm font-medium text-muted-foreground transition-colors peer-disabled:cursor-not-allowed peer-disabled:opacity-70",
        className
      )}
      {...props}
    />
  );
}

