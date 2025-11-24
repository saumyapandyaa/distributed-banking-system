import * as React from "react";
import { cn } from "../../lib/utils";

const variantClasses = {
  default:
    "bg-primary text-primary-foreground shadow hover:bg-primary/90 focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:ring-primary/60",
  secondary:
    "bg-secondary text-secondary-foreground hover:bg-secondary/80 focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:ring-secondary/60",
  outline:
    "border border-input bg-transparent hover:bg-slate-50 text-foreground focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:ring-primary/30",
  ghost:
    "bg-transparent text-foreground hover:bg-slate-50 focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:ring-primary/20",
  destructive:
    "bg-destructive text-destructive-foreground hover:bg-destructive/90 focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:ring-destructive/40",
};

const sizeClasses = {
  default: "h-10 px-4 py-2",
  sm: "h-9 rounded-md px-3",
  lg: "h-11 rounded-md px-5",
  icon: "h-10 w-10",
};

const Button = React.forwardRef(
  (
    { className, variant = "default", size = "default", asChild = false, ...props },
    ref
  ) => {
    const Comp = asChild ? "span" : "button";
    return (
      <Comp
        className={cn(
          "inline-flex items-center justify-center whitespace-nowrap rounded-md text-sm font-medium transition-all focus-visible:outline-none disabled:pointer-events-none disabled:opacity-50",
          variantClasses[variant],
          sizeClasses[size],
          className
        )}
        ref={ref}
        {...props}
      />
    );
  }
);
Button.displayName = "Button";

export { Button };

