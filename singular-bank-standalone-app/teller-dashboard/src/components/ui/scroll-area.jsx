import { cn } from "../../lib/utils";

export function ScrollArea({ className, children, ...props }) {
  return (
    <div
      className={cn(
        "overflow-y-auto overflow-x-hidden [scrollbar-width:thin] [scrollbar-color:rgba(148,163,184,0.5)_transparent]",
        className
      )}
      {...props}
    >
      {children}
    </div>
  );
}

