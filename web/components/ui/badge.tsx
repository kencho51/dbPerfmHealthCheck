import * as React from "react";
import { cn } from "@/lib/utils";
import { type VariantProps, cva } from "class-variance-authority";

const badgeVariants = cva(
  "inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs font-semibold transition-colors",
  {
    variants: {
      variant: {
        default: "border-transparent bg-slate-900 text-white",
        critical: "border-transparent bg-red-600 text-white",
        warning: "border-transparent bg-amber-500 text-white",
        info: "border-transparent bg-blue-500 text-white",
        outline: "border-slate-300 text-slate-700",
        prod: "border-transparent bg-indigo-600 text-white",
        sat: "border-transparent bg-teal-600 text-white",
        sql: "border-transparent bg-slate-600 text-white",
        mongo: "border-transparent bg-green-700 text-white",
        success: "border-transparent bg-green-500 text-white",
        error: "border-transparent bg-red-500 text-white",
      },
    },
    defaultVariants: { variant: "default" },
  }
);

export interface BadgeProps
  extends React.HTMLAttributes<HTMLDivElement>,
    VariantProps<typeof badgeVariants> {}

function Badge({ className, variant, ...props }: BadgeProps) {
  return <div className={cn(badgeVariants({ variant }), className)} {...props} />;
}

export { Badge, badgeVariants };
