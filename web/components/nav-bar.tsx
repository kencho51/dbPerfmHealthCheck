"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { BarChart3, Database, LayoutDashboard, Upload, Layers, Tag } from "lucide-react";
import { cn } from "@/lib/utils";

const nav = [
  { href: "/dashboard", label: "Dashboard", icon: LayoutDashboard },
  { href: "/upload",    label: "Upload",    icon: Upload },
  { href: "/queries",   label: "Queries",   icon: Database },
  { href: "/patterns",  label: "Curated",   icon: Layers },
  { href: "/labels",    label: "Labels",    icon: Tag },
];

export function NavBar() {
  const pathname = usePathname();
  return (
    <aside className="flex h-screen w-56 flex-col border-r border-slate-200 bg-white">
      {/* Brand */}
      <div className="flex items-center gap-2 px-5 py-5 border-b border-slate-100">
        <BarChart3 className="h-5 w-5 text-indigo-600" />
        <span className="text-sm font-bold text-slate-900 leading-tight">
          DB Perfm<br />
          <span className="font-normal text-slate-500">Analysis</span>
        </span>
      </div>

      {/* Links */}
      <nav className="flex flex-col gap-1 p-3 flex-1">
        {nav.map(({ href, label, icon: Icon }) => {
          const active = pathname === href || pathname.startsWith(`${href}/`);
          return (
            <Link
              key={href}
              href={href}
              className={cn(
                "flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium transition-colors",
                active
                  ? "bg-indigo-50 text-indigo-700"
                  : "text-slate-600 hover:bg-slate-100 hover:text-slate-900"
              )}
            >
              <Icon className="h-4 w-4 shrink-0" />
              {label}
            </Link>
          );
        })}
      </nav>

      {/* Footer */}
      <div className="px-5 py-4 text-xs text-slate-400 border-t border-slate-100">
        FastAPI · SQLite · Next.js
      </div>
    </aside>
  );
}
