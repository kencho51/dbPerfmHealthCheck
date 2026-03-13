"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { BarChart3, Database, LayoutDashboard, Upload, Layers, Tag, Users, LogOut, UserCircle } from "lucide-react";
import { cn } from "@/lib/utils";
import { useEffect, useState } from "react";
import { getUser, clearAuth, type AuthUser } from "@/lib/auth-client";

const nav = [
  { href: "/dashboard", label: "Dashboard", icon: LayoutDashboard },
  { href: "/upload",    label: "Upload",    icon: Upload },
  { href: "/queries",   label: "Queries",   icon: Database },
  { href: "/patterns",  label: "Curated",   icon: Layers },
  { href: "/labels",    label: "Labels",    icon: Tag },
];

export function NavBar() {
  const pathname = usePathname();
  const router = useRouter();
  const [user, setUser] = useState<AuthUser | null>(null);

  useEffect(() => {
    setUser(getUser());
  }, []);

  function handleLogout() {
    clearAuth();
    router.push("/login");
    router.refresh();
  }

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

        {user?.role === "admin" && (
          <Link
            href="/admin/users"
            className={cn(
              "flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium transition-colors mt-2 border-t border-slate-100 pt-3",
              pathname.startsWith("/admin/users")
                ? "bg-amber-50 text-amber-700"
                : "text-slate-600 hover:bg-slate-100 hover:text-slate-900"
            )}
          >
            <Users className="h-4 w-4 shrink-0" />
            Users
          </Link>
        )}
      </nav>

      {/* User info + logout */}
      <div className="px-3 py-3 border-t border-slate-100">
        {user ? (
          <div className="flex flex-col gap-1">
            <Link
              href="/account"
              className={`flex items-center gap-2 rounded-lg px-3 py-2 transition-colors ${
                pathname.startsWith("/account")
                  ? "bg-indigo-50 text-indigo-700"
                  : "hover:bg-slate-100"
              }`}
            >
              <UserCircle className="h-4 w-4 shrink-0 text-slate-400" />
              <div className="min-w-0">
                <p className="text-xs font-semibold text-slate-700 truncate">{user.username}</p>
                <p className="text-[11px] text-slate-400 capitalize">{user.role}</p>
              </div>
            </Link>
            <button
              onClick={handleLogout}
              className="flex items-center gap-2 rounded-md px-3 py-2 text-sm text-slate-500 hover:bg-red-50 hover:text-red-600 transition-colors w-full"
            >
              <LogOut className="h-4 w-4" />
              Sign out
            </button>
          </div>
        ) : (
          <div className="px-5 py-2 text-xs text-slate-400">
            FastAPI · SQLite · Next.js
          </div>
        )}
      </div>
    </aside>
  );
}
