"use client";

import { usePathname } from "next/navigation";
import { NavBar } from "@/components/nav-bar";

const NO_SHELL_PATHS = ["/login"];

/**
 * AppShell — renders the sidebar NavBar + content wrapper for authenticated pages.
 * On public paths (e.g. /login) it renders children directly without any chrome.
 */
export function AppShell({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const isPublicPage = NO_SHELL_PATHS.some((p) => pathname.startsWith(p));

  if (isPublicPage) {
    return <>{children}</>;
  }

  return (
    <div className="flex h-screen">
      <NavBar />
      <main className="flex-1 overflow-auto p-8">{children}</main>
    </div>
  );
}
