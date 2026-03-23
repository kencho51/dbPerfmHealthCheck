"use client";

import { usePathname } from "next/navigation";
import { NavBar } from "@/components/nav-bar";
import { IdleWarningModal } from "@/components/idle-warning-modal";
import { useIdleTimeout } from "@/hooks/use-idle-timeout";

const NO_SHELL_PATHS = ["/login"];

/**
 * AppShell — renders the sidebar NavBar + content wrapper for authenticated pages.
 * On public paths (e.g. /login) it renders children directly without any chrome.
 * Also manages idle-timeout detection: warns at 9 min, auto-logs out at 10 min.
 */
export function AppShell({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const isPublicPage = NO_SHELL_PATHS.some((p) => pathname.startsWith(p));

  // Idle timeout is only active on authenticated pages
  const { showWarning, secondsLeft, stayLoggedIn } = useIdleTimeout(!isPublicPage);

  if (isPublicPage) {
    return <>{children}</>;
  }

  return (
    <div className="flex h-screen">
      <NavBar />
      <main className="flex-1 overflow-auto p-8">{children}</main>
      {showWarning && (
        <IdleWarningModal
          secondsLeft={secondsLeft}
          onStayLoggedIn={stayLoggedIn}
        />
      )}
    </div>
  );
}
