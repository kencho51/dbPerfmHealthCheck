"use client";

import { Button } from "@/components/ui/button";
import { clearAuth } from "@/lib/auth-client";
import { useRouter } from "next/navigation";

interface IdleWarningModalProps {
  secondsLeft: number;
  onStayLoggedIn: () => void;
}

/**
 * Fullscreen overlay shown when the user has been idle for IDLE_WARNING_MS.
 * Gives them the option to stay logged in or log out immediately.
 */
export function IdleWarningModal({
  secondsLeft,
  onStayLoggedIn,
}: IdleWarningModalProps) {
  const router = useRouter();

  const handleLogoutNow = () => {
    clearAuth();
    router.replace("/login");
  };

  return (
    // Backdrop
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/50"
      role="dialog"
      aria-modal="true"
      aria-labelledby="idle-warning-title"
    >
      {/* Modal card */}
      <div className="mx-4 w-full max-w-sm rounded-xl bg-white p-6 shadow-2xl">
        {/* Icon */}
        <div className="mb-4 flex items-center justify-center">
          <div className="flex h-12 w-12 items-center justify-center rounded-full bg-amber-100">
            <svg
              className="h-6 w-6 text-amber-600"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
              strokeWidth={2}
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                d="M12 9v3.75m-9.303 3.376c-.866 1.5.217 3.374 1.948 3.374h14.71c1.73 0 2.813-1.874 1.948-3.374L13.949 3.378c-.866-1.5-3.032-1.5-3.898 0L2.697 16.126ZM12 15.75h.007v.008H12v-.008Z"
              />
            </svg>
          </div>
        </div>

        {/* Text */}
        <h2
          id="idle-warning-title"
          className="mb-2 text-center text-lg font-semibold text-slate-900"
        >
          Still there?
        </h2>
        <p className="mb-6 text-center text-sm text-slate-500">
          You&apos;ll be logged out automatically due to inactivity in{" "}
          <span className="font-bold text-amber-600">{secondsLeft}s</span>.
        </p>

        {/* Actions */}
        <div className="flex gap-3">
          <Button
            variant="outline"
            className="flex-1"
            onClick={handleLogoutNow}
          >
            Log out now
          </Button>
          <Button className="flex-1" onClick={onStayLoggedIn}>
            Stay logged in
          </Button>
        </div>
      </div>
    </div>
  );
}
