/**
 * useIdleTimeout — detects user inactivity and triggers auto-logout.
 *
 * How it works:
 *  1. Listens for activity events (mouse, keyboard, touch, scroll).
 *  2. On each event, updates the last-activity timestamp in localStorage.
 *  3. A 30-second interval checks idleMs against the thresholds:
 *       - IDLE_WARNING_MS  → show a "You'll be logged out soon" warning
 *       - IDLE_TIMEOUT_MS  → clear auth and redirect to /login
 *
 * Usage: call this hook once inside an authenticated layout (e.g. AppShell).
 */
"use client";

import { useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import {
  clearAuth,
  getIdleMs,
  isLoggedIn,
  updateLastActivity,
  IDLE_TIMEOUT_MS,
  IDLE_WARNING_MS,
} from "@/lib/auth-client";

const ACTIVITY_EVENTS = [
  "mousemove",
  "mousedown",
  "keydown",
  "touchstart",
  "scroll",
  "wheel",
] as const;

const CHECK_INTERVAL_MS = 30_000; // check every 30 seconds

export interface IdleState {
  /** True when the warning modal should be displayed. */
  showWarning: boolean;
  /** Remaining seconds until automatic logout (refreshes every 30s). */
  secondsLeft: number;
  /** Call this when the user clicks "Stay logged in". */
  stayLoggedIn: () => void;
}

export function useIdleTimeout(enabled = true): IdleState {
  const router = useRouter();
  const [showWarning, setShowWarning] = useState(false);
  const [secondsLeft, setSecondsLeft] = useState(
    Math.round((IDLE_TIMEOUT_MS - IDLE_WARNING_MS) / 1000)
  );
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Throttle activity updates to at most once per second
  const lastThrottleRef = useRef(0);
  const handleActivity = () => {
    const now = Date.now();
    if (now - lastThrottleRef.current > 1_000) {
      lastThrottleRef.current = now;
      updateLastActivity();
      // If the warning was visible but the user moved, dismiss it
      setShowWarning(false);
    }
  };

  const stayLoggedIn = () => {
    updateLastActivity();
    setShowWarning(false);
  };

  const doLogout = () => {
    clearAuth();
    router.replace("/login");
  };

  useEffect(() => {
    if (!enabled) return; // no-op on public pages (e.g. /login)

    // Attach activity listeners
    ACTIVITY_EVENTS.forEach((event) =>
      window.addEventListener(event, handleActivity, { passive: true })
    );

    // Start the idle checker
    intervalRef.current = setInterval(() => {
      if (!isLoggedIn()) {
        // Token expired or already cleared elsewhere
        doLogout();
        return;
      }

      const idle = getIdleMs();

      if (idle >= IDLE_TIMEOUT_MS) {
        doLogout();
        return;
      }

      if (idle >= IDLE_WARNING_MS) {
        const remaining = Math.max(
          0,
          Math.round((IDLE_TIMEOUT_MS - idle) / 1000)
        );
        setSecondsLeft(remaining);
        setShowWarning(true);
      } else {
        setShowWarning(false);
      }
    }, CHECK_INTERVAL_MS);

    return () => {
      ACTIVITY_EVENTS.forEach((event) =>
        window.removeEventListener(event, handleActivity)
      );
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return { showWarning, secondsLeft, stayLoggedIn };
}
