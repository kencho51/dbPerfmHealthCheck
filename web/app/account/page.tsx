"use client";

import { useState, FormEvent, useEffect } from "react";
import { UserCircle, Mail, Lock, CheckCircle2, AlertCircle, Loader2 } from "lucide-react";
import { getToken, getUser, saveAuth } from "@/lib/auth-client";
import { authApi } from "@/lib/api";
import { useRouter } from "next/navigation";

export default function AccountPage() {
  const router = useRouter();
  const token = getToken();
  const currentUser = getUser();

  useEffect(() => {
    if (!token) router.replace("/login");
  }, []);

  // ── Email form ──────────────────────────────────────────────────────────
  const [newEmail, setNewEmail] = useState(currentUser?.email ?? "");
  const [emailPassword, setEmailPassword] = useState("");
  const [emailLoading, setEmailLoading] = useState(false);
  const [emailMsg, setEmailMsg] = useState<{ text: string; ok: boolean } | null>(null);

  async function handleEmailSubmit(e: FormEvent) {
    e.preventDefault();
    if (!token) return;
    setEmailLoading(true);
    setEmailMsg(null);
    try {
      const updated = await authApi.updateProfile(token, {
        email: newEmail,
        current_password: emailPassword,
      });
      // Persist the updated user so the NavBar reflects the new email
      saveAuth(token, updated);
      setEmailPassword("");
      setEmailMsg({ text: "Email updated successfully.", ok: true });
    } catch (err: unknown) {
      setEmailMsg({ text: err instanceof Error ? err.message : "Update failed", ok: false });
    } finally {
      setEmailLoading(false);
    }
  }

  // ── Password form ───────────────────────────────────────────────────────
  const [currentPw, setCurrentPw] = useState("");
  const [newPw, setNewPw] = useState("");
  const [confirmPw, setConfirmPw] = useState("");
  const [pwLoading, setPwLoading] = useState(false);
  const [pwMsg, setPwMsg] = useState<{ text: string; ok: boolean } | null>(null);

  async function handlePasswordSubmit(e: FormEvent) {
    e.preventDefault();
    if (!token) return;
    if (newPw !== confirmPw) {
      setPwMsg({ text: "New passwords do not match.", ok: false });
      return;
    }
    if (newPw.length < 8) {
      setPwMsg({ text: "New password must be at least 8 characters.", ok: false });
      return;
    }
    setPwLoading(true);
    setPwMsg(null);
    try {
      await authApi.updateProfile(token, {
        current_password: currentPw,
        new_password: newPw,
      });
      setCurrentPw("");
      setNewPw("");
      setConfirmPw("");
      setPwMsg({ text: "Password changed successfully.", ok: true });
    } catch (err: unknown) {
      setPwMsg({ text: err instanceof Error ? err.message : "Update failed", ok: false });
    } finally {
      setPwLoading(false);
    }
  }

  if (!currentUser) return null;

  return (
    <div className="max-w-xl mx-auto space-y-8">
      {/* Header */}
      <div className="flex items-center gap-3">
        <UserCircle className="h-7 w-7 text-indigo-600" />
        <div>
          <h1 className="text-2xl font-bold text-slate-900">Account Settings</h1>
          <p className="text-sm text-slate-500">
            Logged in as <span className="font-semibold text-slate-700">{currentUser.username}</span>
            {" · "}
            <span className={`text-xs font-semibold rounded-full px-1.5 py-0.5 ${
              currentUser.role === "admin"
                ? "bg-amber-100 text-amber-700"
                : "bg-slate-100 text-slate-600"
            }`}>
              {currentUser.role}
            </span>
          </p>
        </div>
      </div>

      {/* Change email */}
      <section className="bg-white rounded-xl border border-slate-200 shadow-sm p-6 space-y-4">
        <div className="flex items-center gap-2 mb-1">
          <Mail className="h-4 w-4 text-slate-500" />
          <h2 className="font-semibold text-slate-800">Change Email</h2>
        </div>

        <form onSubmit={handleEmailSubmit} className="space-y-4">
          <div>
            <label className="block text-xs font-medium text-slate-600 mb-1">New email</label>
            <input
              type="email"
              required
              value={newEmail}
              onChange={(e) => setNewEmail(e.target.value)}
              className="w-full rounded-lg border border-slate-300 px-3.5 py-2.5 text-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500/20"
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-slate-600 mb-1">Current password to confirm</label>
            <input
              type="password"
              required
              value={emailPassword}
              onChange={(e) => setEmailPassword(e.target.value)}
              className="w-full rounded-lg border border-slate-300 px-3.5 py-2.5 text-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500/20"
              placeholder="••••••••"
            />
          </div>

          {emailMsg && (
            <Feedback ok={emailMsg.ok} text={emailMsg.text} />
          )}

          <button
            type="submit"
            disabled={emailLoading || newEmail === currentUser.email}
            className="flex items-center gap-2 rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700 disabled:opacity-50 transition"
          >
            {emailLoading && <Loader2 className="h-4 w-4 animate-spin" />}
            {emailLoading ? "Saving…" : "Update email"}
          </button>
        </form>
      </section>

      {/* Change password */}
      <section className="bg-white rounded-xl border border-slate-200 shadow-sm p-6 space-y-4">
        <div className="flex items-center gap-2 mb-1">
          <Lock className="h-4 w-4 text-slate-500" />
          <h2 className="font-semibold text-slate-800">Change Password</h2>
        </div>

        <form onSubmit={handlePasswordSubmit} className="space-y-4">
          <div>
            <label className="block text-xs font-medium text-slate-600 mb-1">Current password</label>
            <input
              type="password"
              required
              value={currentPw}
              onChange={(e) => setCurrentPw(e.target.value)}
              className="w-full rounded-lg border border-slate-300 px-3.5 py-2.5 text-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500/20"
              placeholder="••••••••"
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-slate-600 mb-1">New password</label>
            <input
              type="password"
              required
              minLength={8}
              value={newPw}
              onChange={(e) => setNewPw(e.target.value)}
              className="w-full rounded-lg border border-slate-300 px-3.5 py-2.5 text-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500/20"
              placeholder="min 8 characters"
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-slate-600 mb-1">Confirm new password</label>
            <input
              type="password"
              required
              value={confirmPw}
              onChange={(e) => setConfirmPw(e.target.value)}
              className={`w-full rounded-lg border px-3.5 py-2.5 text-sm focus:outline-none focus:ring-2 transition ${
                confirmPw && confirmPw !== newPw
                  ? "border-red-400 focus:border-red-500 focus:ring-red-500/20"
                  : "border-slate-300 focus:border-indigo-500 focus:ring-indigo-500/20"
              }`}
              placeholder="••••••••"
            />
            {confirmPw && confirmPw !== newPw && (
              <p className="mt-1 text-xs text-red-600">Passwords do not match.</p>
            )}
          </div>

          {pwMsg && (
            <Feedback ok={pwMsg.ok} text={pwMsg.text} />
          )}

          <button
            type="submit"
            disabled={pwLoading}
            className="flex items-center gap-2 rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700 disabled:opacity-50 transition"
          >
            {pwLoading && <Loader2 className="h-4 w-4 animate-spin" />}
            {pwLoading ? "Saving…" : "Change password"}
          </button>
        </form>
      </section>
    </div>
  );
}

function Feedback({ ok, text }: { ok: boolean; text: string }) {
  return (
    <div className={`flex items-center gap-2 rounded-lg border px-3.5 py-3 text-sm ${
      ok
        ? "bg-emerald-50 border-emerald-200 text-emerald-700"
        : "bg-red-50 border-red-200 text-red-700"
    }`}>
      {ok
        ? <CheckCircle2 className="h-4 w-4 shrink-0" />
        : <AlertCircle className="h-4 w-4 shrink-0" />}
      {text}
    </div>
  );
}
