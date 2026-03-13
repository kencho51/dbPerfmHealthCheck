"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { Users, Plus, Trash2, ShieldCheck, Eye, Loader2, AlertCircle, CheckCircle2, X } from "lucide-react";
import { getToken, getUser } from "@/lib/auth-client";
import { authApi, type AuthUser, type UserRole } from "@/lib/api";

export default function AdminUsersPage() {
  const router = useRouter();
  const [users, setUsers] = useState<AuthUser[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [showCreateForm, setShowCreateForm] = useState(false);

  // Create form state
  const [newUsername, setNewUsername] = useState("");
  const [newEmail, setNewEmail] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [creating, setCreating] = useState(false);

  const currentUser = getUser();
  const token = getToken();

  // Guard: admin only
  useEffect(() => {
    if (currentUser?.role !== "admin") {
      router.replace("/dashboard");
    }
  }, []);

  async function fetchUsers() {
    if (!token) return;
    try {
      const data = await authApi.listUsers(token);
      setUsers(data);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Failed to load users");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    fetchUsers();
  }, []);

  function flash(msg: string, isError = false) {
    if (isError) {
      setError(msg);
      setTimeout(() => setError(null), 4000);
    } else {
      setSuccess(msg);
      setTimeout(() => setSuccess(null), 3000);
    }
  }

  async function toggleRole(user: AuthUser) {
    if (!token) return;
    const newRole: UserRole = user.role === "admin" ? "viewer" : "admin";
    try {
      await authApi.updateUser(token, user.id, { role: newRole });
      setUsers((prev) => prev.map((u) => (u.id === user.id ? { ...u, role: newRole } : u)));
      flash(`${user.username} is now ${newRole}`);
    } catch (err: unknown) {
      flash(err instanceof Error ? err.message : "Update failed", true);
    }
  }

  async function toggleActive(user: AuthUser) {
    if (!token) return;
    try {
      await authApi.updateUser(token, user.id, { is_active: !user.is_active });
      setUsers((prev) =>
        prev.map((u) => (u.id === user.id ? { ...u, is_active: !u.is_active } : u))
      );
      flash(`${user.username} ${!user.is_active ? "activated" : "deactivated"}`);
    } catch (err: unknown) {
      flash(err instanceof Error ? err.message : "Update failed", true);
    }
  }

  async function deleteUser(user: AuthUser) {
    if (!token) return;
    if (!confirm(`Delete user "${user.username}"? This cannot be undone.`)) return;
    try {
      await authApi.deleteUser(token, user.id);
      setUsers((prev) => prev.filter((u) => u.id !== user.id));
      flash(`${user.username} deleted`);
    } catch (err: unknown) {
      flash(err instanceof Error ? err.message : "Delete failed", true);
    }
  }

  async function createUser(e: React.FormEvent) {
    e.preventDefault();
    if (!token) return;
    setCreating(true);
    try {
      const created = await authApi.createUser(token, {
        username: newUsername,
        email: newEmail,
        password: newPassword,
      });
      setUsers((prev) => [...prev, created]);
      setNewUsername("");
      setNewEmail("");
      setNewPassword("");
      setShowCreateForm(false);
      flash(`User "${created.username}" created`);
    } catch (err: unknown) {
      flash(err instanceof Error ? err.message : "Create failed", true);
    } finally {
      setCreating(false);
    }
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="h-8 w-8 animate-spin text-indigo-500" />
      </div>
    );
  }

  return (
    <div className="max-w-4xl mx-auto space-y-6">
      {/* Header */}
      <div className="flex items-start justify-between">
        <div>
          <div className="flex items-center gap-2">
            <Users className="h-6 w-6 text-indigo-600" />
            <h1 className="text-2xl font-bold text-slate-900">User Management</h1>
          </div>
          <p className="text-sm text-slate-500 mt-1">
            Manage accounts, roles, and access. Admin only.
          </p>
        </div>
        <button
          onClick={() => setShowCreateForm((v) => !v)}
          className="flex items-center gap-2 rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white shadow-sm hover:bg-indigo-700 transition"
        >
          <Plus className="h-4 w-4" />
          Add user
        </button>
      </div>

      {/* Notifications */}
      {error && (
        <div className="flex items-center gap-2 rounded-lg bg-red-50 border border-red-200 px-4 py-3 text-sm text-red-700">
          <AlertCircle className="h-4 w-4 shrink-0" />
          {error}
        </div>
      )}
      {success && (
        <div className="flex items-center gap-2 rounded-lg bg-emerald-50 border border-emerald-200 px-4 py-3 text-sm text-emerald-700">
          <CheckCircle2 className="h-4 w-4 shrink-0" />
          {success}
        </div>
      )}

      {/* Create user form */}
      {showCreateForm && (
        <div className="bg-white rounded-xl border border-slate-200 shadow-sm p-6">
          <div className="flex items-center justify-between mb-4">
            <h2 className="font-semibold text-slate-800">New User</h2>
            <button onClick={() => setShowCreateForm(false)} className="text-slate-400 hover:text-slate-600">
              <X className="h-4 w-4" />
            </button>
          </div>
          <form onSubmit={createUser} className="grid grid-cols-1 sm:grid-cols-3 gap-4">
            <div>
              <label className="block text-xs font-medium text-slate-600 mb-1">Username</label>
              <input
                required
                value={newUsername}
                onChange={(e) => setNewUsername(e.target.value)}
                className="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500/20"
                placeholder="username"
              />
            </div>
            <div>
              <label className="block text-xs font-medium text-slate-600 mb-1">Email</label>
              <input
                type="email"
                required
                value={newEmail}
                onChange={(e) => setNewEmail(e.target.value)}
                className="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500/20"
                placeholder="user@example.com"
              />
            </div>
            <div>
              <label className="block text-xs font-medium text-slate-600 mb-1">Password</label>
              <input
                type="password"
                required
                minLength={8}
                value={newPassword}
                onChange={(e) => setNewPassword(e.target.value)}
                className="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500/20"
                placeholder="min 8 chars"
              />
            </div>
            <div className="sm:col-span-3 flex justify-end">
              <button
                type="submit"
                disabled={creating}
                className="flex items-center gap-2 rounded-lg bg-indigo-600 px-5 py-2 text-sm font-semibold text-white hover:bg-indigo-700 disabled:opacity-60 transition"
              >
                {creating && <Loader2 className="h-4 w-4 animate-spin" />}
                {creating ? "Creating…" : "Create user"}
              </button>
            </div>
          </form>
        </div>
      )}

      {/* Users table */}
      <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
        <table className="w-full text-sm">
          <thead className="bg-slate-50 border-b border-slate-200">
            <tr>
              <th className="text-left px-5 py-3 font-semibold text-slate-600">User</th>
              <th className="text-left px-4 py-3 font-semibold text-slate-600">Email</th>
              <th className="text-left px-4 py-3 font-semibold text-slate-600">Role</th>
              <th className="text-left px-4 py-3 font-semibold text-slate-600">Status</th>
              <th className="text-left px-4 py-3 font-semibold text-slate-600">Last login</th>
              <th className="px-4 py-3" />
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {users.map((u) => {
              const isSelf = u.id === currentUser?.id;
              return (
                <tr key={u.id} className="hover:bg-slate-50 transition-colors">
                  <td className="px-5 py-3 font-medium text-slate-800">
                    {u.username}
                    {isSelf && (
                      <span className="ml-2 text-[10px] font-semibold rounded-full bg-indigo-100 text-indigo-700 px-1.5 py-0.5">
                        you
                      </span>
                    )}
                  </td>
                  <td className="px-4 py-3 text-slate-500">{u.email}</td>
                  <td className="px-4 py-3">
                    <span
                      className={`inline-flex items-center gap-1 text-xs font-semibold rounded-full px-2 py-0.5 ${
                        u.role === "admin"
                          ? "bg-amber-100 text-amber-700"
                          : "bg-slate-100 text-slate-600"
                      }`}
                    >
                      {u.role === "admin" ? <ShieldCheck className="h-3 w-3" /> : <Eye className="h-3 w-3" />}
                      {u.role}
                    </span>
                  </td>
                  <td className="px-4 py-3">
                    <span
                      className={`text-xs font-semibold rounded-full px-2 py-0.5 ${
                        u.is_active
                          ? "bg-emerald-100 text-emerald-700"
                          : "bg-red-100 text-red-600"
                      }`}
                    >
                      {u.is_active ? "Active" : "Inactive"}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-slate-400 text-xs">
                    {u.last_login
                      ? new Date(u.last_login).toLocaleString()
                      : "Never"}
                  </td>
                  <td className="px-4 py-3">
                    {!isSelf && (
                      <div className="flex items-center gap-1 justify-end">
                        <button
                          onClick={() => toggleRole(u)}
                          title={`Switch to ${u.role === "admin" ? "viewer" : "admin"}`}
                          className="rounded-md p-1.5 text-slate-400 hover:bg-amber-50 hover:text-amber-600 transition"
                        >
                          {u.role === "admin" ? <Eye className="h-3.5 w-3.5" /> : <ShieldCheck className="h-3.5 w-3.5" />}
                        </button>
                        <button
                          onClick={() => toggleActive(u)}
                          title={u.is_active ? "Deactivate" : "Activate"}
                          className="rounded-md p-1.5 text-slate-400 hover:bg-slate-100 hover:text-slate-700 transition text-xs font-medium px-2"
                        >
                          {u.is_active ? "Deactivate" : "Activate"}
                        </button>
                        <button
                          onClick={() => deleteUser(u)}
                          title="Delete user"
                          className="rounded-md p-1.5 text-slate-400 hover:bg-red-50 hover:text-red-600 transition"
                        >
                          <Trash2 className="h-3.5 w-3.5" />
                        </button>
                      </div>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>

        {users.length === 0 && (
          <div className="py-12 text-center text-slate-400 text-sm">No users found.</div>
        )}
      </div>
    </div>
  );
}
