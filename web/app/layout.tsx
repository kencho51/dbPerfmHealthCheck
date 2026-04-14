import type { Metadata } from "next";
import "./globals.css";
import { AppShell } from "@/components/app-shell";
import Providers from "./providers";

export const metadata: Metadata = {
  title: "DB Perfm Analysis",
  description: "Database performance analysis dashboard",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className="min-h-screen bg-slate-50 text-slate-900 antialiased">
        <Providers>
          <AppShell>{children}</AppShell>
        </Providers>
      </body>
    </html>
  );
}
