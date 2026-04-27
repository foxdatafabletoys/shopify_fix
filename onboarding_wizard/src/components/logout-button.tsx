"use client";

import { useRouter } from "next/navigation";
import { useTransition } from "react";

import { getSupabaseBrowserClient } from "@/lib/supabase/browser";

export function LogoutButton() {
  const [isPending, startTransition] = useTransition();
  const router = useRouter();

  function signOut() {
    startTransition(async () => {
      const supabase = getSupabaseBrowserClient();
      await supabase.auth.signOut();
      router.push("/login");
      router.refresh();
    });
  }

  return (
    <button
      type="button"
      onClick={signOut}
      disabled={isPending}
      className="rounded-full border border-[var(--border)] px-4 py-2 text-sm font-medium transition hover:border-[var(--accent)] disabled:opacity-70"
    >
      {isPending ? "Signing out..." : "Sign out"}
    </button>
  );
}
