"use client";

import { useRouter } from "next/navigation";
import { useState, useTransition } from "react";

import { getSupabaseBrowserClient } from "@/lib/supabase/browser";

type AuthFormProps = {
  mode: "login" | "signup";
};

export function AuthForm({ mode }: AuthFormProps) {
  const [error, setError] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();
  const router = useRouter();

  function handleSubmit(formData: FormData) {
    startTransition(async () => {
      setError(null);
      setMessage(null);

      const supabase = getSupabaseBrowserClient();
      const email = String(formData.get("email") ?? "").trim();
      const password = String(formData.get("password") ?? "");

      if (!email || !password) {
        setError("Email and password are required.");
        return;
      }

      if (mode === "signup") {
        const fullName = String(formData.get("fullName") ?? "").trim();
        const companyName = String(formData.get("companyName") ?? "").trim();
        const roleTitle = String(formData.get("roleTitle") ?? "").trim();

        if (!fullName) {
          setError("Full name is required.");
          return;
        }

        const { data, error: signUpError } = await supabase.auth.signUp({
          email,
          password,
          options: {
            data: {
              full_name: fullName,
              company_name: companyName || null,
              role_title: roleTitle || null,
            },
          },
        });

        if (signUpError) {
          setError(signUpError.message);
          return;
        }

        if (data.session) {
          router.push("/dashboard");
          router.refresh();
          return;
        }

        setMessage(
          "Account created. If your Supabase project requires email confirmation, check your inbox before signing in.",
        );
        return;
      }

      const { error: signInError } = await supabase.auth.signInWithPassword({
        email,
        password,
      });

      if (signInError) {
        setError(signInError.message);
        return;
      }

      router.push("/dashboard");
      router.refresh();
    });
  }

  return (
    <form
      action={handleSubmit}
      className="glass-panel flex w-full max-w-xl flex-col gap-5 rounded-[2rem] px-6 py-7 text-sm md:px-8"
    >
      <div className="space-y-2">
        <p className="text-xs font-semibold uppercase tracking-[0.28em] text-[var(--muted)]">
          {mode === "signup" ? "Create account" : "Welcome back"}
        </p>
        <h1 className="text-3xl font-semibold tracking-tight">
          {mode === "signup"
            ? "Start capturing what your team knows."
            : "Pick up where onboarding left off."}
        </h1>
      </div>

      {mode === "signup" ? (
        <>
          <label className="space-y-2">
            <span className="font-medium">Full name</span>
            <input
              name="fullName"
              required
              className="w-full rounded-2xl border border-[var(--border)] bg-white/70 px-4 py-3 outline-none transition focus:border-[var(--accent)] dark:bg-white/5"
              placeholder="Avery Chen"
            />
          </label>
          <div className="grid gap-4 md:grid-cols-2">
            <label className="space-y-2">
              <span className="font-medium">Company</span>
              <input
                name="companyName"
                className="w-full rounded-2xl border border-[var(--border)] bg-white/70 px-4 py-3 outline-none transition focus:border-[var(--accent)] dark:bg-white/5"
                placeholder="Northline Logistics"
              />
            </label>
            <label className="space-y-2">
              <span className="font-medium">Role</span>
              <input
                name="roleTitle"
                className="w-full rounded-2xl border border-[var(--border)] bg-white/70 px-4 py-3 outline-none transition focus:border-[var(--accent)] dark:bg-white/5"
                placeholder="Operations lead"
              />
            </label>
          </div>
        </>
      ) : null}

      <label className="space-y-2">
        <span className="font-medium">Email</span>
        <input
          type="email"
          name="email"
          required
          className="w-full rounded-2xl border border-[var(--border)] bg-white/70 px-4 py-3 outline-none transition focus:border-[var(--accent)] dark:bg-white/5"
          placeholder="name@company.com"
        />
      </label>

      <label className="space-y-2">
        <span className="font-medium">Password</span>
        <input
          type="password"
          name="password"
          required
          minLength={8}
          className="w-full rounded-2xl border border-[var(--border)] bg-white/70 px-4 py-3 outline-none transition focus:border-[var(--accent)] dark:bg-white/5"
          placeholder="Minimum 8 characters"
        />
      </label>

      {error ? (
        <p className="rounded-2xl border border-red-400/30 bg-red-500/10 px-4 py-3 text-sm text-red-700 dark:text-red-200">
          {error}
        </p>
      ) : null}

      {message ? (
        <p className="rounded-2xl border border-emerald-400/30 bg-emerald-500/10 px-4 py-3 text-sm text-emerald-700 dark:text-emerald-200">
          {message}
        </p>
      ) : null}

      <button
        type="submit"
        disabled={isPending}
        className="rounded-full bg-[var(--accent)] px-5 py-3 text-sm font-semibold text-white transition hover:bg-[var(--accent-strong)] disabled:cursor-not-allowed disabled:opacity-70"
      >
        {isPending
          ? "Working..."
          : mode === "signup"
            ? "Create account"
            : "Sign in"}
      </button>
    </form>
  );
}
