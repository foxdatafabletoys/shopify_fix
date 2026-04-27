import Link from "next/link";
import { redirect } from "next/navigation";

import { getOptionalAuthContext } from "@/lib/auth";

export const dynamic = "force-dynamic";

const highlights = [
  "Supabase auth for internal access",
  "ElevenLabs voice sessions for onboarding and updates",
  "OpenRouter processing for readable internal notes",
  "Google Docs as canonical append-only storage",
];

export default async function HomePage() {
  const { user } = await getOptionalAuthContext();

  if (user) {
    redirect("/dashboard");
  }

  return (
    <main className="flex flex-1 py-10 md:py-14">
      <div className="section-shell grid gap-8 xl:grid-cols-[1.15fr_0.85fr]">
        <section className="glass-panel rounded-[2.5rem] px-6 py-8 md:px-9 md:py-10">
          <div className="max-w-3xl space-y-6">
            <p className="text-xs font-semibold uppercase tracking-[0.32em] text-[var(--muted)]">
              Internal ops capture
            </p>
            <h1 className="max-w-4xl text-5xl font-semibold tracking-tight md:text-6xl">
              Turn spoken tribal knowledge into Google Docs your whole team can
              actually use.
            </h1>
            <p className="max-w-2xl text-base leading-8 text-[var(--muted)] md:text-lg">
              Onboarding Pilot uses voice-first interviews to capture how the
              business really works, then routes the useful output into
              canonical Google Docs that stay compatible with NotebookLM.
            </p>
          </div>

          <div className="mt-8 flex flex-wrap gap-3">
            <Link
              href="/signup"
              className="rounded-full bg-[var(--accent)] px-5 py-3 text-sm font-semibold text-white transition hover:bg-[var(--accent-strong)]"
            >
              Create account
            </Link>
            <Link
              href="/login"
              className="rounded-full border border-[var(--border)] px-5 py-3 text-sm font-semibold transition hover:border-[var(--accent)]"
            >
              Sign in
            </Link>
          </div>
        </section>

        <aside className="glass-panel rounded-[2.5rem] px-6 py-8 md:px-8 md:py-10">
          <div className="space-y-4">
            <p className="text-xs font-semibold uppercase tracking-[0.32em] text-[var(--muted)]">
              What ships in v1
            </p>
            <div className="space-y-3">
              {highlights.map((item) => (
                <div
                  key={item}
                  className="rounded-[1.75rem] border border-[var(--border)] bg-white/35 px-4 py-4 text-sm leading-7 dark:bg-white/5"
                >
                  {item}
                </div>
              ))}
            </div>
          </div>
        </aside>
      </div>
    </main>
  );
}
