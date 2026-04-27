import Link from "next/link";

import { LogoutButton } from "@/components/logout-button";
import { getRequiredAuthContext } from "@/lib/auth";
import { getProfileDisplayName } from "@/lib/data";

export const dynamic = "force-dynamic";

export default async function DashboardPage() {
  const { user, profile } = await getRequiredAuthContext();
  const displayName = getProfileDisplayName(profile, user);

  const cards = [
    {
      href: "/onboarding",
      eyebrow: "Initial capture",
      title: "Run the full onboarding interview",
      body: "Use the ElevenLabs agent to collect the deep business context that normally stays trapped in people's heads.",
    },
    {
      href: "/updates",
      eyebrow: "Ongoing changes",
      title: "Record verbal updates",
      body: "Append fresh operational changes into the separate canonical updates document with one tab per teammate.",
    },
  ];

  return (
    <main className="flex flex-1 py-10 md:py-14">
      <div className="section-shell space-y-8">
        <section className="glass-panel rounded-[2.5rem] px-6 py-8 md:px-9 md:py-10">
          <div className="flex flex-col gap-5 md:flex-row md:items-end md:justify-between">
            <div className="space-y-3">
              <p className="text-xs font-semibold uppercase tracking-[0.28em] text-[var(--muted)]">
                Dashboard
              </p>
              <h1 className="text-4xl font-semibold tracking-tight md:text-5xl">
                {displayName}
              </h1>
              <p className="max-w-2xl text-sm leading-7 text-[var(--muted)]">
                Capture onboarding conversations, append updates, and keep Google
                Docs as the single canonical source for operational knowledge.
              </p>
            </div>
            <LogoutButton />
          </div>
        </section>

        <section className="grid gap-6 md:grid-cols-2">
          {cards.map((card) => (
            <Link
              key={card.href}
              href={card.href}
              className="glass-panel rounded-[2.2rem] px-6 py-7 transition hover:-translate-y-1"
            >
              <p className="text-xs font-semibold uppercase tracking-[0.24em] text-[var(--muted)]">
                {card.eyebrow}
              </p>
              <h2 className="mt-4 text-2xl font-semibold tracking-tight">
                {card.title}
              </h2>
              <p className="mt-3 text-sm leading-7 text-[var(--muted)]">
                {card.body}
              </p>
            </Link>
          ))}
        </section>

        <section className="grid gap-6 md:grid-cols-2">
          <div className="glass-panel rounded-[2rem] px-6 py-6">
            <p className="text-xs font-semibold uppercase tracking-[0.24em] text-[var(--muted)]">
              Onboarding doc
            </p>
            <p className="mt-3 text-sm leading-7 text-[var(--muted)]">
              {profile?.onboarding_doc_url
                ? "Your onboarding document has already been linked."
                : "Your onboarding document will be created from the Google Docs template on first successful onboarding save."}
            </p>
            {profile?.onboarding_doc_url ? (
              <a
                href={profile.onboarding_doc_url}
                target="_blank"
                rel="noreferrer"
                className="mt-5 inline-flex rounded-full border border-[var(--border)] px-4 py-2 text-sm font-semibold transition hover:border-[var(--accent)]"
              >
                Open onboarding doc
              </a>
            ) : null}
          </div>
          <div className="glass-panel rounded-[2rem] px-6 py-6">
            <p className="text-xs font-semibold uppercase tracking-[0.24em] text-[var(--muted)]">
              Update strategy
            </p>
            <p className="mt-3 text-sm leading-7 text-[var(--muted)]">
              Update writes are append-only. The system may create a dedicated
              tab for you in the master updates document, but it never deletes
              existing content.
            </p>
          </div>
        </section>
      </div>
    </main>
  );
}
