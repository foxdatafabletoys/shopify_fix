import Link from "next/link";

import { VoiceWorkflow } from "@/components/voice-workflow";
import { getRequiredAuthContext } from "@/lib/auth";
import { getProfileDisplayName } from "@/lib/data";

export const dynamic = "force-dynamic";

export default async function UpdatesPage() {
  const { user, profile } = await getRequiredAuthContext();
  const displayName = getProfileDisplayName(profile, user);

  return (
    <main className="flex flex-1 py-10 md:py-14">
      <div className="section-shell space-y-6">
        <div className="flex items-center justify-between gap-4">
          <Link
            href="/dashboard"
            className="text-sm font-semibold text-[var(--accent)]"
          >
            Back to dashboard
          </Link>
        </div>

        <VoiceWorkflow
          mode="update"
          title="Append fresh changes without rewriting history"
          description="Record verbal updates whenever warehouse processes, suppliers, systems, or responsibilities change. The output is appended into your dedicated tab in the master updates Google Doc."
          profile={{
            displayName,
            companyName: profile?.company_name ?? null,
            roleTitle: profile?.role_title ?? null,
          }}
        />
      </div>
    </main>
  );
}
