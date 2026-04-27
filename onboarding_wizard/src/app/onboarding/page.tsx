import Link from "next/link";

import { VoiceWorkflow } from "@/components/voice-workflow";
import { getRequiredAuthContext } from "@/lib/auth";
import { getProfileDisplayName } from "@/lib/data";

export const dynamic = "force-dynamic";

export default async function OnboardingPage() {
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
          mode="onboarding"
          title="Capture the full business picture"
          description="Run the long-form onboarding conversation here. The finished transcript will be processed through OpenRouter and appended into the copied Google Doc for this teammate."
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
