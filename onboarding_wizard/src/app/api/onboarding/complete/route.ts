import { NextResponse } from "next/server";

import {
  appendToOnboardingDocument,
  ensureOnboardingDocument,
} from "@/lib/google-docs";
import {
  completeConversationRun,
  createConversationRun,
  ensureProfileForUser,
  failConversationRun,
  getProfileDisplayName,
  updateProfileDocumentRefs,
} from "@/lib/data";
import { processTranscriptWithOpenRouter } from "@/lib/openrouter";
import { getSupabaseServerClient } from "@/lib/supabase/server";
import {
  isTranscriptPayload,
  normalizeTranscriptTurns,
} from "@/lib/transcript";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function POST(request: Request) {
  const payload = await request.json().catch(() => null);

  if (!isTranscriptPayload(payload)) {
    return NextResponse.json(
      { error: "Invalid transcript payload." },
      { status: 400 },
    );
  }

  const turns = normalizeTranscriptTurns(payload.turns);

  if (turns.length === 0) {
    return NextResponse.json(
      { error: "Transcript was empty." },
      { status: 400 },
    );
  }

  const supabase = await getSupabaseServerClient();
  const {
    data: { user },
  } = await supabase.auth.getUser();

  if (!user) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const profile = await ensureProfileForUser(user);
  const runId = await createConversationRun({
    userId: user.id,
    mode: "onboarding",
    conversationId: payload.conversationId ?? null,
    turns,
  });

  try {
    const content = await processTranscriptWithOpenRouter({
      mode: "onboarding",
      profile,
      turns,
      conversationId: payload.conversationId ?? null,
    });

    const displayName = getProfileDisplayName(profile, user);
    const document = await ensureOnboardingDocument({
      displayName,
      existingDocumentId: profile?.onboarding_doc_id,
    });

    await appendToOnboardingDocument({
      documentId: document.documentId,
      content,
    });

    await updateProfileDocumentRefs({
      userId: user.id,
      onboardingDocId: document.documentId,
      onboardingDocUrl: document.documentUrl,
      updatesTabId: profile?.updates_tab_id,
    });

    await completeConversationRun({
      runId,
      processedSummary: content,
      targetDocId: document.documentId,
      targetTabId: null,
    });

    return NextResponse.json({
      content,
      documentUrl: document.documentUrl,
    });
  } catch (error) {
    const message =
      error instanceof Error
        ? error.message
        : "Failed to process onboarding transcript.";

    await failConversationRun(runId, message).catch(() => null);

    return NextResponse.json({ error: message }, { status: 500 });
  }
}
