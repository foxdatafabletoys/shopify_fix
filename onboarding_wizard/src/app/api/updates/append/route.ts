import { NextResponse } from "next/server";

import {
  appendToUpdatesDocument,
  ensureUpdatesTab,
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
    mode: "update",
    conversationId: payload.conversationId ?? null,
    turns,
  });

  try {
    const content = await processTranscriptWithOpenRouter({
      mode: "update",
      profile,
      turns,
      conversationId: payload.conversationId ?? null,
    });

    const tab = await ensureUpdatesTab({
      title: getProfileDisplayName(profile, user),
      existingTabId: profile?.updates_tab_id,
    });

    await appendToUpdatesDocument({
      documentId: tab.documentId,
      tabId: tab.tabId,
      content,
    });

    await updateProfileDocumentRefs({
      userId: user.id,
      onboardingDocId: profile?.onboarding_doc_id,
      onboardingDocUrl: profile?.onboarding_doc_url,
      updatesTabId: tab.tabId,
    });

    await completeConversationRun({
      runId,
      processedSummary: content,
      targetDocId: tab.documentId,
      targetTabId: tab.tabId,
    });

    return NextResponse.json({
      content,
      documentUrl: tab.documentUrl,
    });
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Failed to append update.";

    await failConversationRun(runId, message).catch(() => null);

    return NextResponse.json({ error: message }, { status: 500 });
  }
}
