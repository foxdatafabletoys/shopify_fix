import type { User } from "@supabase/supabase-js";

import { getSupabaseAdminClient } from "@/lib/supabase/server";
import type { Database, Json } from "@/lib/supabase/types";
import type { TranscriptTurn } from "@/lib/transcript";

export type ConversationMode = "onboarding" | "update";
export type ProfileRecord = Database["public"]["Tables"]["profiles"]["Row"];

function readString(value: unknown) {
  return typeof value === "string" && value.trim().length > 0
    ? value.trim()
    : null;
}

export function getProfileDisplayName(profile: ProfileRecord | null, user: User) {
  return (
    profile?.full_name ||
    readString(user.user_metadata?.full_name) ||
    readString(user.user_metadata?.name) ||
    user.email ||
    "Team member"
  );
}

export async function ensureProfileForUser(user: User) {
  const admin = getSupabaseAdminClient();

  const profilePayload = {
    id: user.id,
    email: user.email ?? null,
    full_name:
      readString(user.user_metadata?.full_name) ||
      readString(user.user_metadata?.name),
    company_name: readString(user.user_metadata?.company_name),
    role_title: readString(user.user_metadata?.role_title),
    updated_at: new Date().toISOString(),
  };

  const { error: upsertError } = await admin
    .from("profiles")
    .upsert(profilePayload, { onConflict: "id" });

  if (upsertError) {
    throw upsertError;
  }

  const { data, error } = await admin
    .from("profiles")
    .select("*")
    .eq("id", user.id)
    .maybeSingle();

  if (error) {
    throw error;
  }

  return data;
}

export async function createConversationRun(options: {
  userId: string;
  mode: ConversationMode;
  conversationId?: string | null;
  turns: TranscriptTurn[];
}) {
  const admin = getSupabaseAdminClient();

  const { data, error } = await admin
    .from("conversation_runs")
    .insert({
      user_id: options.userId,
      mode: options.mode,
      elevenlabs_conversation_id: options.conversationId ?? null,
      status: "processing",
      raw_transcript: options.turns as unknown as Json,
      updated_at: new Date().toISOString(),
    })
    .select("id")
    .single();

  if (error) {
    throw error;
  }

  return data.id;
}

export async function completeConversationRun(options: {
  runId: string;
  processedSummary: string;
  targetDocId: string;
  targetTabId?: string | null;
}) {
  const admin = getSupabaseAdminClient();

  const { error } = await admin
    .from("conversation_runs")
    .update({
      status: "completed",
      processed_summary: options.processedSummary,
      target_doc_id: options.targetDocId,
      target_tab_id: options.targetTabId ?? null,
      updated_at: new Date().toISOString(),
    })
    .eq("id", options.runId);

  if (error) {
    throw error;
  }
}

export async function failConversationRun(runId: string, message: string) {
  const admin = getSupabaseAdminClient();

  const { error } = await admin
    .from("conversation_runs")
    .update({
      status: "failed",
      error_message: message,
      updated_at: new Date().toISOString(),
    })
    .eq("id", runId);

  if (error) {
    throw error;
  }
}

export async function updateProfileDocumentRefs(options: {
  userId: string;
  onboardingDocId?: string | null;
  onboardingDocUrl?: string | null;
  updatesTabId?: string | null;
}) {
  const admin = getSupabaseAdminClient();

  const { error } = await admin
    .from("profiles")
    .update({
      onboarding_doc_id: options.onboardingDocId,
      onboarding_doc_url: options.onboardingDocUrl,
      updates_tab_id: options.updatesTabId,
      updated_at: new Date().toISOString(),
    })
    .eq("id", options.userId);

  if (error) {
    throw error;
  }
}
