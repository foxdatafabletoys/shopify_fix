import type { ConversationMode, ProfileRecord } from "@/lib/data";
import { getOpenRouterEnv } from "@/lib/env";
import { transcriptToPlainText, type TranscriptTurn } from "@/lib/transcript";

type OpenRouterResponse = {
  choices?: Array<{
    message?: {
      content?:
        | string
        | Array<{
            type?: string;
            text?: string;
          }>;
    };
  }>;
};

function buildSystemPrompt(mode: ConversationMode) {
  if (mode === "onboarding") {
    return [
      "You turn raw onboarding conversations into clear internal operational documentation.",
      "Preserve concrete facts such as names of warehouses, suppliers, systems, people, and workflows.",
      "Do not invent missing details.",
      "Use readable markdown with concise headings and bullets only when they improve clarity.",
      "Because v1 has no rigid schema, organize the material pragmatically rather than forcing a template.",
    ].join(" ");
  }

  return [
    "You turn raw spoken update conversations into clean internal update notes.",
    "Focus on what changed, why it matters, and any open follow-up points.",
    "Do not invent missing facts.",
    "Return readable markdown that can be appended directly into a running Google Doc.",
  ].join(" ");
}

function buildUserPrompt(options: {
  mode: ConversationMode;
  profile: ProfileRecord | null;
  turns: TranscriptTurn[];
  conversationId?: string | null;
}) {
  const transcript = transcriptToPlainText(options.turns);
  const person =
    options.profile?.full_name || options.profile?.email || "Unknown teammate";

  const intro =
    options.mode === "onboarding"
      ? "Create onboarding knowledge notes for this teammate."
      : "Create an update entry for this teammate.";

  return [
    intro,
    `Teammate: ${person}`,
    options.profile?.company_name
      ? `Company: ${options.profile.company_name}`
      : null,
    options.profile?.role_title ? `Role: ${options.profile.role_title}` : null,
    options.conversationId ? `Conversation ID: ${options.conversationId}` : null,
    "",
    "Transcript:",
    transcript,
  ]
    .filter(Boolean)
    .join("\n");
}

function extractMessageContent(payload: OpenRouterResponse) {
  const content = payload.choices?.[0]?.message?.content;

  if (typeof content === "string") {
    return content;
  }

  if (Array.isArray(content)) {
    return content
      .map((part) => (typeof part.text === "string" ? part.text : ""))
      .join("")
      .trim();
  }

  return "";
}

export async function processTranscriptWithOpenRouter(options: {
  mode: ConversationMode;
  profile: ProfileRecord | null;
  turns: TranscriptTurn[];
  conversationId?: string | null;
}) {
  const env = getOpenRouterEnv();

  const response = await fetch("https://openrouter.ai/api/v1/chat/completions", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${env.apiKey}`,
      "Content-Type": "application/json",
      ...(env.siteUrl ? { "HTTP-Referer": env.siteUrl } : {}),
      ...(env.siteName ? { "X-Title": env.siteName } : {}),
    },
    body: JSON.stringify({
      model: env.model,
      messages: [
        {
          role: "system",
          content: buildSystemPrompt(options.mode),
        },
        {
          role: "user",
          content: buildUserPrompt(options),
        },
      ],
    }),
  });

  if (!response.ok) {
    const message = await response.text();

    throw new Error(
      `OpenRouter request failed with ${response.status}: ${message}`,
    );
  }

  const data = (await response.json()) as OpenRouterResponse;
  const content = extractMessageContent(data).trim();

  if (!content) {
    throw new Error("OpenRouter returned an empty completion.");
  }

  return content;
}
