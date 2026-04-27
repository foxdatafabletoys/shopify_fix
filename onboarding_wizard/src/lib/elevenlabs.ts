import { getElevenLabsEnv } from "@/lib/env";
import type { ConversationMode } from "@/lib/data";

function getAgentIdForMode(mode: ConversationMode) {
  const env = getElevenLabsEnv();

  return mode === "onboarding"
    ? env.onboardingAgentId
    : env.updatesAgentId;
}

export async function createSignedConversationUrl(mode: ConversationMode) {
  const env = getElevenLabsEnv();
  const agentId = getAgentIdForMode(mode);

  const url = new URL(
    "https://api.elevenlabs.io/v1/convai/conversation/get-signed-url",
  );
  url.searchParams.set("agent_id", agentId);

  if (env.environment) {
    url.searchParams.set("environment", env.environment);
  }

  const response = await fetch(url, {
    headers: {
      "xi-api-key": env.apiKey,
    },
  });

  if (!response.ok) {
    const message = await response.text();

    throw new Error(
      `Failed to create ElevenLabs signed URL: ${response.status} ${message}`,
    );
  }

  const data = (await response.json()) as { signed_url?: string };

  if (!data.signed_url) {
    throw new Error("ElevenLabs did not return a signed_url value.");
  }

  return data.signed_url;
}
