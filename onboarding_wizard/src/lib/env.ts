function getRequiredEnv(name: string) {
  const value = process.env[name];

  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }

  return value;
}

function getOptionalEnv(name: string) {
  const value = process.env[name];

  return value && value.length > 0 ? value : null;
}

export function getSupabaseEnv() {
  return {
    url: getRequiredEnv("NEXT_PUBLIC_SUPABASE_URL"),
    anonKey: getRequiredEnv("NEXT_PUBLIC_SUPABASE_ANON_KEY"),
    serviceRoleKey: getRequiredEnv("SUPABASE_SERVICE_ROLE_KEY"),
  };
}

export function getOpenRouterEnv() {
  return {
    apiKey: getRequiredEnv("OPENROUTER_API_KEY"),
    model: getRequiredEnv("OPENROUTER_MODEL"),
    siteUrl: getOptionalEnv("OPENROUTER_SITE_URL"),
    siteName: getOptionalEnv("OPENROUTER_SITE_NAME"),
  };
}

export function getElevenLabsEnv() {
  return {
    apiKey: getRequiredEnv("ELEVENLABS_API_KEY"),
    onboardingAgentId: getRequiredEnv("ELEVENLABS_ONBOARDING_AGENT_ID"),
    updatesAgentId: getRequiredEnv("ELEVENLABS_UPDATES_AGENT_ID"),
    environment: getOptionalEnv("ELEVENLABS_ENVIRONMENT"),
  };
}

export function getGoogleWorkspaceEnv() {
  return {
    clientEmail: getRequiredEnv("GOOGLE_SERVICE_ACCOUNT_EMAIL"),
    privateKey: getRequiredEnv("GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY").replace(
      /\\n/g,
      "\n",
    ),
    onboardingTemplateDocId: getRequiredEnv(
      "GOOGLE_ONBOARDING_TEMPLATE_DOC_ID",
    ),
    onboardingTargetFolderId: getOptionalEnv(
      "GOOGLE_ONBOARDING_TARGET_FOLDER_ID",
    ),
    updatesDocId: getRequiredEnv("GOOGLE_UPDATES_DOC_ID"),
  };
}
