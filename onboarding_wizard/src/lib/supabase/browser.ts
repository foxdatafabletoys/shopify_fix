"use client";

import { createBrowserClient } from "@supabase/ssr";

import { getSupabaseEnv } from "@/lib/env";
import type { Database } from "@/lib/supabase/types";

let browserClient:
  | ReturnType<typeof createBrowserClient<Database>>
  | undefined;

export function getSupabaseBrowserClient() {
  if (!browserClient) {
    const env = getSupabaseEnv();
    browserClient = createBrowserClient<Database>(env.url, env.anonKey);
  }

  return browserClient;
}
