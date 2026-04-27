import { createServerClient } from "@supabase/ssr";
import { createClient } from "@supabase/supabase-js";
import { cookies } from "next/headers";

import { getSupabaseEnv } from "@/lib/env";
import type { Database } from "@/lib/supabase/types";

let adminClient: ReturnType<typeof createClient<Database>> | null = null;

export async function getSupabaseServerClient() {
  const cookieStore = await cookies();
  const env = getSupabaseEnv();

  return createServerClient<Database>(env.url, env.anonKey, {
    cookies: {
      getAll() {
        return cookieStore.getAll();
      },
      setAll(cookiesToSet) {
        try {
          cookiesToSet.forEach(({ name, value, options }) => {
            cookieStore.set(name, value, options);
          });
        } catch {
          // Server Components may not allow setting cookies during render.
        }
      },
    },
  });
}

export function getSupabaseAdminClient() {
  if (!adminClient) {
    const env = getSupabaseEnv();

    adminClient = createClient<Database>(env.url, env.serviceRoleKey, {
      auth: {
        autoRefreshToken: false,
        persistSession: false,
      },
    });
  }

  return adminClient;
}
