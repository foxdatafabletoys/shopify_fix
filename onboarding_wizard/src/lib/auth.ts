import { redirect } from "next/navigation";

import { ensureProfileForUser } from "@/lib/data";
import { getSupabaseServerClient } from "@/lib/supabase/server";

export async function getOptionalAuthContext() {
  const supabase = await getSupabaseServerClient();
  const {
    data: { user },
  } = await supabase.auth.getUser();

  if (!user) {
    return {
      user: null,
      profile: null,
    };
  }

  const profile = await ensureProfileForUser(user);

  return {
    user,
    profile,
  };
}

export async function getRequiredAuthContext() {
  const context = await getOptionalAuthContext();

  if (!context.user) {
    redirect("/login");
  }

  return context;
}
