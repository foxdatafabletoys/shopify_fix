import { NextResponse } from "next/server";

import { createSignedConversationUrl } from "@/lib/elevenlabs";
import { getSupabaseServerClient } from "@/lib/supabase/server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET(request: Request) {
  const url = new URL(request.url);
  const mode = url.searchParams.get("mode");

  if (mode !== "onboarding" && mode !== "update") {
    return NextResponse.json(
      { error: "mode must be onboarding or update" },
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

  try {
    const signedUrl = await createSignedConversationUrl(mode);

    return NextResponse.json({ signedUrl });
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Failed to create signed URL.";

    return NextResponse.json({ error: message }, { status: 500 });
  }
}
