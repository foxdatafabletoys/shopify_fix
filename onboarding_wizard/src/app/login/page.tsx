import Link from "next/link";
import { redirect } from "next/navigation";

import { AuthForm } from "@/components/auth-form";
import { getOptionalAuthContext } from "@/lib/auth";

export const dynamic = "force-dynamic";

export default async function LoginPage() {
  const { user } = await getOptionalAuthContext();

  if (user) {
    redirect("/dashboard");
  }

  return (
    <main className="flex flex-1 items-center py-10 md:py-14">
      <div className="section-shell flex flex-col gap-6">
        <AuthForm mode="login" />
        <p className="px-2 text-sm text-[var(--muted)]">
          Need an account?{" "}
          <Link href="/signup" className="font-semibold text-[var(--accent)]">
            Create one here
          </Link>
          .
        </p>
      </div>
    </main>
  );
}
