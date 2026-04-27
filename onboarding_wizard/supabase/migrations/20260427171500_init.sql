create extension if not exists pgcrypto;

create table if not exists public.profiles (
  id uuid primary key references auth.users (id) on delete cascade,
  email text,
  full_name text,
  company_name text,
  role_title text,
  onboarding_doc_id text,
  onboarding_doc_url text,
  updates_tab_id text,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now())
);

create table if not exists public.conversation_runs (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references public.profiles (id) on delete cascade,
  mode text not null check (mode in ('onboarding', 'update')),
  elevenlabs_conversation_id text,
  status text not null default 'processing',
  raw_transcript jsonb,
  processed_summary text,
  target_doc_id text,
  target_tab_id text,
  error_message text,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now())
);

alter table public.profiles enable row level security;
alter table public.conversation_runs enable row level security;

create policy if not exists "profiles_select_own"
on public.profiles
for select
to authenticated
using (auth.uid() = id);

create policy if not exists "profiles_update_own"
on public.profiles
for update
to authenticated
using (auth.uid() = id)
with check (auth.uid() = id);

create policy if not exists "profiles_insert_own"
on public.profiles
for insert
to authenticated
with check (auth.uid() = id);

create policy if not exists "conversation_runs_select_own"
on public.conversation_runs
for select
to authenticated
using (auth.uid() = user_id);

create policy if not exists "conversation_runs_insert_own"
on public.conversation_runs
for insert
to authenticated
with check (auth.uid() = user_id);

create policy if not exists "conversation_runs_update_own"
on public.conversation_runs
for update
to authenticated
using (auth.uid() = user_id)
with check (auth.uid() = user_id);
