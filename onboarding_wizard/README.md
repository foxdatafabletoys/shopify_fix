# Onboarding Pilot

Internal onboarding platform built with Next.js, Supabase Auth, ElevenLabs, OpenRouter, and Google Docs.

## What it does

- Authenticated teammates sign up and sign in with Supabase.
- ElevenLabs runs voice onboarding and update sessions in the browser.
- OpenRouter converts transcript turns into readable markdown notes.
- Google Docs remains the canonical storage layer:
  - onboarding creates or reuses a user-specific doc copied from a template
  - updates append into a separate pre-existing master doc with one tab per person
- Writes are append-only. The app never deletes Google Docs content.

## Environment

Copy `.env.example` to `.env.local` and fill in:

- `NEXT_PUBLIC_SUPABASE_URL`
- `NEXT_PUBLIC_SUPABASE_ANON_KEY`
- `SUPABASE_SERVICE_ROLE_KEY`
- `OPENROUTER_API_KEY`
- `OPENROUTER_MODEL`
- `ELEVENLABS_API_KEY`
- `ELEVENLABS_ONBOARDING_AGENT_ID`
- `ELEVENLABS_UPDATES_AGENT_ID`
- `GOOGLE_SERVICE_ACCOUNT_EMAIL`
- `GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY`
- `GOOGLE_ONBOARDING_TEMPLATE_DOC_ID`
- `GOOGLE_UPDATES_DOC_ID`

Optional:

- `OPENROUTER_SITE_URL`
- `OPENROUTER_SITE_NAME`
- `ELEVENLABS_ENVIRONMENT`
- `GOOGLE_ONBOARDING_TARGET_FOLDER_ID`

## Supabase setup

Run the migration in [20260427171500_init.sql](/Users/alessaweiler/Documents/telemachus/telemachus/onboarding_wizard/supabase/migrations/20260427171500_init.sql:1) against your Supabase project.

This creates:

- `profiles`
- `conversation_runs`

The app keeps transcript metadata in Supabase, but business knowledge itself is meant to live in Google Docs.

## Google Docs setup

1. Create a Google service account with Docs and Drive API access.
2. Share the onboarding template doc with that service account.
3. Share the updates master doc with that service account.
4. Optionally share a target folder if onboarding doc copies should land in a specific Drive location.

## Development

```bash
npm install
npm run dev
```

Open `http://localhost:3000`.

## Verification

```bash
npm run lint
npm run build
```
