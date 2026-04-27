"use client";

import {
  Conversation,
  type IncomingSocketEvent,
  type Mode as ElevenMode,
  type Status,
  type VoiceConversation,
} from "@elevenlabs/client";
import { startTransition, useEffect, useRef, useState } from "react";

import type { ConversationMode } from "@/lib/data";
import type { TranscriptTurn } from "@/lib/transcript";

type VoiceWorkflowProps = {
  mode: ConversationMode;
  title: string;
  description: string;
  profile: {
    displayName: string;
    companyName: string | null;
    roleTitle: string | null;
  };
};

function upsertTurn(
  turns: TranscriptTurn[],
  nextTurn: TranscriptTurn,
): TranscriptTurn[] {
  const existingIndex = turns.findIndex((turn) => turn.id === nextTurn.id);

  if (existingIndex === -1) {
    return [...turns, nextTurn];
  }

  return turns.map((turn, index) =>
    index === existingIndex ? { ...turn, text: nextTurn.text } : turn,
  );
}

export function VoiceWorkflow({
  mode,
  title,
  description,
  profile,
}: VoiceWorkflowProps) {
  const [agentMode, setAgentMode] = useState<ElevenMode>("listening");
  const [connectionStatus, setConnectionStatus] =
    useState<Status>("disconnected");
  const [isBooting, setIsBooting] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<{
    documentUrl: string;
    content: string;
  } | null>(null);
  const [turns, setTurns] = useState<TranscriptTurn[]>([]);
  const conversationRef = useRef<VoiceConversation | null>(null);
  const conversationIdRef = useRef<string | null>(null);

  function handleIncomingMessage(event: IncomingSocketEvent) {
    if (event.type === "conversation_initiation_metadata") {
      conversationIdRef.current =
        event.conversation_initiation_metadata_event.conversation_id;
      return;
    }

    if (event.type === "user_transcript") {
      startTransition(() => {
        setTurns((current) =>
          upsertTurn(current, {
            id: `user-${event.user_transcription_event.event_id}`,
            role: "user",
            eventId: event.user_transcription_event.event_id,
            text: event.user_transcription_event.user_transcript,
          }),
        );
      });
      return;
    }

    if (event.type === "agent_response") {
      startTransition(() => {
        setTurns((current) =>
          upsertTurn(current, {
            id: `assistant-${event.agent_response_event.event_id}`,
            role: "assistant",
            eventId: event.agent_response_event.event_id,
            text: event.agent_response_event.agent_response,
          }),
        );
      });
      return;
    }

    if (event.type === "agent_response_correction") {
      startTransition(() => {
        setTurns((current) => {
          const matchingIndex = current.findIndex(
            (turn) =>
              turn.role === "assistant" &&
              turn.text ===
                event.agent_response_correction_event.original_agent_response,
          );

          if (matchingIndex === -1) {
            return current;
          }

          return current.map((turn, index) =>
            index === matchingIndex
              ? {
                  ...turn,
                  text: event.agent_response_correction_event.corrected_agent_response,
                }
              : turn,
          );
        });
      });
    }
  }

  async function startSession() {
    setIsBooting(true);
    setError(null);
    setResult(null);
    setTurns([]);
    conversationIdRef.current = null;

    try {
      await navigator.mediaDevices.getUserMedia({ audio: true });

      const response = await fetch(`/api/elevenlabs/signed-url?mode=${mode}`);

      if (!response.ok) {
        const data = (await response.json()) as { error?: string };
        throw new Error(data.error || "Failed to start voice session.");
      }

      const data = (await response.json()) as { signedUrl: string };
      const conversation = await Conversation.startSession({
        signedUrl: data.signedUrl,
        dynamicVariables: {
          user_name: profile.displayName,
          company_name: profile.companyName || "Unknown company",
          role_title: profile.roleTitle || "Unknown role",
          mode,
        },
        onConnect: ({ conversationId }) => {
          conversationIdRef.current = conversationId;
        },
        onDisconnect: () => {
          setConnectionStatus("disconnected");
        },
        onError: (message) => {
          setError(String(message));
        },
        onMessage: handleIncomingMessage,
        onModeChange: setAgentMode,
        onStatusChange: setConnectionStatus,
      });

      conversationRef.current = conversation as VoiceConversation;
    } catch (sessionError) {
      const message =
        sessionError instanceof Error
          ? sessionError.message
          : "Voice session failed to start.";

      setError(message);
    } finally {
      setIsBooting(false);
    }
  }

  async function saveTranscript() {
    const endpoint =
      mode === "onboarding" ? "/api/onboarding/complete" : "/api/updates/append";

    const response = await fetch(endpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        conversationId: conversationIdRef.current,
        turns,
      }),
    });

    if (!response.ok) {
      const data = (await response.json()) as { error?: string };
      throw new Error(data.error || "Failed to save transcript.");
    }

    const data = (await response.json()) as {
      content: string;
      documentUrl: string;
    };

    setResult(data);
  }

  async function finishAndSave() {
    setIsSaving(true);
    setError(null);

    try {
      if (conversationRef.current) {
        const activeConversation = conversationRef.current;
        conversationRef.current = null;
        await activeConversation.endSession();
      }

      if (turns.length === 0) {
        throw new Error("No transcript was captured yet.");
      }

      await saveTranscript();
    } catch (saveError) {
      const message =
        saveError instanceof Error
          ? saveError.message
          : "Failed to save transcript.";

      setError(message);
    } finally {
      setIsSaving(false);
    }
  }

  useEffect(() => {
    return () => {
      if (!conversationRef.current) {
        return;
      }

      const activeConversation = conversationRef.current;
      conversationRef.current = null;
      void activeConversation.endSession();
    };
  }, []);

  return (
    <div className="grid gap-6 xl:grid-cols-[1.15fr_0.85fr]">
      <section className="glass-panel rounded-[2rem] p-6 md:p-7">
        <div className="flex flex-col gap-4 border-b border-[var(--border)] pb-5">
          <div className="space-y-2">
            <p className="text-xs font-semibold uppercase tracking-[0.28em] text-[var(--muted)]">
              {mode === "onboarding" ? "Voice onboarding" : "Voice updates"}
            </p>
            <h1 className="text-3xl font-semibold tracking-tight">{title}</h1>
            <p className="max-w-2xl text-sm leading-7 text-[var(--muted)]">
              {description}
            </p>
          </div>

          <dl className="grid gap-3 text-sm md:grid-cols-3">
            <div className="rounded-2xl border border-[var(--border)] bg-white/30 px-4 py-3 dark:bg-white/5">
              <dt className="text-[var(--muted)]">Connection</dt>
              <dd className="mt-1 font-medium capitalize">{connectionStatus}</dd>
            </div>
            <div className="rounded-2xl border border-[var(--border)] bg-white/30 px-4 py-3 dark:bg-white/5">
              <dt className="text-[var(--muted)]">Agent mode</dt>
              <dd className="mt-1 font-medium capitalize">{agentMode}</dd>
            </div>
            <div className="rounded-2xl border border-[var(--border)] bg-white/30 px-4 py-3 dark:bg-white/5">
              <dt className="text-[var(--muted)]">Captured turns</dt>
              <dd className="mt-1 font-medium">{turns.length}</dd>
            </div>
          </dl>
        </div>

        <div className="mt-6 flex flex-wrap gap-3">
          <button
            type="button"
            onClick={startSession}
            disabled={isBooting || connectionStatus !== "disconnected"}
            className="rounded-full bg-[var(--accent)] px-5 py-3 text-sm font-semibold text-white transition hover:bg-[var(--accent-strong)] disabled:cursor-not-allowed disabled:opacity-70"
          >
            {isBooting ? "Connecting..." : "Start voice session"}
          </button>
          <button
            type="button"
            onClick={finishAndSave}
            disabled={isSaving || (turns.length === 0 && connectionStatus === "disconnected")}
            className="rounded-full border border-[var(--border)] px-5 py-3 text-sm font-semibold transition hover:border-[var(--accent)] disabled:cursor-not-allowed disabled:opacity-70"
          >
            {isSaving ? "Saving to Google Docs..." : "Finish and save"}
          </button>
        </div>

        {error ? (
          <p className="mt-5 rounded-2xl border border-red-400/30 bg-red-500/10 px-4 py-3 text-sm text-red-700 dark:text-red-200">
            {error}
          </p>
        ) : null}

        <div className="mt-6 space-y-3">
          {turns.length === 0 ? (
            <div className="rounded-[1.75rem] border border-dashed border-[var(--border)] px-5 py-10 text-sm text-[var(--muted)]">
              Once the session starts, transcript turns will appear here in real
              time.
            </div>
          ) : (
            turns.map((turn) => (
              <article
                key={turn.id}
                className="rounded-[1.6rem] border border-[var(--border)] px-5 py-4"
              >
                <p className="text-xs font-semibold uppercase tracking-[0.22em] text-[var(--muted)]">
                  {turn.role === "user" ? profile.displayName : "Agent"}
                </p>
                <p className="mt-2 text-sm leading-7">{turn.text}</p>
              </article>
            ))
          )}
        </div>
      </section>

      <aside className="glass-panel rounded-[2rem] p-6 md:p-7">
        <div className="space-y-2 border-b border-[var(--border)] pb-5">
          <p className="text-xs font-semibold uppercase tracking-[0.28em] text-[var(--muted)]">
            Doc write preview
          </p>
          <h2 className="text-2xl font-semibold tracking-tight">
            Canonical output
          </h2>
          <p className="text-sm leading-7 text-[var(--muted)]">
            The processed OpenRouter output will appear here after it is written
            into Google Docs.
          </p>
        </div>

        {result ? (
          <div className="mt-6 space-y-5">
            <a
              href={result.documentUrl}
              target="_blank"
              rel="noreferrer"
              className="inline-flex rounded-full border border-[var(--border)] px-4 py-2 text-sm font-semibold transition hover:border-[var(--accent)]"
            >
              Open Google Doc
            </a>
            <pre className="overflow-x-auto rounded-[1.75rem] border border-[var(--border)] bg-black/5 p-5 text-sm leading-7 whitespace-pre-wrap dark:bg-white/5">
              {result.content}
            </pre>
          </div>
        ) : (
          <div className="mt-6 rounded-[1.75rem] border border-dashed border-[var(--border)] px-5 py-10 text-sm leading-7 text-[var(--muted)]">
            Nothing has been written yet. Finish a voice session to process the
            transcript and append it into the canonical document flow.
          </div>
        )}
      </aside>
    </div>
  );
}
