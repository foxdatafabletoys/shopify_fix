export type TranscriptTurn = {
  id: string;
  role: "user" | "assistant";
  text: string;
  eventId?: number;
};

export type TranscriptPayload = {
  conversationId?: string | null;
  turns: TranscriptTurn[];
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

export function isTranscriptPayload(value: unknown): value is TranscriptPayload {
  if (!isRecord(value) || !Array.isArray(value.turns)) {
    return false;
  }

  return value.turns.every((turn) => {
    if (!isRecord(turn)) {
      return false;
    }

    return (
      typeof turn.id === "string" &&
      (turn.role === "user" || turn.role === "assistant") &&
      typeof turn.text === "string"
    );
  });
}

export function normalizeTranscriptTurns(turns: TranscriptTurn[]) {
  return turns
    .map((turn) => ({
      ...turn,
      text: turn.text.trim(),
    }))
    .filter((turn) => turn.text.length > 0);
}

export function transcriptToPlainText(turns: TranscriptTurn[]) {
  return turns
    .map((turn) => `${turn.role === "user" ? "User" : "Agent"}: ${turn.text}`)
    .join("\n");
}

export function transcriptToMarkdown(turns: TranscriptTurn[]) {
  return turns
    .map(
      (turn) =>
        `**${turn.role === "user" ? "User" : "Agent"}**\n${turn.text.trim()}`,
    )
    .join("\n\n");
}
