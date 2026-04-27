import { google, type docs_v1 } from "googleapis";

import type { ConversationMode } from "@/lib/data";
import { getGoogleWorkspaceEnv } from "@/lib/env";

type FlatTab = {
  id: string;
  title: string;
};

function buildGoogleAuth() {
  const env = getGoogleWorkspaceEnv();

  return new google.auth.JWT({
    email: env.clientEmail,
    key: env.privateKey,
    scopes: [
      "https://www.googleapis.com/auth/documents",
      "https://www.googleapis.com/auth/drive",
    ],
  });
}

function getDocsClient() {
  return google.docs({
    version: "v1",
    auth: buildGoogleAuth(),
  });
}

function getDriveClient() {
  return google.drive({
    version: "v3",
    auth: buildGoogleAuth(),
  });
}

function flattenTabs(tabs: docs_v1.Schema$Tab[] | null | undefined): FlatTab[] {
  if (!tabs) {
    return [];
  }

  return tabs.flatMap((tab) => {
    const current =
      tab.tabProperties?.tabId && tab.tabProperties?.title
        ? [
            {
              id: tab.tabProperties.tabId,
              title: tab.tabProperties.title,
            },
          ]
        : [];

    return [...current, ...flattenTabs(tab.childTabs)];
  });
}

async function getDocumentTabs(documentId: string) {
  const docs = getDocsClient();
  const { data } = await docs.documents.get({
    documentId,
    includeTabsContent: true,
  });

  return flattenTabs(data.tabs);
}

async function getPrimaryTabId(documentId: string) {
  const tabs = await getDocumentTabs(documentId);

  return tabs[0]?.id ?? null;
}

function buildDocumentUrl(documentId: string) {
  return `https://docs.google.com/document/d/${documentId}/edit`;
}

export async function ensureOnboardingDocument(options: {
  displayName: string;
  existingDocumentId?: string | null;
}) {
  if (options.existingDocumentId) {
    return {
      documentId: options.existingDocumentId,
      documentUrl: buildDocumentUrl(options.existingDocumentId),
    };
  }

  const env = getGoogleWorkspaceEnv();
  const drive = getDriveClient();

  const { data } = await drive.files.copy({
    fileId: env.onboardingTemplateDocId,
    fields: "id,webViewLink",
    requestBody: {
      name: options.displayName,
      ...(env.onboardingTargetFolderId
        ? { parents: [env.onboardingTargetFolderId] }
        : {}),
    },
  });

  if (!data.id) {
    throw new Error("Google Drive did not return an onboarding document ID.");
  }

  return {
    documentId: data.id,
    documentUrl: data.webViewLink || buildDocumentUrl(data.id),
  };
}

export async function ensureUpdatesTab(options: {
  title: string;
  existingTabId?: string | null;
}) {
  const env = getGoogleWorkspaceEnv();
  const docs = getDocsClient();
  const currentTabs = await getDocumentTabs(env.updatesDocId);

  const matchingTab =
    currentTabs.find((tab) => tab.id === options.existingTabId) ||
    currentTabs.find((tab) => tab.title === options.title);

  if (matchingTab) {
    return {
      documentId: env.updatesDocId,
      tabId: matchingTab.id,
      title: matchingTab.title,
      documentUrl: buildDocumentUrl(env.updatesDocId),
    };
  }

  await docs.documents.batchUpdate({
    documentId: env.updatesDocId,
    requestBody: {
      requests: [
        {
          addDocumentTab: {
            tabProperties: {
              title: options.title,
            },
          },
        },
      ],
    },
  });

  const refreshedTabs = await getDocumentTabs(env.updatesDocId);
  const createdTab = refreshedTabs.find((tab) => tab.title === options.title);

  if (!createdTab) {
    throw new Error("Failed to create Google Docs tab for user updates.");
  }

  return {
    documentId: env.updatesDocId,
    tabId: createdTab.id,
    title: createdTab.title,
    documentUrl: buildDocumentUrl(env.updatesDocId),
  };
}

async function appendMarkdown(options: {
  documentId: string;
  content: string;
  tabId?: string | null;
}) {
  const docs = getDocsClient();

  await docs.documents.batchUpdate({
    documentId: options.documentId,
    requestBody: {
      requests: [
        {
          insertText: {
            text: options.content,
            endOfSegmentLocation: options.tabId
              ? { tabId: options.tabId }
              : {},
          },
        },
      ],
    },
  });
}

function createEntryHeading(mode: ConversationMode) {
  const label = mode === "onboarding" ? "Onboarding capture" : "Update capture";

  return `## ${label} - ${new Date().toLocaleString("en-GB", {
    dateStyle: "medium",
    timeStyle: "short",
  })}`;
}

export async function appendToOnboardingDocument(options: {
  documentId: string;
  content: string;
}) {
  const tabId = await getPrimaryTabId(options.documentId);

  await appendMarkdown({
    documentId: options.documentId,
    tabId,
    content: `\n\n${createEntryHeading("onboarding")}\n\n${options.content.trim()}\n`,
  });
}

export async function appendToUpdatesDocument(options: {
  documentId: string;
  tabId: string;
  content: string;
}) {
  await appendMarkdown({
    documentId: options.documentId,
    tabId: options.tabId,
    content: `\n\n${createEntryHeading("update")}\n\n${options.content.trim()}\n`,
  });
}
