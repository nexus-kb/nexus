import type {
  ThreadSearchResponse,
  MailingListResponse,
  MessageDetail,
  PatchLineage,
  PatchLineageCollectionResponse,
  ThreadDetail,
  ThreadListResponse,
  ThreadMessagesResponse,
  CommitLookup,
} from "./types";

interface VaporErrorBody {
  reason?: string;
  error?: boolean;
}

export class ApiError extends Error {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

export interface ThreadListParameters {
  mailingList?: string;
  cursor?: string;
  limit?: number;
}

export interface ThreadSearchParameters {
  mailingList?: string;
  q?: string;
  cursor?: string;
  limit?: number;
}

export function encodeMessageID(messageID: string): string {
  return encodeURIComponent(messageID);
}

export function threadRoute(messageID: string): string {
  const query = new URLSearchParams({ root: messageID });
  return `/thread?${query.toString()}`;
}

export function threadListURL({
  mailingList,
  cursor,
  limit = 25,
}: ThreadListParameters = {}): string {
  const query = new URLSearchParams({ limit: String(limit) });

  if (mailingList) {
    query.set("mailingList", mailingList);
  }

  if (cursor) {
    query.set("cursor", cursor);
  }

  return `/api/v1/threads?${query.toString()}`;
}

export function threadSearchURL({
  mailingList,
  q,
  cursor,
  limit = 25,
}: ThreadSearchParameters): string {
  const query = new URLSearchParams({ limit: String(limit) });

  if (mailingList) {
    query.set("mailingList", mailingList);
  }

  if (q) {
    query.set("q", q);
  }

  if (cursor) {
    query.set("cursor", cursor);
  }

  return `/api/v1/search?${query.toString()}`;
}

async function fetchJSON<T>(url: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(url, {
    headers: { Accept: "application/json" },
    signal,
  });

  if (!response.ok) {
    let message = `${response.status} ${response.statusText}`.trim();

    try {
      const body = (await response.json()) as VaporErrorBody;
      if (body.reason) {
        message = body.reason;
      }
    } catch {
      // Preserve the HTTP status text when the response is not JSON.
    }

    throw new ApiError(response.status, message);
  }

  return (await response.json()) as T;
}

export function getThreads(
  parameters?: ThreadListParameters,
  signal?: AbortSignal,
): Promise<ThreadListResponse> {
  return fetchJSON(threadListURL(parameters), signal);
}

export function searchThreads(
  parameters: ThreadSearchParameters,
  signal?: AbortSignal,
): Promise<ThreadSearchResponse> {
  return fetchJSON(threadSearchURL(parameters), signal);
}

export function getMailingLists(signal?: AbortSignal): Promise<MailingListResponse> {
  return fetchJSON("/api/v1/mailing-lists", signal);
}

export function getThread(rootMessageID: string, signal?: AbortSignal): Promise<ThreadDetail> {
  return fetchJSON(`/api/v1/threads/${encodeMessageID(rootMessageID)}`, signal);
}

export function getThreadMessages(
  rootMessageID: string,
  cursor?: string,
  signal?: AbortSignal,
): Promise<ThreadMessagesResponse> {
  const query = new URLSearchParams({ limit: "200" });
  if (cursor) {
    query.set("cursor", cursor);
  }

  return fetchJSON(
    `/api/v1/threads/${encodeMessageID(rootMessageID)}/messages?${query.toString()}`,
    signal,
  );
}

export function getMessage(messageID: string, signal?: AbortSignal): Promise<MessageDetail> {
  return fetchJSON(`/api/v1/messages/${encodeMessageID(messageID)}`, signal);
}

export function getPatchLineage(
  lineageID: number,
  signal?: AbortSignal,
): Promise<PatchLineage> {
  return fetchJSON("/api/v1/patch-lineages/" + lineageID, signal);
}

export function getThreadPatchLineages(
  rootMessageID: string,
  signal?: AbortSignal,
): Promise<PatchLineageCollectionResponse> {
  return fetchJSON(
    "/api/v1/threads/" + encodeMessageID(rootMessageID) + "/patch-lineages",
    signal,
  );
}

export function getCommit(commitID: string, signal?: AbortSignal): Promise<CommitLookup> {
  return fetchJSON(`/api/v1/commits/${encodeURIComponent(commitID)}`, signal);
}
