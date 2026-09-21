import { HashRouter, Route } from "@solidjs/router";
import { cleanup, render, screen, waitFor, within } from "@solidjs/testing-library";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ThreadListPage } from "./pages/ThreadListPage";
import { ThreadPage } from "./pages/ThreadPage";
import type {
  ThreadSearchResponse,
  MessageDetail,
  PatchLineageCollectionResponse,
  ThreadDetail,
  ThreadListResponse,
  ThreadMessagesResponse,
} from "./types";

const thread: ThreadDetail = {
  rootMessageId: "root/id?value#fragment%test@example.com",
  subject: "[PATCH] net: repair the packet path",
  author: "Kernel Developer <developer@example.com>",
  startedAt: "2026-08-15T12:00:00Z",
  lastActivityAt: "2026-08-15T13:00:00Z",
  messageCount: 3,
  missingMessageCount: 1,
  kind: "patch-series",
  mailingLists: [{ name: "Linux Kernel Mailing List", archiveGroup: "lkml" }],
  subsystems: [{ name: "Networking", mailingListAddress: "netdev@example.com" }],
  patchSeries: [
    {
      coverLetterMessageId: null,
      status: "pending",
      totalParts: 2,
      receivedParts: 2,
    },
  ],
};

const searchResult: ThreadSearchResponse["items"][number] = {
  ...thread,
  score: 18.25,
  snippet: "The matching RCU grace period [text] is here.",
};

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  window.location.hash = "";
});

describe("ThreadListPage", () => {
  it("replaces the current threads with a skeleton while switching mailing lists", async () => {
    const initialSubject = "[PATCH] net: repair the packet path";
    const lkmlSubject = "[PATCH] lkml-only thread";
    const lkmlThread: ThreadDetail = {
      ...thread,
      rootMessageId: "lkml-thread@example.com",
      subject: lkmlSubject,
    };
    let resolveLkml!: (response: Response) => void;
    const lkmlResponse = new Promise<Response>((resolve) => {
      resolveLkml = resolve;
    });
    const fetchMock = vi.fn((input: RequestInfo | URL) => {
      const url = String(input);
      if (url === "/api/v1/mailing-lists") {
        return Promise.resolve(
          jsonResponse({
            items: [{ name: "Linux Kernel Mailing List", archiveGroup: "lkml" }],
          }),
        );
      }
      if (url.includes("mailingList=lkml")) {
        return lkmlResponse;
      }
      return Promise.resolve(
        jsonResponse({
          items: [thread],
          pagination: { previousCursor: null, nextCursor: null },
        } satisfies ThreadListResponse),
      );
    });
    vi.stubGlobal("fetch", fetchMock);

    const { container } = render(() => (
      <HashRouter preload={false}>
        <Route path="/" component={ThreadListPage} />
      </HashRouter>
    ));

    expect(await screen.findByText(initialSubject)).toBeInTheDocument();
    const resultsRegion = container.querySelector("[aria-live='polite']");
    expect(resultsRegion).toHaveAttribute("aria-busy", "false");
    await userEvent.selectOptions(screen.getByLabelText("Filter by mailing list"), "lkml");

    expect(await screen.findByRole("list", { name: "Loading threads" })).toBeInTheDocument();
    expect(resultsRegion).toBe(container.querySelector("[aria-live='polite']"));
    expect(resultsRegion).toHaveAttribute("aria-busy", "true");
    expect(screen.queryByText(initialSubject)).not.toBeInTheDocument();

    resolveLkml(
      jsonResponse({
        items: [lkmlThread],
        pagination: { previousCursor: null, nextCursor: null },
      } satisfies ThreadListResponse),
    );

    expect(await screen.findByText(lkmlSubject)).toBeInTheDocument();
    expect(resultsRegion).toBe(container.querySelector("[aria-live='polite']"));
    expect(resultsRegion).toHaveAttribute("aria-busy", "false");
    expect(screen.queryByText(initialSubject)).not.toBeInTheDocument();
    expect(screen.queryByRole("list", { name: "Loading threads" })).not.toBeInTheDocument();
  });

  it("filters, searches, clears search, resets the cursor, and paginates", async () => {
    window.location.hash = "#/?cursor=old-cursor";
    const firstPage: ThreadListResponse = {
      items: [thread],
      pagination: { previousCursor: "previous-cursor", nextCursor: "next-cursor" },
    };
    const fetchMock = vi.fn((input: RequestInfo | URL) => {
      const url = String(input);
      if (url === "/api/v1/mailing-lists") {
        return Promise.resolve(
          jsonResponse({
            items: [
              { name: "Linux Kernel Mailing List", archiveGroup: "lkml" },
              { name: "Netdev", archiveGroup: "netdev" },
            ],
          }),
        );
      }
      if (url.includes("q=date%3A2026-02-30")) {
        return Promise.resolve(
          jsonResponse(
            { error: true, reason: "Invalid search date: 2026-02-30" },
            400,
          ),
        );
      }
      if (url.startsWith("/api/v1/search?")) {
        return Promise.resolve(
          jsonResponse({
            items: [searchResult],
            pagination: {
              previousCursor: "previous-search-cursor",
              nextCursor: "next-search-cursor",
            },
          } satisfies ThreadSearchResponse),
        );
      }
      return Promise.resolve(jsonResponse(firstPage));
    });
    vi.stubGlobal("fetch", fetchMock);

    render(() => (
      <HashRouter preload={false}>
        <Route path="/" component={ThreadListPage} />
      </HashRouter>
    ));

    expect(await screen.findByText("[PATCH] net: repair the packet path")).toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: "Threads" })).not.toBeInTheDocument();
    expect(screen.queryByText("Mailing list")).not.toBeInTheDocument();
    expect(screen.queryByText("1 thread")).not.toBeInTheDocument();
    expect(screen.getByText("created 2026-08-15 12:00 UTC")).toBeInTheDocument();
    expect(screen.getByText("updated 2026-08-15 13:00 UTC")).toBeInTheDocument();
    expect(within(screen.getByRole("list")).getByText("lkml")).toHaveAttribute(
      "title",
      "Linux Kernel Mailing List",
    );
    expect(screen.getByRole("option", { name: "lkml" })).toBeInTheDocument();
    await userEvent.selectOptions(screen.getByLabelText("Filter by mailing list"), "netdev");

    await waitFor(() => {
      expect(window.location.hash).toContain("mailingList=netdev");
      expect(window.location.hash).not.toContain("old-cursor");
    });

    await userEvent.type(
      screen.getByRole("searchbox", { name: "Search threads" }),
      'RCU subject:"grace period" author:"Paul McKenney"',
    );
    await userEvent.click(screen.getByRole("button", { name: "Search" }));

    await waitFor(() => {
      expect(window.location.hash).toContain(
        "q=RCU+subject%3A%22grace+period%22+author%3A%22Paul+McKenney%22",
      );
      expect(window.location.hash).not.toContain("cursor=");
    });

    expect(screen.queryByText(/matching RCU grace period/)).not.toBeInTheDocument();
    expect(screen.queryByText("score 18.25")).not.toBeInTheDocument();
    expect(screen.getByText("3 messages")).toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: "Next" }));
    await waitFor(() => expect(window.location.hash).toContain("cursor=next-search-cursor"));

    await userEvent.click(screen.getByRole("button", { name: "Clear" }));
    await waitFor(() => {
      expect(window.location.hash).not.toContain("q=");
      expect(window.location.hash).not.toContain("cursor=");
    });
    expect(screen.getByRole("searchbox", { name: "Search threads" })).toHaveValue("");

    await userEvent.type(
      screen.getByRole("searchbox", { name: "Search threads" }),
      "date:2026-02-30",
    );
    await userEvent.click(screen.getByRole("button", { name: "Search" }));
    expect(
      await screen.findByRole("alert"),
    ).toHaveTextContent("Invalid search date: 2026-02-30");

    const searchRequests = fetchMock.mock.calls
      .map(([input]) => String(input))
      .filter((url) => url.startsWith("/api/v1/search?"));
    expect(searchRequests.some((url) => url.includes("mailingList=netdev"))).toBe(true);
    expect(
      searchRequests.some((url) =>
        url.includes(
          "q=RCU+subject%3A%22grace+period%22+author%3A%22Paul+McKenney%22",
        ),
      ),
    ).toBe(true);
  });
});

describe("ThreadPage", () => {
  it("loads full messages, expands them locally, and appends pages", async () => {
    const rootMessage: MessageDetail = {
      messageId: thread.rootMessageId,
      rootMessageId: thread.rootMessageId,
      inReplyToMessageId: null,
      referenceMessageIds: [],
      availability: "available" as const,
      subject: thread.subject,
      author: thread.author,
      to: [{ name: "Netdev", email: "netdev@example.com" }],
      cc: [],
      sentAt: thread.startedAt,
      body: "Full root message body",
      patch: { partIndex: 0, totalParts: 2 },
      mailingLists: thread.mailingLists,
      subsystems: thread.subsystems,
      loreUrl: "https://lore.kernel.org/r/root",
    };
    const firstMessages: ThreadMessagesResponse = {
      rootMessageId: thread.rootMessageId,
      items: [
        rootMessage,
        {
          messageId: "missing@example.com",
          rootMessageId: thread.rootMessageId,
          inReplyToMessageId: thread.rootMessageId,
          referenceMessageIds: [thread.rootMessageId],
          availability: "missing",
          subject: null,
          author: null,
          to: [],
          cc: [],
          sentAt: null,
          body: null,
          patch: null,
          mailingLists: [],
          subsystems: [],
          loreUrl: "https://lore.kernel.org/r/missing",
        },
      ],
      pagination: { previousCursor: null, nextCursor: "more-messages" },
    };
    const secondMessages: ThreadMessagesResponse = {
      rootMessageId: thread.rootMessageId,
      items: [
        {
          messageId: "child@example.com",
          rootMessageId: thread.rootMessageId,
          inReplyToMessageId: thread.rootMessageId,
          referenceMessageIds: [thread.rootMessageId],
          availability: "available",
          subject: "Re: repair the packet path",
          author: "Reviewer <reviewer@example.com>",
          to: [],
          cc: [],
          sentAt: "2026-08-15T13:00:00Z",
          body: "Reviewed-by: Reviewer",
          patch: null,
          mailingLists: thread.mailingLists,
          subsystems: thread.subsystems,
          loreUrl: "https://lore.kernel.org/r/child",
        },
      ],
      pagination: { previousCursor: "back", nextCursor: null },
    };
    const lineages: PatchLineageCollectionResponse = {
      items: [
        {
          id: 41,
          subject: "net: repair the packet path",
          firstSentAt: "2026-08-14T12:00:00Z",
          latestSentAt: thread.startedAt,
          revisions: [
            {
              patchsetId: 102,
              rootMessageId: thread.rootMessageId,
              coverLetterMessageId: thread.rootMessageId,
              subject: thread.subject!,
              author: thread.author,
              sentAt: thread.startedAt,
              status: "complete",
              totalParts: 2,
              receivedParts: 2,
              phase: "PATCH",
              revision: 2,
              revisionExplicit: true,
              isResend: false,
              changeId: "packet-path",
              baseCommit: null,
              matchSource: "change-id",
              matchConfidence: 100,
              mailingLists: thread.mailingLists,
            },
            {
              patchsetId: 101,
              rootMessageId: "v1@example.com",
              coverLetterMessageId: "v1@example.com",
              subject: "[PATCH] net: repair the packet path",
              author: thread.author,
              sentAt: "2026-08-14T12:00:00Z",
              status: "complete",
              totalParts: 2,
              receivedParts: 2,
              phase: "PATCH",
              revision: 1,
              revisionExplicit: false,
              isResend: false,
              changeId: "packet-path",
              baseCommit: null,
              matchSource: "change-id",
              matchConfidence: 100,
              mailingLists: thread.mailingLists,
            },
          ],
        },
      ],
    };
    let resolveLineages!: (response: Response) => void;
    const lineageResponse = new Promise<Response>((resolve) => {
      resolveLineages = resolve;
    });
    const fetchMock = vi.fn((input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes("/messages?")) {
        return Promise.resolve(
          jsonResponse(url.includes("cursor=more-messages") ? secondMessages : firstMessages),
        );
      }
      if (url.endsWith("/patch-lineages")) {
        return lineageResponse;
      }
      if (url.startsWith("/api/v1/threads/")) {
        return Promise.resolve(jsonResponse(thread));
      }
      return Promise.resolve(jsonResponse({ reason: "Not found" }, 404));
    });
    vi.stubGlobal("fetch", fetchMock);
    window.location.hash = `#${new URL("https://nexus.test" + `/thread?${new URLSearchParams({ root: thread.rootMessageId })}`).pathname}${new URL("https://nexus.test" + `/thread?${new URLSearchParams({ root: thread.rootMessageId })}`).search}`;

    render(() => (
      <HashRouter preload={false}>
        <Route path="/thread" component={ThreadPage} />
      </HashRouter>
    ));

    expect(await screen.findByText("Full root message body")).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "Threads" })).not.toBeInTheDocument();
    expect(screen.getByText("created 2026-08-15 12:00 UTC")).toBeInTheDocument();
    expect(screen.getByText("updated 2026-08-15 13:00 UTC")).toBeInTheDocument();
    expect(screen.getByText("2026-08-15 12:00 UTC")).toBeInTheDocument();
    expect(screen.getByRole("region", { name: "Patch lineage" })).toHaveAttribute(
      "aria-busy",
      "true",
    );
    expect(screen.queryByText("Patch lineage · 2 versions")).not.toBeInTheDocument();
    expect(screen.getByText("lkml")).toHaveAttribute(
      "title",
      "Linux Kernel Mailing List",
    );
    resolveLineages(jsonResponse(lineages));
    expect(await screen.findByText("Patch lineage · 2 versions")).toBeInTheDocument();
    expect(screen.getByRole("region", { name: "Patch lineage" })).not.toHaveAttribute(
      "aria-busy",
    );
    expect(screen.getByRole("link", { name: "PATCH v2" })).toHaveClass("active");
    expect(screen.getByRole("link", { name: "PATCH" })).toHaveAttribute(
      "href",
      expect.stringContaining("v1%40example.com"),
    );
    expect(screen.queryByText("[message unavailable]", { exact: false })).not.toBeInTheDocument();
    expect(screen.queryByText("patch 0/2")).not.toBeInTheDocument();
    expect(
      fetchMock.mock.calls.some(([input]) => String(input).includes("missing%40example.com")),
    ).toBe(false);

    await userEvent.click(screen.getByRole("button", { name: /Collapse message from/ }));
    expect(screen.queryByText("Full root message body")).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: /Expand message from/ }));
    expect(screen.getByText("Full root message body")).toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: "Load more messages" }));
    expect(
      await screen.findByRole("button", { name: /Expand message from Reviewer/ }),
    ).toBeInTheDocument();
    expect(screen.queryByText("Reviewed-by: Reviewer")).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: /Expand message from Reviewer/ }));
    expect(await screen.findByText("Reviewed-by: Reviewer")).toBeInTheDocument();
    expect(screen.getByText("2026-08-15 13:00 UTC")).toBeInTheDocument();

    const detailRequests = fetchMock.mock.calls.filter(([input]) =>
      String(input).startsWith("/api/v1/messages/"),
    );
    expect(detailRequests).toHaveLength(0);
  });

  it("expands levels zero and one by default and provides expand-all controls", async () => {
    const message = (
      messageId: string,
      inReplyToMessageId: string | null,
      body: string,
    ): MessageDetail => ({
      messageId,
      rootMessageId: thread.rootMessageId,
      inReplyToMessageId,
      referenceMessageIds: inReplyToMessageId ? [inReplyToMessageId] : [],
      availability: "available",
      subject: messageId,
      author: messageId,
      to: [],
      cc: [],
      sentAt: thread.startedAt,
      body,
      patch: null,
      mailingLists: thread.mailingLists,
      subsystems: thread.subsystems,
      loreUrl: `https://lore.kernel.org/r/${messageId}`,
    });
    const messages: ThreadMessagesResponse = {
      rootMessageId: thread.rootMessageId,
      items: [
        message("root@example.com", null, "Level zero body"),
        message("child@example.com", "root@example.com", "Level one body"),
        message("grandchild@example.com", "child@example.com", "Level two body"),
      ],
      pagination: { previousCursor: null, nextCursor: null },
    };
    const fetchMock = vi.fn((input: RequestInfo | URL) => {
      const url = String(input);
      if (url.endsWith("/patch-lineages")) {
        return Promise.resolve(jsonResponse({ items: [] }));
      }
      if (url.includes("/messages?")) {
        return Promise.resolve(jsonResponse(messages));
      }
      if (url.startsWith("/api/v1/threads/")) {
        return Promise.resolve(jsonResponse(thread));
      }
      return Promise.resolve(jsonResponse({ reason: "Not found" }, 404));
    });
    vi.stubGlobal("fetch", fetchMock);
    window.location.hash = `#/thread?${new URLSearchParams({ root: thread.rootMessageId })}`;

    render(() => (
      <HashRouter preload={false}>
        <Route path="/thread" component={ThreadPage} />
      </HashRouter>
    ));

    expect(await screen.findByText("Level zero body")).toBeInTheDocument();
    expect(screen.getByText("Level one body")).toBeInTheDocument();
    expect(screen.queryByText("Level two body")).not.toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: "Expand all" }));
    expect(screen.getByText("Level two body")).toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: "Collapse all" }));
    expect(screen.queryByText("Level zero body")).not.toBeInTheDocument();
    expect(screen.queryByText("Level one body")).not.toBeInTheDocument();
    expect(screen.queryByText("Level two body")).not.toBeInTheDocument();
  });

  it("replaces thread details with a skeleton while switching versions", async () => {
    const nextRootMessageID = "v1@example.com";
    const nextThread: ThreadDetail = {
      ...thread,
      rootMessageId: nextRootMessageID,
      subject: "[PATCH] net: repair the packet path v1",
    };
    let resolveNextThread!: (response: Response) => void;
    let resolveNextMessages!: (response: Response) => void;
    const nextThreadResponse = new Promise<Response>((resolve) => {
      resolveNextThread = resolve;
    });
    const nextMessagesResponse = new Promise<Response>((resolve) => {
      resolveNextMessages = resolve;
    });
    const emptyMessages = (rootMessageId: string): ThreadMessagesResponse => ({
      rootMessageId,
      items: [],
      pagination: { previousCursor: null, nextCursor: null },
    });
    const encodedNextRoot = encodeURIComponent(nextRootMessageID);
    const fetchMock = vi.fn((input: RequestInfo | URL) => {
      const url = String(input);
      if (url.endsWith("/patch-lineages")) {
        return Promise.resolve(jsonResponse({ items: [] }));
      }
      if (url.includes("/messages?")) {
        return url.includes(encodedNextRoot)
          ? nextMessagesResponse
          : Promise.resolve(jsonResponse(emptyMessages(thread.rootMessageId)));
      }
      if (url.includes(encodedNextRoot)) {
        return nextThreadResponse;
      }
      if (url.startsWith("/api/v1/threads/")) {
        return Promise.resolve(jsonResponse(thread));
      }
      return Promise.resolve(jsonResponse({ reason: "Not found" }, 404));
    });
    vi.stubGlobal("fetch", fetchMock);
    window.location.hash = `#/thread?${new URLSearchParams({ root: thread.rootMessageId })}`;

    render(() => (
      <HashRouter preload={false}>
        <Route path="/thread" component={ThreadPage} />
      </HashRouter>
    ));

    expect(
      await screen.findByRole("heading", { name: "[PATCH] net: repair the packet path" }),
    ).toBeInTheDocument();
    expect(screen.getByText("This thread has no messages.")).toBeInTheDocument();

    window.location.hash = `#/thread?${new URLSearchParams({ root: nextRootMessageID })}`;

    expect(await screen.findByLabelText("Loading thread")).toBeInTheDocument();
    expect(
      screen.queryByRole("heading", { name: "[PATCH] net: repair the packet path" }),
    ).not.toBeInTheDocument();
    expect(screen.queryByText("This thread has no messages.")).not.toBeInTheDocument();

    resolveNextThread(jsonResponse(nextThread));
    resolveNextMessages(jsonResponse(emptyMessages(nextRootMessageID)));

    expect(
      await screen.findByRole("heading", { name: "[PATCH] net: repair the packet path v1" }),
    ).toBeInTheDocument();
  });

  it.each([
    {
      name: "an empty lineage response",
      lineageResponse: () => jsonResponse({ items: [] }),
      expectedText: "This thread is not part of a patch lineage.",
    },
    {
      name: "an unavailable lineage response",
      lineageResponse: () => jsonResponse({ reason: "Unavailable" }, 503),
      expectedText: "Patch lineage data is unavailable.",
    },
  ])("renders $name without waiting for the thread", async ({
    lineageResponse,
    expectedText,
  }) => {
    let resolveThread!: (response: Response) => void;
    let resolveMessages!: (response: Response) => void;
    const threadResponse = new Promise<Response>((resolve) => {
      resolveThread = resolve;
    });
    const messagesResponse = new Promise<Response>((resolve) => {
      resolveMessages = resolve;
    });
    const fetchMock = vi.fn((input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes("/messages?")) {
        return messagesResponse;
      }
      if (url.endsWith("/patch-lineages")) {
        return Promise.resolve(lineageResponse());
      }
      if (url.startsWith("/api/v1/threads/")) {
        return threadResponse;
      }
      return Promise.resolve(jsonResponse({ reason: "Not found" }, 404));
    });
    vi.stubGlobal("fetch", fetchMock);
    window.location.hash = `#/thread?${new URLSearchParams({ root: thread.rootMessageId })}`;

    render(() => (
      <HashRouter preload={false}>
        <Route path="/thread" component={ThreadPage} />
      </HashRouter>
    ));

    expect(await screen.findByText(expectedText)).toBeInTheDocument();
    expect(screen.getByLabelText("Loading thread")).toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledTimes(3);

    resolveThread(jsonResponse(thread));
    resolveMessages(
      jsonResponse({
        rootMessageId: thread.rootMessageId,
        items: [],
        pagination: { previousCursor: null, nextCursor: null },
      } satisfies ThreadMessagesResponse),
    );

    expect(
      await screen.findByRole("heading", { name: "[PATCH] net: repair the packet path" }),
    ).toBeInTheDocument();
  });
});
