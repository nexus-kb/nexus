import { HashRouter, Route } from "@solidjs/router";
import { cleanup, render, screen } from "@solidjs/testing-library";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CommitPage } from "./pages/CommitPage";
import { laterMergedRevision, mainlineLabel } from "./pages/ThreadPage";
import type { MainlineMatch, PatchLineageRevision } from "./types";

const match = (state: MainlineMatch["state"], overrides: Partial<MainlineMatch> = {}): MainlineMatch => ({
  state, checkedAt: null, coverageStart: null, indexedTip: null, totalParts: 5,
  matchedParts: 3, firstRelease: null, patches: [], ...overrides,
});

afterEach(() => { cleanup(); vi.unstubAllGlobals(); window.location.hash = ""; });

describe("mainline labels", () => {
  it("renders every agreed state without treating release candidates as releases", () => {
    expect(mainlineLabel(match("not_checked"))).toBe("Not checked");
    expect(mainlineLabel(match("no_match"))).toBe("No mainline match found");
    expect(mainlineLabel(match("partial"))).toBe("Partially merged · 3/5 patches");
    expect(mainlineLabel(match("merged_unreleased"))).toBe("Merged in mainline · unreleased");
    expect(mainlineLabel(match("merged_released", { firstRelease: "v6.19" }))).toBe("Merged · first fully included in v6.19");
  });

  it("labels a merged later revision without claiming the viewed revision was applied", () => {
    const base = { revision: 1, mainline: match("no_match") } as PatchLineageRevision;
    const later = { revision: 2, mainline: match("merged_released", { firstRelease: "v6.19" }) } as PatchLineageRevision;
    expect(laterMergedRevision([base, later], base)?.revision).toBe(2);
    expect(mainlineLabel(base.mainline!)).toBe("No mainline match found");
  });
});

describe("commit reverse lookup", () => {
  const show = (status = 200) => {
    window.location.hash = "#/commits/deadbeef";
    vi.stubGlobal("fetch", vi.fn(() => Promise.resolve(new Response(JSON.stringify(status === 200 ? {
      oid: "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef", subject: "net: fix packets", firstRelease: null,
      submissions: [
        { messageId: "v1@example.com", subject: "[PATCH v1] fix", rootMessageId: "v1@example.com", patchsetId: 1, lineageId: 9, revision: 1, partIndex: 1, matchKind: "submission" },
        { messageId: "v2@example.com", subject: "[PATCH v2] fix", rootMessageId: "v2@example.com", patchsetId: 2, lineageId: 9, revision: 2, partIndex: 1, matchKind: "equivalent" },
      ], references: ["external@example.com"],
    } : { reason: status === 409 ? "Prefix matches multiple commits" : "No commit found" }), { status, headers: { "Content-Type": "application/json" } }))));
    render(() => <HashRouter preload={false}><Route path="/commits/:commitID" component={CommitPage}/></HashRouter>);
  };

  it("shows metadata, multiple revisions, evidence, and unindexed lore references", async () => {
    show();
    expect(await screen.findByRole("heading", { name: "net: fix packets" })).toBeInTheDocument();
    expect(screen.getByText("Unreleased")).toBeInTheDocument();
    expect(screen.getByText(/source submission/)).toBeInTheDocument();
    expect(screen.getByText(/equivalent content/)).toBeInTheDocument();
    expect(screen.getByText("message not indexed")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "external@example.com" })).toHaveAttribute("href", expect.stringContaining("lore.kernel.org"));
  });

  it.each([[400, "Invalid commit hash"], [404, "Commit not found"], [409, "Ambiguous commit prefix"]])("handles %s responses", async (status, title) => {
    show(status as number);
    expect(await screen.findByRole("heading", { name: title as string })).toBeInTheDocument();
  });
});
