import { describe, expect, it } from "vitest";
import { buildThreadTree, mergeMessages } from "./threadTree";
import type { MessageDetail } from "./types";

function message(
  messageId: string,
  inReplyToMessageId: string | null = null,
  referenceMessageIds: string[] = [],
): MessageDetail {
  return {
    messageId,
    rootMessageId: "root",
    inReplyToMessageId,
    referenceMessageIds,
    availability: "available",
    subject: messageId,
    author: "Developer <developer@example.com>",
    to: [],
    cc: [],
    sentAt: "2026-08-15T12:00:00Z",
    body: messageId,
    patch: null,
    mailingLists: [],
    subsystems: [],
    loreUrl: "https://lore.kernel.org/r/" + messageId,
  };
}

describe("buildThreadTree", () => {
  it("treats the canonical promoted root as parentless", () => {
    const missingParent = "missing@example.com";
    const promotedRoot = message("promoted@example.com", missingParent, [missingParent]);
    promotedRoot.rootMessageId = promotedRoot.messageId;

    const roots = buildThreadTree([
      message("other@example.com"),
      promotedRoot,
    ]);

    expect(roots.map((node) => node.message.messageId)).toEqual([
      "other@example.com",
      "promoted@example.com",
    ]);
  });

  it("builds nested replies and preserves sibling input order", () => {
    const tree = buildThreadTree([
      message("root"),
      message("reply-b", "root"),
      message("reply-a", "root"),
      message("nested", "reply-b"),
    ]);

    expect(tree.map((node) => node.message.messageId)).toEqual(["root"]);
    expect(tree[0]?.children.map((node) => node.message.messageId)).toEqual([
      "reply-b",
      "reply-a",
    ]);
    expect(tree[0]?.children[0]?.children[0]?.message.messageId).toBe("nested");
  });

  it("keeps unrelated and unresolved messages as roots", () => {
    const tree = buildThreadTree([
      message("root-a"),
      message("orphan", "not-loaded"),
      message("root-b"),
    ]);

    expect(tree.map((node) => node.message.messageId)).toEqual([
      "root-a",
      "orphan",
      "root-b",
    ]);
  });

  it("falls back to the nearest loaded reference", () => {
    const tree = buildThreadTree([
      message("root"),
      message("reply", "missing-parent", ["root", "missing-parent"]),
    ]);

    expect(tree[0]?.children[0]?.message.messageId).toBe("reply");
  });

  it("breaks malformed parent cycles", () => {
    const tree = buildThreadTree([message("a", "b"), message("b", "a")]);

    expect(tree).toHaveLength(1);
    expect(tree[0]?.message.messageId).toBe("b");
    expect(tree[0]?.children[0]?.message.messageId).toBe("a");
  });

  it("reparents an orphan after another page supplies its parent", () => {
    const firstPage = [message("root"), message("child", "late-parent")];
    const merged = mergeMessages(firstPage, [message("late-parent", "root"), message("child")]);
    const tree = buildThreadTree(merged);

    expect(merged).toHaveLength(3);
    expect(tree[0]?.children[0]?.message.messageId).toBe("late-parent");
    expect(tree[0]?.children[0]?.children[0]?.message.messageId).toBe("child");
  });
});
