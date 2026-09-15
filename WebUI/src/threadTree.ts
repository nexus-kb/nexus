import type { MessageDetail } from "./types";

export interface ThreadTreeNode {
  message: MessageDetail;
  children: ThreadTreeNode[];
}

function candidateParentIDs(message: MessageDetail): string[] {
  if (message.messageId === message.rootMessageId) {
    return [];
  }

  const references = [...message.referenceMessageIds].reverse();
  return message.inReplyToMessageId
    ? [message.inReplyToMessageId, ...references]
    : references;
}

function wouldCreateCycle(
  childID: string,
  parentID: string,
  parents: ReadonlyMap<string, string>,
): boolean {
  let current: string | undefined = parentID;
  const visited = new Set<string>();

  while (current) {
    if (current === childID || visited.has(current)) {
      return true;
    }

    visited.add(current);
    current = parents.get(current);
  }

  return false;
}

export function buildThreadTree(messages: readonly MessageDetail[]): ThreadTreeNode[] {
  const uniqueMessages = new Map<string, MessageDetail>();
  const order = new Map<string, number>();

  messages.forEach((message, index) => {
    if (!uniqueMessages.has(message.messageId)) {
      uniqueMessages.set(message.messageId, message);
      order.set(message.messageId, index);
    }
  });

  const parents = new Map<string, string>();

  for (const message of uniqueMessages.values()) {
    for (const candidateID of candidateParentIDs(message)) {
      if (
        candidateID !== message.messageId &&
        uniqueMessages.has(candidateID) &&
        !wouldCreateCycle(message.messageId, candidateID, parents)
      ) {
        parents.set(message.messageId, candidateID);
        break;
      }
    }
  }

  const nodes = new Map<string, ThreadTreeNode>();
  for (const message of uniqueMessages.values()) {
    nodes.set(message.messageId, { message, children: [] });
  }

  const roots: ThreadTreeNode[] = [];
  for (const [messageID, node] of nodes) {
    const parentID = parents.get(messageID);
    const parent = parentID ? nodes.get(parentID) : undefined;

    if (parent) {
      parent.children.push(node);
    } else {
      roots.push(node);
    }
  }

  const sortNodes = (values: ThreadTreeNode[]): void => {
    values.sort(
      (left, right) =>
        (order.get(left.message.messageId) ?? 0) -
        (order.get(right.message.messageId) ?? 0),
    );
    values.forEach((node) => sortNodes(node.children));
  };

  sortNodes(roots);
  return roots;
}

export function mergeMessages(
  current: readonly MessageDetail[],
  incoming: readonly MessageDetail[],
): MessageDetail[] {
  const merged = new Map(current.map((message) => [message.messageId, message]));

  for (const message of incoming) {
    if (!merged.has(message.messageId)) {
      merged.set(message.messageId, message);
    }
  }

  return [...merged.values()];
}
