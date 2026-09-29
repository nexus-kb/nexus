import { A, useSearchParams } from "@solidjs/router";
import {
  For,
  Match,
  Show,
  Switch,
  createEffect,
  createMemo,
  createResource,
  createSignal,
} from "solid-js";
import {
  getThread,
  getThreadMessages,
  getThreadPatchLineages,
  threadRoute,
} from "../api";
import { absoluteDate, displayAuthor, displaySubject, plural } from "../format";
import { MessageBody } from "../messageBody";
import { buildThreadTree, mergeMessages, type ThreadTreeNode } from "../threadTree";
import type { MainlineMatch, MessageDetail, PatchLineageRevision } from "../types";

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : "The request failed";
}

function firstParameter(value: string | string[] | undefined): string | undefined {
  return Array.isArray(value) ? value[0] : value;
}

function mailboxList(mailboxes: MessageDetail["to"]): string {
  return mailboxes
    .map((mailbox) => (mailbox.name ? `${mailbox.name} <${mailbox.email}>` : mailbox.email))
    .join(", ");
}

function revisionLabel(
  phase: "RFC" | "PATCH",
  revision: number,
  revisionExplicit: boolean,
  isResend: boolean,
): string {
  const version = revisionExplicit || revision > 1 ? " v" + revision : "";
  const resend = isResend ? " RESEND" : "";
  return phase + version + resend;
}

export function mainlineLabel(mainline: MainlineMatch): string {
  switch (mainline.state) {
    case "not_checked": return "Not checked";
    case "no_match": return "No mainline match found";
    case "partial": return `Partially merged · ${mainline.matchedParts}/${mainline.totalParts} patches`;
    case "merged_unreleased": return "Merged in mainline · unreleased";
    case "merged_released": return `Merged · first fully included in ${mainline.firstRelease}`;
  }
}

export function laterMergedRevision(
  revisions: readonly PatchLineageRevision[],
  viewed: PatchLineageRevision,
): PatchLineageRevision | undefined {
  return revisions.find((candidate) => candidate.revision > viewed.revision &&
    (candidate.mainline?.state === "merged_released" || candidate.mainline?.state === "merged_unreleased"));
}

function MainlineStatus(props: { revision: PatchLineageRevision }) {
  return <Show when={props.revision.mainline}>{(mainline) => <div class="mainline-status">
    <div class={`mainline-state state-${mainline().state}`}>v{props.revision.revision} · {mainlineLabel(mainline())}</div>
    <Show when={mainline().checkedAt || mainline().coverageStart || mainline().indexedTip}>
      <div class="mainline-coverage">
        <Show when={mainline().checkedAt}>Checked {absoluteDate(mainline().checkedAt)} · </Show>
        <Show when={mainline().coverageStart && mainline().indexedTip}>History after <code>{mainline().coverageStart}</code> through <code title={mainline().indexedTip!}>{mainline().indexedTip?.slice(0, 12)}</code></Show>
      </div>
    </Show>
    <Show when={mainline().patches.length}><div class="mainline-patches"><For each={mainline().patches}>{(patch) => <details>
      <summary>Patch {patch.partIndex}: {patch.subject} · {plural(patch.commits.length, "commit")}</summary>
      <Show when={patch.commits.length} fallback={<p class="mainline-coverage">{mainline().state === "not_checked" ? "Matching is not complete." : "No accepted mainline match within indexed history."}</p>}>
        <ul><For each={patch.commits}>{(commit) => <li><A class="commit-hash" title={commit.oid} href={`/commits/${commit.oid}`}>{commit.oid.slice(0, 12)}</A> {commit.subject}<span class="commit-evidence"> · {commit.firstRelease ? `first included in ${commit.firstRelease}` : "unreleased"} · {commit.matchKind === "submission" ? "source submission (link + patch ID)" : "equivalent content (patch ID; revision not confirmed)"}</span></li>}</For></ul>
      </Show>
    </details>}</For></div></Show>
  </div>}</Show>;
}

function expandedMessageIDs(
  nodes: readonly ThreadTreeNode[],
  maximumDepth: number,
): ReadonlySet<string> {
  const messageIDs = new Set<string>();

  const visit = (node: ThreadTreeNode, depth: number) => {
    if (depth > maximumDepth) {
      return;
    }

    if (node.message.availability === "available") {
      messageIDs.add(node.message.messageId);
    }

    for (const child of node.children) {
      visit(child, depth + 1);
    }
  };

  for (const node of nodes) {
    visit(node, 0);
  }

  return messageIDs;
}

interface MessageNodeProps {
  node: ThreadTreeNode;
  depth: number;
  expanded: (messageID: string) => boolean;
  toggle: (message: MessageDetail) => void;
}

function MessageNode(props: MessageNodeProps) {
  const message = () => props.node.message;
  const isExpanded = () => props.expanded(message().messageId);

  return (
    <li
      class="message-node"
      classList={{ "message-nested": props.depth > 0 && props.depth <= 10 }}
    >
      <article
        classList={{ "message-missing": message().availability === "missing" }}
        data-message-id={message().messageId}
      >
        <Show
          when={message().availability === "available"}
          fallback={
            <div class="message-summary">
              <div class="message-header">
                <span class="message-author">{displayAuthor(message().author)}</span>
              </div>
              <Show when={message().subject}>
                <div class="message-subject">{displaySubject(message().subject)}</div>
              </Show>
            </div>
          }
        >
          <button
            aria-expanded={isExpanded()}
            aria-label={`${isExpanded() ? "Collapse" : "Expand"} message from ${displayAuthor(message().author)}: ${displaySubject(message().subject)}`}
            class="message-summary"
            onClick={() => props.toggle(message())}
            type="button"
          >
            <div class="message-header">
              <span class="message-author">{displayAuthor(message().author)}</span>
              <Show when={isExpanded()}>
                <span>{absoluteDate(message().sentAt)}</span>
              </Show>
            </div>
            <Show when={message().subject}>
              <div class="message-subject">{displaySubject(message().subject)}</div>
            </Show>
          </button>
        </Show>

        <Show
          when={message().availability === "available" && isExpanded()}
        >
          <div class="message-recipients">
            <Show when={message().to.length > 0}>
              <div>
                <span>to</span> {mailboxList(message().to)}
              </div>
            </Show>
            <Show when={message().cc.length > 0}>
              <div>
                <span>cc</span> {mailboxList(message().cc)}
              </div>
            </Show>
          </div>
          <MessageBody body={message().body || "(empty message body)"} />
          <a class="lore-link" href={message().loreUrl} rel="noreferrer" target="_blank">
            View on lore.kernel.org
          </a>
        </Show>
      </article>

      <Show when={props.node.children.length > 0}>
        <ol class="message-children">
          <For each={props.node.children}>
            {(child) => <MessageNode {...props} depth={props.depth + 1} node={child} />}
          </For>
        </ol>
      </Show>
    </li>
  );
}

export function ThreadPage() {
  const [searchParams] = useSearchParams();
  const rootMessageID = () => firstParameter(searchParams.root)?.trim() || undefined;
  const [pageData, { refetch }] = createResource(rootMessageID, async (root) => {
    const [thread, messagePage] = await Promise.all([
      getThread(root),
      getThreadMessages(root),
    ]);
    return { thread, messagePage };
  });
  const [lineagePage, { refetch: refetchLineage }] = createResource(
    rootMessageID,
    async (root) => getThreadPatchLineages(root),
  );
  const [messages, setMessages] = createSignal<MessageDetail[]>([]);
  const [nextCursor, setNextCursor] = createSignal<string | null>(null);
  const [loadingMore, setLoadingMore] = createSignal(false);
  const [loadMoreError, setLoadMoreError] = createSignal<string>();
  const [expandedIDs, setExpandedIDs] = createSignal<ReadonlySet<string>>(new Set());

  createEffect(() => {
    const value = pageData();
    if (value) {
      setMessages(value.messagePage.items);
      setNextCursor(value.messagePage.pagination.nextCursor);
      setLoadMoreError(undefined);
      setExpandedIDs(expandedMessageIDs(buildThreadTree(value.messagePage.items), 1));
    }
  });

  const tree = createMemo(() => buildThreadTree(messages()));
  const isExpanded = (messageID: string) => expandedIDs().has(messageID);
  const toggleMessage = (message: MessageDetail) => {
    const messageID = message.messageId;
    setExpandedIDs((current) => {
      const next = new Set(current);
      if (next.has(messageID)) {
        next.delete(messageID);
      } else {
        next.add(messageID);
      }
      return next;
    });
  };
  const collapseAll = () => setExpandedIDs(new Set<string>());
  const expandAll = () => {
    setExpandedIDs(
      new Set(
        messages()
          .filter((message) => message.availability === "available")
          .map((message) => message.messageId),
      ),
    );
  };

  const loadMore = async () => {
    const root = rootMessageID();
    const cursor = nextCursor();
    if (!root || !cursor || loadingMore()) {
      return;
    }

    setLoadingMore(true);
    setLoadMoreError(undefined);
    try {
      const page = await getThreadMessages(root, cursor);
      setMessages((current) => mergeMessages(current, page.items));
      setNextCursor(page.pagination.nextCursor);
    } catch (error) {
      setLoadMoreError(errorMessage(error));
    } finally {
      setLoadingMore(false);
    }
  };

  return (
    <section aria-labelledby="thread-heading">
      <Show when={!rootMessageID()}>
        <div class="error-state" role="alert">
          <h1 id="thread-heading">No thread selected</h1>
          <p>The thread URL does not contain a root message ID.</p>
        </div>
      </Show>

      <Show when={rootMessageID()}>
        <Switch>
          <Match when={lineagePage.loading && !lineagePage.latest}>
            <section
              class="lineage-summary lineage-summary-skeleton"
              aria-busy="true"
              aria-label="Patch lineage"
            >
              <div class="lineage-eyebrow">Patch lineage</div>
              <div class="lineage-skeleton-body" aria-hidden="true">
                <span />
                <span />
              </div>
            </section>
          </Match>

          <Match when={lineagePage.error}>
            <section class="lineage-summary" aria-label="Patch lineage">
              <div class="lineage-eyebrow">Patch lineage</div>
              <div class="lineage-empty" role="status">
                <span>Patch lineage data is unavailable.</span>
                <button onClick={() => void refetchLineage()} type="button">
                  Retry
                </button>
              </div>
            </section>
          </Match>

          <Match when={lineagePage()?.items.length === 0}>
            <section class="lineage-summary" aria-label="Patch lineage">
              <div class="lineage-eyebrow">Patch lineage</div>
              <p class="lineage-empty">This thread is not part of a patch lineage.</p>
            </section>
          </Match>

          <Match when={lineagePage()}>
            {(page) => (
              <For each={page().items}>
                {(lineage) => (
                  <section class="lineage-summary" aria-label="Patch lineage">
                    <div class="lineage-eyebrow">
                      Patch lineage · {plural(lineage.revisions.length, "version")}
                    </div>
                    <h1>{lineage.subject}</h1>
                    <nav class="lineage-revisions" aria-label="Patch versions">
                      <For each={lineage.revisions}>
                        {(revision) => (
                          <A
                            class="lineage-revision"
                            classList={{
                              active: revision.rootMessageId === rootMessageID(),
                            }}
                            href={threadRoute(revision.rootMessageId)}
                          >
                            {revisionLabel(
                              revision.phase,
                              revision.revision,
                              revision.revisionExplicit,
                              revision.isResend,
                            )}
                          </A>
                        )}
                      </For>
                    </nav>
                    <For each={lineage.revisions.filter((revision) => revision.rootMessageId === rootMessageID())}>
                      {(revision) => <>
                        <Show when={laterMergedRevision(lineage.revisions, revision)}>
                          {(later) => <div class="later-merged">Later revision v{later().revision} merged</div>}
                        </Show>
                        <MainlineStatus revision={revision} />
                      </>}
                    </For>
                  </section>
                )}
              </For>
            )}
          </Match>
        </Switch>
      </Show>

      <Switch>
        <Match when={rootMessageID() && pageData.loading}>
          <div class="thread-page-skeleton" aria-busy="true" aria-label="Loading thread">
            <span />
            <span />
            <span />
          </div>
        </Match>

        <Match when={pageData.error}>
          <div class="error-state" role="alert">
            <h1 id="thread-heading">Could not load thread</h1>
            <p>{errorMessage(pageData.error)}</p>
            <button onClick={() => void refetch()} type="button">
              Retry
            </button>
          </div>
        </Match>

        <Match when={pageData()}>
          {(data) => (
            <>
              <header class="thread-heading">
                <div class="thread-heading-row">
                  <h1 id="thread-heading">{displaySubject(data().thread.subject)}</h1>
                  <div class="thread-actions" role="group" aria-label="Message display">
                    <button onClick={collapseAll} type="button">
                      Collapse all
                    </button>
                    <button onClick={expandAll} type="button">
                      Expand all
                    </button>
                  </div>
                </div>
                <div class="thread-detail-meta">
                  <span>{displayAuthor(data().thread.author)}</span>
                  <Show when={data().thread.startedAt}>
                    <span>created {absoluteDate(data().thread.startedAt)}</span>
                  </Show>
                  <span>updated {absoluteDate(data().thread.lastActivityAt)}</span>
                  <span>{plural(data().thread.messageCount, "message")}</span>
                </div>
                <div class="thread-tags">
                  <For each={data().thread.mailingLists}>
                    {(mailingList) => (
                      <span class="meta-tag" title={mailingList.name}>
                        {mailingList.archiveGroup}
                      </span>
                    )}
                  </For>
                  <For each={data().thread.subsystems}>
                    {(subsystem) => <span class="meta-tag">{subsystem.name}</span>}
                  </For>
                  <Show when={data().thread.missingMessageCount > 0}>
                    <span class="warning-text">
                      {plural(data().thread.missingMessageCount, "missing message")}
                    </span>
                  </Show>
                </div>
              </header>

              <Show
                when={tree().length > 0}
                fallback={<p class="empty-state">This thread has no messages.</p>}
              >
                <ol class="message-tree" aria-live="polite">
                  <For each={tree()}>
                    {(node) => (
                      <MessageNode
                        depth={0}
                        expanded={isExpanded}
                        node={node}
                        toggle={toggleMessage}
                      />
                    )}
                  </For>
                </ol>
              </Show>

              <Show when={loadMoreError()}>
                {(message) => (
                  <div class="inline-error load-more-error" role="alert">
                    {message()}
                  </div>
                )}
              </Show>
              <Show when={nextCursor()}>
                <div class="load-more">
                  <button disabled={loadingMore()} onClick={() => void loadMore()} type="button">
                    {loadingMore() ? "Loading…" : "Load more messages"}
                  </button>
                </div>
              </Show>
            </>
          )}
        </Match>
      </Switch>
    </section>
  );
}
