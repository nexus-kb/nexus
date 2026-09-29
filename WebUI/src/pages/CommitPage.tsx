import { A, useParams } from "@solidjs/router";
import { For, Match, Show, Switch, createResource } from "solid-js";
import { ApiError, getCommit, threadRoute } from "../api";

const loreURL = (messageID: string) => `https://lore.kernel.org/r/${encodeURIComponent(messageID)}`;

export function CommitPage() {
  const params = useParams();
  const commitID = () => params.commitID || "";
  const [commit, { refetch }] = createResource(commitID, (id) => getCommit(id));
  const errorTitle = () => {
    const error = commit.error;
    if (error instanceof ApiError && error.status === 404) return "Commit not found";
    if (error instanceof ApiError && error.status === 409) return "Ambiguous commit prefix";
    if (error instanceof ApiError && error.status === 400) return "Invalid commit hash";
    return "Could not look up commit";
  };

  return <section class="commit-page" aria-labelledby="commit-heading">
    <Switch>
      <Match when={commit.loading}><div class="thread-page-skeleton" aria-label="Loading commit" aria-busy="true"><span/><span/><span/></div></Match>
      <Match when={commit.error}><div class="error-state" role="alert"><h1 id="commit-heading">{errorTitle()}</h1><p>{commit.error instanceof Error ? commit.error.message : "The request failed"}</p><button type="button" onClick={() => void refetch()}>Retry</button></div></Match>
      <Match when={commit()}>{(item) => <>
        <header class="commit-heading"><div class="lineage-eyebrow">Mainline commit</div><h1 id="commit-heading">{item().subject}</h1><code class="commit-oid">{item().oid}</code><p>{item().firstRelease ? `First included in ${item().firstRelease}` : "Unreleased"}</p></header>
        <h2>Patch submissions</h2>
        <Show when={item().submissions.length} fallback={<p class="empty-state">No indexed source submissions.</p>}>
          <ol class="submission-list"><For each={item().submissions}>{(submission) => <li>
            <div><A href={threadRoute(submission.rootMessageId)}>{submission.subject}</A></div>
            <div class="commit-meta">v{submission.revision ?? "?"} · patch {submission.partIndex} · {submission.matchKind === "submission" ? "source submission (link + patch ID)" : "equivalent content (patch ID; revision not confirmed)"} · <a href={loreURL(submission.messageId)} target="_blank" rel="noreferrer">message on lore</a></div>
          </li>}</For></ol>
        </Show>
        <Show when={item().references.length}><h2>External references</h2><p>These links may refer to submissions or background discussion; they are not confirmed source matches.</p><ul class="reference-list"><For each={item().references}>{(reference) => <li><a href={loreURL(reference)} target="_blank" rel="noreferrer">{reference}</a> <span>message not indexed</span></li>}</For></ul></Show>
      </>}</Match>
    </Switch>
  </section>;
}
