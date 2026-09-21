import { A, useSearchParams } from "@solidjs/router";
import { For, Show, createEffect, createResource, createSignal } from "solid-js";
import { getMailingLists, getThreads, searchThreads, threadRoute } from "../api";
import { absoluteDate, displayAuthor, displaySubject, plural } from "../format";
import type {
  ThreadSearchResult,
  MailingListResponse,
  Pagination,
  PatchSeries,
  ThreadSummary,
} from "../types";

interface ListingPage {
  mode: "threads" | "search";
  threads: ThreadSummary[];
  results: ThreadSearchResult[];
  pagination: Pagination;
}

function firstParameter(value: string | string[] | undefined): string | undefined {
  return Array.isArray(value) ? value[0] : value;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : "The request failed";
}

function patchLabel(patchSeries: readonly PatchSeries[]): string | undefined {
  const patch = patchSeries[0];
  if (!patch) {
    return undefined;
  }

  const parts =
    patch.receivedParts === patch.totalParts
      ? `${patch.totalParts} parts`
      : `${patch.receivedParts}/${patch.totalParts} parts`;
  return `${patch.status} · ${parts}`;
}

function ThreadRow(props: {
  thread: ThreadSummary;
}) {
  const thread = () => props.thread;

  return (
    <li class="thread-row">
      <div class="thread-title-line">
        <A href={threadRoute(thread().rootMessageId)}>{displaySubject(thread().subject)}</A>
        {/*<span class={`kind-badge kind-${thread().kind}`}>
          {thread().kind === "patch-series" ? "patch series" : "discussion"}
        </span>*/}
      </div>
      <div class="thread-meta">
        <span>{displayAuthor(thread().author)}</span>
        <Show when={thread().startedAt}>
          {(startedAt) => (
            <span>created {absoluteDate(startedAt())}</span>
          )}
        </Show>
        <span>updated {absoluteDate(thread().lastActivityAt)}</span>
        <span>{plural(thread().messageCount, "message")}</span>
        <For each={thread().mailingLists}>
          {(mailingList) => (
            <span class="meta-tag" title={mailingList.name}>
              {mailingList.archiveGroup}
            </span>
          )}
        </For>
        <For each={thread().subsystems}>
          {(subsystem) => <span class="meta-tag">{subsystem.name}</span>}
        </For>
      </div>
    </li>
  );
}

export function ThreadListPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [searchText, setSearchText] = createSignal("");

  createEffect(() => {
    setSearchText(firstParameter(searchParams.q) || "");
  });

  const [mailingLists, { refetch: refetchMailingLists }] =
    createResource<MailingListResponse>(() => getMailingLists());
  const [listing, { refetch: refetchListing }] = createResource<
    ListingPage,
    { mailingList?: string; q?: string; cursor?: string }
  >(
    () => ({
      mailingList: firstParameter(searchParams.mailingList),
      q: firstParameter(searchParams.q),
      cursor: firstParameter(searchParams.cursor),
    }),
    async (parameters) => {
      if (parameters.q) {
        const page = await searchThreads(parameters);
        return {
          mode: "search",
          threads: [],
          results: page.items,
          pagination: page.pagination,
        };
      }

      const page = await getThreads(parameters);
      return {
        mode: "threads",
        threads: page.items,
        results: [],
        pagination: page.pagination,
      };
    },
  );
  const visibleListing = () => (listing.error ? undefined : listing());

  const selectMailingList = (event: Event) => {
    const value = (event.currentTarget as HTMLSelectElement).value;
    setSearchParams({
      mailingList: value || undefined,
      cursor: undefined,
    });
  };

  const moveToCursor = (cursor: string | null) => {
    if (cursor) {
      setSearchParams({ cursor });
    }
  };

  const submitSearch = (event: SubmitEvent) => {
    event.preventDefault();
    const q = searchText().trim();
    setSearchParams({
      q: q || undefined,
      cursor: undefined,
    });
  };

  const clearSearch = () => {
    setSearchText("");
    setSearchParams({
      q: undefined,
      cursor: undefined,
    });
  };

  return (
    <section aria-label="Threads">
      <div class="page-heading">
        <select
          class="filter-control"
          aria-label="Filter by mailing list"
          disabled={mailingLists.loading}
          onChange={selectMailingList}
          value={firstParameter(searchParams.mailingList) || ""}
        >
          <option value="">All lists</option>
          <For each={mailingLists()?.items}>
            {(mailingList) => (
              <option value={mailingList.archiveGroup}>{mailingList.archiveGroup}</option>
            )}
          </For>
        </select>
        <form class="search-form" onSubmit={submitSearch}>
          <input
            aria-label="Search threads"
            maxlength={512}
            onInput={(event) => setSearchText(event.currentTarget.value)}
            placeholder='Text, subject:"phrase", author:"name", date:YYYY-MM-DD..YYYY-MM-DD'
            type="search"
            value={searchText()}
          />
          <button type="submit">Search</button>
          <button
            disabled={!searchText() && !firstParameter(searchParams.q)}
            onClick={clearSearch}
            type="button"
          >
            Clear
          </button>
        </form>
      </div>

      <Show when={mailingLists.error}>
        <div class="inline-notice" role="status">
          Mailing-list filters are unavailable. {errorMessage(mailingLists.error)}{" "}
          <button class="text-button" onClick={() => void refetchMailingLists()} type="button">
            Retry
          </button>
        </div>
      </Show>

      <Show when={listing.error}>
        <div class="error-state" role="alert">
          <h2>Could not load results</h2>
          <p>{errorMessage(listing.error)}</p>
          <button onClick={() => void refetchListing()} type="button">
            Retry
          </button>
        </div>
      </Show>

      <div aria-busy={listing.loading} aria-live="polite">
        <Show
          when={!listing.loading}
          fallback={
            <ol class="thread-list" aria-label="Loading threads">
              <For each={Array.from({ length: 8 })}>
                {() => (
                  <li class="thread-row thread-skeleton">
                    <span />
                    <span />
                  </li>
                )}
              </For>
            </ol>
          }
        >
          <Show when={visibleListing()}>
            {(page) => (
              <Show
                when={
                  page().mode === "search" ? page().results.length > 0 : page().threads.length > 0
                }
                fallback={
                  <p class="empty-state">
                    {page().mode === "search"
                      ? "No threads match this search."
                      : "No threads match these filters."}
                  </p>
                }
              >
                <ol class="thread-list">
                  <Show
                    when={page().mode === "search"}
                    fallback={
                      <For each={page().threads}>
                        {(thread) => <ThreadRow thread={thread} />}
                      </For>
                    }
                  >
                    <For each={page().results}>
                      {(result) => <ThreadRow thread={result} />}
                    </For>
                  </Show>
                </ol>
              </Show>
            )}
          </Show>
        </Show>
      </div>

      <Show when={visibleListing()}>
        {(page) => (
          <nav class="pagination" aria-label="Thread pages">
            <button
              disabled={!page().pagination.previousCursor || listing.loading}
              onClick={() => moveToCursor(page().pagination.previousCursor)}
              type="button"
            >
              Previous
            </button>
            <button
              disabled={!page().pagination.nextCursor || listing.loading}
              onClick={() => moveToCursor(page().pagination.nextCursor)}
              type="button"
            >
              Next
            </button>
          </nav>
        )}
      </Show>
    </section>
  );
}
