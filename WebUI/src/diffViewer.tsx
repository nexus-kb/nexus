import { For, Show, createMemo, createResource, createSignal } from "solid-js";
import type {
  BundledLanguage,
  LanguageRegistration,
  ShikiTransformer,
} from "shiki";

export interface DiffFile {
  label: string;
  language: BundledLanguage;
  text: string;
}

const LANGUAGE_BY_FILENAME: Record<string, BundledLanguage> = {
  "cmakelists.txt": "cmake",
  dockerfile: "docker",
  kconfig: "makefile",
  makefile: "makefile",
};

const LANGUAGE_BY_EXTENSION: Record<string, BundledLanguage> = {
  asm: "asm",
  bash: "bash",
  c: "c",
  cc: "cpp",
  cjs: "javascript",
  cpp: "cpp",
  cs: "csharp",
  css: "css",
  cts: "typescript",
  cxx: "cpp",
  dts: "c",
  dtsi: "c",
  el: "lisp",
  erl: "erlang",
  ex: "elixir",
  exs: "elixir",
  fs: "fsharp",
  go: "go",
  graphql: "graphql",
  h: "c",
  hh: "cpp",
  hpp: "cpp",
  hrl: "erlang",
  hs: "haskell",
  htm: "html",
  html: "html",
  hxx: "cpp",
  java: "java",
  js: "javascript",
  json: "json",
  jsonc: "jsonc",
  jsx: "jsx",
  kt: "kotlin",
  kts: "kotlin",
  lua: "lua",
  md: "markdown",
  mjs: "javascript",
  ml: "ocaml",
  mli: "ocaml",
  mm: "objective-cpp",
  mts: "typescript",
  nix: "nix",
  php: "php",
  pl: "perl",
  pm: "perl",
  proto: "proto",
  py: "python",
  rb: "ruby",
  rs: "rust",
  s: "asm",
  scss: "scss",
  sh: "bash",
  sql: "sql",
  swift: "swift",
  svelte: "svelte",
  toml: "toml",
  ts: "typescript",
  tsx: "tsx",
  vue: "vue",
  xml: "xml",
  yaml: "yaml",
  yml: "yaml",
  zig: "zig",
  zsh: "zsh",
};

type LanguageLoader = () => Promise<LanguageRegistration[]>;

const LANGUAGE_LOADERS: Partial<Record<BundledLanguage, LanguageLoader>> = {
  asm: () => import("@shikijs/langs/asm").then((module) => module.default),
  bash: () => import("@shikijs/langs/bash").then((module) => module.default),
  c: () => import("@shikijs/langs/c").then((module) => module.default),
  cmake: () => import("@shikijs/langs/cmake").then((module) => module.default),
  cpp: () => import("@shikijs/langs/cpp").then((module) => module.default),
  csharp: () => import("@shikijs/langs/csharp").then((module) => module.default),
  css: () => import("@shikijs/langs/css").then((module) => module.default),
  docker: () => import("@shikijs/langs/docker").then((module) => module.default),
  elixir: () => import("@shikijs/langs/elixir").then((module) => module.default),
  erlang: () => import("@shikijs/langs/erlang").then((module) => module.default),
  fsharp: () => import("@shikijs/langs/fsharp").then((module) => module.default),
  go: () => import("@shikijs/langs/go").then((module) => module.default),
  graphql: () => import("@shikijs/langs/graphql").then((module) => module.default),
  haskell: () => import("@shikijs/langs/haskell").then((module) => module.default),
  html: () => import("@shikijs/langs/html").then((module) => module.default),
  java: () => import("@shikijs/langs/java").then((module) => module.default),
  javascript: () => import("@shikijs/langs/javascript").then((module) => module.default),
  json: () => import("@shikijs/langs/json").then((module) => module.default),
  jsonc: () => import("@shikijs/langs/jsonc").then((module) => module.default),
  jsx: () => import("@shikijs/langs/jsx").then((module) => module.default),
  kotlin: () => import("@shikijs/langs/kotlin").then((module) => module.default),
  lisp: () => import("@shikijs/langs/lisp").then((module) => module.default),
  lua: () => import("@shikijs/langs/lua").then((module) => module.default),
  makefile: () => import("@shikijs/langs/makefile").then((module) => module.default),
  markdown: () => import("@shikijs/langs/markdown").then((module) => module.default),
  nix: () => import("@shikijs/langs/nix").then((module) => module.default),
  "objective-cpp": () => import("@shikijs/langs/objective-cpp").then((module) => module.default),
  ocaml: () => import("@shikijs/langs/ocaml").then((module) => module.default),
  perl: () => import("@shikijs/langs/perl").then((module) => module.default),
  php: () => import("@shikijs/langs/php").then((module) => module.default),
  proto: () => import("@shikijs/langs/proto").then((module) => module.default),
  python: () => import("@shikijs/langs/python").then((module) => module.default),
  ruby: () => import("@shikijs/langs/ruby").then((module) => module.default),
  rust: () => import("@shikijs/langs/rust").then((module) => module.default),
  scss: () => import("@shikijs/langs/scss").then((module) => module.default),
  sql: () => import("@shikijs/langs/sql").then((module) => module.default),
  svelte: () => import("@shikijs/langs/svelte").then((module) => module.default),
  swift: () => import("@shikijs/langs/swift").then((module) => module.default),
  toml: () => import("@shikijs/langs/toml").then((module) => module.default),
  tsx: () => import("@shikijs/langs/tsx").then((module) => module.default),
  typescript: () => import("@shikijs/langs/typescript").then((module) => module.default),
  vue: () => import("@shikijs/langs/vue").then((module) => module.default),
  xml: () => import("@shikijs/langs/xml").then((module) => module.default),
  yaml: () => import("@shikijs/langs/yaml").then((module) => module.default),
  zig: () => import("@shikijs/langs/zig").then((module) => module.default),
  zsh: () => import("@shikijs/langs/zsh").then((module) => module.default),
};

export function languageForPath(path: string): BundledLanguage {
  const unquoted = path.replace(/^"(.*)"$/, "$1");
  const filename = unquoted.split("/").at(-1)?.toLowerCase() ?? "";
  const filenameLanguage = LANGUAGE_BY_FILENAME[filename];

  if (filenameLanguage) {
    return filenameLanguage;
  }

  const extensionIndex = filename.lastIndexOf(".");
  if (extensionIndex === -1) {
    return "diff";
  }

  return LANGUAGE_BY_EXTENSION[filename.slice(extensionIndex + 1)] ?? "diff";
}

export function splitDiffFiles(diff: string): DiffFile[] {
  const matches = [...diff.matchAll(/^diff --git a\/(.+) b\/(.+)\r?$/gm)];

  if (matches.length === 0) {
    return [{ label: "Diff", language: "diff", text: diff }];
  }

  return matches.map((match, index) => {
    const label = match[2] ?? "Diff";
    const start = match.index ?? 0;
    const end = matches[index + 1]?.index ?? diff.length;

    return {
      label,
      language: languageForPath(label),
      text: diff.slice(start, end),
    };
  });
}

type DiffLineKind = "metadata" | "hunk" | "addition" | "deletion" | "context";

interface PreparedDiffLine {
  kind: DiffLineKind;
  prefix: string;
  oldLine?: number;
  newLine?: number;
}

interface PreparedDiff {
  code: string;
  lines: PreparedDiffLine[];
}

function prepareDiffForLanguage(code: string): PreparedDiff {
  let inHunk = false;
  let oldLine = 0;
  let newLine = 0;
  const lines: PreparedDiffLine[] = [];

  const highlightedLines = code.split("\n").map((rawLine) => {
    let kind: DiffLineKind = "metadata";
    let prefix = "";
    let sourceLine = rawLine;
    let currentOldLine: number | undefined;
    let currentNewLine: number | undefined;
    const hunk = /^@@ -(\d+)(?:,\d+)? \+(\d+)(?:,\d+)? @@/.exec(rawLine);

    if (hunk) {
      inHunk = true;
      oldLine = Number(hunk[1]);
      newLine = Number(hunk[2]);
      kind = "hunk";
    } else if (inHunk && rawLine.startsWith("+")) {
      kind = "addition";
      prefix = "+";
      sourceLine = rawLine.slice(1);
      currentNewLine = newLine;
      newLine += 1;
    } else if (inHunk && rawLine.startsWith("-")) {
      kind = "deletion";
      prefix = "-";
      sourceLine = rawLine.slice(1);
      currentOldLine = oldLine;
      oldLine += 1;
    } else if (inHunk && rawLine.startsWith(" ")) {
      kind = "context";
      prefix = " ";
      sourceLine = rawLine.slice(1);
      currentOldLine = oldLine;
      currentNewLine = newLine;
      oldLine += 1;
      newLine += 1;
    }

    lines.push({
      kind,
      prefix,
      oldLine: currentOldLine,
      newLine: currentNewLine,
    });
    return sourceLine;
  });

  return { code: highlightedLines.join("\n"), lines };
}

function gutterCell(value: number | string | undefined, className: string) {
  return {
    type: "element" as const,
    tagName: "span",
    properties: { class: className },
    children: [{ type: "text" as const, value: value?.toString() ?? "" }],
  };
}

function unifiedDiffTransformer(lines: readonly PreparedDiffLine[]): ShikiTransformer {
  return {
    name: "nexus-unified-diff",

    line(node, lineNumber) {
      const line = lines[lineNumber - 1];
      if (!line) {
        return;
      }

      this.addClassToHast(node, "diff-code-line");
      this.addClassToHast(node, `diff-${line.kind}`);
      node.children.unshift({
        type: "element",
        tagName: "span",
        properties: { class: "diff-line-gutter", "aria-hidden": "true" },
        children: [
          gutterCell(line.oldLine, "diff-old-line"),
          gutterCell(line.newLine, "diff-new-line"),
          gutterCell(line.prefix, "diff-line-prefix"),
        ],
      });
    },
  };
}

async function loadDiffHighlighter() {
  const [
    { createHighlighterCore },
    { createJavaScriptRegexEngine },
    { default: diff },
    { default: ayuLight },
    { default: ayuDark },
  ] = await Promise.all([
    import("shiki/core"),
    import("shiki/engine/javascript"),
    import("@shikijs/langs/diff"),
    import("@shikijs/themes/ayu-light"),
    import("@shikijs/themes/ayu-dark"),
  ]);

  return createHighlighterCore({
    langs: diff,
    themes: [ayuLight, ayuDark],
    engine: createJavaScriptRegexEngine(),
  });
}

let diffHighlighter: ReturnType<typeof loadDiffHighlighter> | undefined;

async function highlightDiff(
  code: string,
  requestedLanguage: BundledLanguage,
  lineNumbers: boolean,
): Promise<string> {
  const highlighter = await (diffHighlighter ??= loadDiffHighlighter());
  let language = requestedLanguage;

  if (language !== "diff" && !highlighter.getLoadedLanguages().includes(language)) {
    const loader = LANGUAGE_LOADERS[language];

    if (!loader) {
      language = "diff";
    } else {
      try {
        highlighter.loadLanguageSync(await loader());
      } catch {
        language = "diff";
      }
    }
  }

  if (!lineNumbers) {
    return highlighter.codeToHtml(code, {
      lang: language,
      themes: { light: "ayu-light", dark: "ayu-dark" },
      defaultColor: "light",
    });
  }

  const prepared = prepareDiffForLanguage(code);

  return highlighter.codeToHtml(prepared.code, {
    lang: language,
    themes: { light: "ayu-light", dark: "ayu-dark" },
    defaultColor: "light",
    transformers: [unifiedDiffTransformer(prepared.lines)],
  });
}

function HighlightedDiff(props: {
  code: string;
  language: BundledLanguage;
  lineNumbers?: boolean;
}) {
  const [html] = createResource(
    () => ({
      code: props.code,
      language: props.language,
      lineNumbers: props.lineNumbers ?? false,
    }),
    ({ code, language, lineNumbers }) =>
      highlightDiff(code, language, lineNumbers),
  );
  const rendered = () => (html.error ? undefined : html());

  return (
    <div class="message-highlight">
      <Show
        when={rendered()}
        fallback={<pre class="message-highlight-fallback">{props.code}</pre>}
      >
        {(value) => <div innerHTML={value()} />}
      </Show>
    </div>
  );
}

export function DiffStat(props: { text: string }) {
  return (
    <div aria-label="Diffstat" class="message-body-segment message-diffstat">
      <HighlightedDiff code={props.text} language="diff" />
    </div>
  );
}

export function DiffViewer(props: { diff: string }) {
  const files = createMemo(() => splitDiffFiles(props.diff));
  const [expanded, setExpanded] = createSignal(true);
  const [raw, setRaw] = createSignal(false);

  return (
    <section aria-label="Diff" class="message-body-segment diff-viewer">
      <div class="diff-viewer-header">
        <button
          type="button"
          class="diff-viewer-toggle"
          aria-expanded={expanded()}
          aria-label={expanded() ? "Collapse diff section" : "Expand diff section"}
          onClick={() => setExpanded((value) => !value)}
        >
          <span aria-hidden="true" class="diff-viewer-chevron">
            {expanded() ? "▾" : "▸"}
          </span>
          <span>Diff</span>
          <span class="diff-viewer-count">
            {files().length} {files().length === 1 ? "file" : "files"}
          </span>
        </button>
        <label class="diff-viewer-raw-toggle">
          <input
            type="checkbox"
            checked={raw()}
            onChange={(event) => setRaw(event.currentTarget.checked)}
          />
          Raw
        </label>
      </div>

      <Show when={expanded()}>
        <Show
          when={!raw()}
          fallback={<pre class="diff-viewer-raw">{props.diff}</pre>}
        >
          <div class="diff-viewer-files">
            <For each={files()}>
              {(file) => (
                <details class="diff-file" open>
                  <summary class="diff-file-summary">{file.label}</summary>
                  <HighlightedDiff
                    code={file.text}
                    language={file.language}
                    lineNumbers
                  />
                </details>
              )}
            </For>
          </div>
        </Show>
      </Show>
    </section>
  );
}
