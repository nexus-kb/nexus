import { HashRouter, Route } from "@solidjs/router";
import { render } from "solid-js/web";
import { AppShell } from "./App";
import { ThreadListPage } from "./pages/ThreadListPage";
import { ThreadPage } from "./pages/ThreadPage";
import { CommitPage } from "./pages/CommitPage";
import { applyTheme, storedTheme } from "./theme";
import "./styles.css";

const root = document.getElementById("root");

if (!root) {
  throw new Error("Missing #root element");
}

applyTheme(storedTheme());

render(
  () => (
    <HashRouter root={AppShell}>
      <Route path="/" component={ThreadListPage} />
      <Route path="/thread" component={ThreadPage} />
      <Route path="/commits/:commitID" component={CommitPage} />
    </HashRouter>
  ),
  root,
);
