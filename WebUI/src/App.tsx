import { A, type RouteSectionProps } from "@solidjs/router";
import { createSignal } from "solid-js";
import { saveTheme, storedTheme, type ThemePreference } from "./theme";

export function ThemeToggle() {
  const [theme, setTheme] = createSignal(storedTheme());

  const changeTheme = (nextTheme: ThemePreference) => {
    setTheme(nextTheme);
    saveTheme(nextTheme);
  };

  return (
    <label class="theme-control">
      <span>Theme</span>
      <select
        aria-label="Theme"
        value={theme()}
        onChange={(event) => changeTheme(event.currentTarget.value as ThemePreference)}
      >
        <option value="system">Follow system</option>
        <option value="light">Light</option>
        <option value="dark">Dark</option>
      </select>
    </label>
  );
}

export function AppShell(props: RouteSectionProps) {
  return (
    <div class="site-shell">
      <header class="site-header">
        <A class="site-title" href="/" end>
          Nexus KB
        </A>
        <ThemeToggle />
      </header>
      <main id="main-content">{props.children}</main>
    </div>
  );
}
