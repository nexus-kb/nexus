export const THEME_STORAGE_KEY = "nexus-theme";

export type ThemePreference = "system" | "light" | "dark";

export function storedTheme(): ThemePreference {
  const value = localStorage.getItem(THEME_STORAGE_KEY);
  return value === "light" || value === "dark" ? value : "system";
}

export function applyTheme(theme: ThemePreference): void {
  document.documentElement.dataset.theme = theme;
}

export function saveTheme(theme: ThemePreference): void {
  if (theme === "system") {
    localStorage.removeItem(THEME_STORAGE_KEY);
  } else {
    localStorage.setItem(THEME_STORAGE_KEY, theme);
  }
  applyTheme(theme);
}
