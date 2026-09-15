import { cleanup, render, screen } from "@solidjs/testing-library";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it } from "vitest";
import { ThemeToggle } from "./App";
import { THEME_STORAGE_KEY } from "./theme";

afterEach(() => {
  cleanup();
  localStorage.clear();
  delete document.documentElement.dataset.theme;
});

describe("ThemeToggle", () => {
  it("defaults to the system theme and persists explicit choices", async () => {
    render(() => <ThemeToggle />);
    const toggle = screen.getByRole("combobox", { name: "Theme" });

    expect(toggle).toHaveValue("system");

    await userEvent.selectOptions(toggle, "dark");
    expect(document.documentElement).toHaveAttribute("data-theme", "dark");
    expect(localStorage.getItem(THEME_STORAGE_KEY)).toBe("dark");

    await userEvent.selectOptions(toggle, "light");
    expect(document.documentElement).toHaveAttribute("data-theme", "light");
    expect(localStorage.getItem(THEME_STORAGE_KEY)).toBe("light");

    await userEvent.selectOptions(toggle, "system");
    expect(document.documentElement).toHaveAttribute("data-theme", "system");
    expect(localStorage.getItem(THEME_STORAGE_KEY)).toBeNull();
  });

  it("restores a saved theme", () => {
    localStorage.setItem(THEME_STORAGE_KEY, "dark");
    render(() => <ThemeToggle />);

    expect(screen.getByRole("combobox", { name: "Theme" })).toHaveValue("dark");
  });
});
