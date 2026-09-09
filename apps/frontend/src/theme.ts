export type Theme = "light" | "dark";

const themeKey = "digital-expert-theme";

export function preferredTheme(): Theme {
  try {
    const saved = localStorage.getItem(themeKey);
    if (saved === "light" || saved === "dark") return saved;
  } catch {
    // A blocked storage area must not prevent the workspace from opening.
  }
  return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}

export function selectTheme(theme: Theme): void {
  document.documentElement.dataset.theme = theme;
  try {
    localStorage.setItem(themeKey, theme);
  } catch {
    // The current tab still changes theme when preferences cannot be saved.
  }
}
