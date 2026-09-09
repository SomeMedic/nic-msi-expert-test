import { useState } from "react";
import { selectTheme, type Theme } from "../theme";

export function ThemeToggle() {
  const [theme, setTheme] = useState<Theme>(() => document.documentElement.dataset.theme === "dark" ? "dark" : "light");
  return (
    <button
      type="button"
      className="theme-toggle"
      aria-label={theme === "dark" ? "Светлая тема" : "Тёмная тема"}
      title={theme === "dark" ? "Переключить на светлую тему" : "Переключить на тёмную тему"}
      onClick={() => {
        const next = theme === "dark" ? "light" : "dark";
        selectTheme(next);
        setTheme(next);
      }}
    >
      <span aria-hidden="true">{theme === "dark" ? "◐" : "◑"}</span>
      <span className="theme-label">{theme === "dark" ? "Светлая тема" : "Тёмная тема"}</span>
    </button>
  );
}
