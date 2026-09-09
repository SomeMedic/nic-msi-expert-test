import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "./App";
import { preferredTheme } from "./theme";
import "./styles.css";

document.documentElement.dataset.theme = preferredTheme();

const container = document.getElementById("root");
if (!container) throw new Error("Missing application root");

createRoot(container).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
