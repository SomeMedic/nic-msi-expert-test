import { afterEach, describe, expect, it, vi } from "vitest";
import { preferredTheme, selectTheme } from "../src/theme";

afterEach(() => vi.unstubAllGlobals());

describe("theme preference", () => {
  it("uses a valid saved choice ahead of the operating system", () => {
    vi.stubGlobal("localStorage", { getItem: () => "light" });
    vi.stubGlobal("window", { matchMedia: () => ({ matches: true }) });
    expect(preferredTheme()).toBe("light");
  });

  it.each([null, "unexpected"])("uses the operating system for an unset or invalid choice: %s", (saved) => {
    vi.stubGlobal("localStorage", { getItem: () => saved });
    vi.stubGlobal("window", { matchMedia: () => ({ matches: true }) });
    expect(preferredTheme()).toBe("dark");
  });

  it("opens and changes theme even when browser storage is denied", () => {
    vi.stubGlobal("localStorage", {
      getItem: () => { throw new Error("storage denied"); },
      setItem: () => { throw new Error("storage denied"); },
    });
    vi.stubGlobal("window", { matchMedia: () => ({ matches: false }) });
    const dataset: Record<string, string> = {};
    vi.stubGlobal("document", { documentElement: { dataset } });
    expect(preferredTheme()).toBe("light");
    expect(() => selectTheme("dark")).not.toThrow();
    expect(dataset.theme).toBe("dark");
  });
});
