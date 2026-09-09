import { describe, expect, it } from "vitest";
import type { components } from "../src/api/generated";
import { citationButtonLabels, citationSourceGroups } from "../src/App";

type CitationDTO = components["schemas"]["CitationDTO"];

function citation(overrides: Partial<CitationDTO> & Pick<CitationDTO, "citation_id" | "evidence_id">): CitationDTO {
  const fixture: CitationDTO = {
    citation_id: overrides.citation_id,
    document_title: overrides.document_title ?? "MSI Expert Test Document",
    evidence_id: overrides.evidence_id,
    pdf_pages: overrides.pdf_pages ?? [73],
    printed_page_labels: overrides.printed_page_labels ?? [],
    source_url: overrides.source_url ?? "minio://documents/source-a.pdf",
    structural_path: overrides.structural_path ?? ["Раздел V", "Пункт 43", "Таблица"],
  };
  if (overrides.version_label !== undefined) {
    fixture.version_label = overrides.version_label;
  }
  return fixture;
}

describe("answer citation presentation", () => {
  it("gives same-page fragments distinct labels while preserving exact evidence ids", () => {
    const citations = [
      citation({ citation_id: "c1", evidence_id: "evidence-1", structural_path: ["path", "a"] }),
      citation({ citation_id: "c2", evidence_id: "evidence-2", structural_path: ["path", "b"] }),
      citation({ citation_id: "c3", evidence_id: "evidence-3", structural_path: ["path", "c"] }),
      citation({ citation_id: "c4", evidence_id: "evidence-4", structural_path: ["path", "d"] }),
    ];

    const labels = citationButtonLabels(citations);

    expect(labels.get("c1")).toMatchObject({ buttonLabel: "Фрагмент 1 · стр. 73", evidenceId: "evidence-1" });
    expect(labels.get("c2")).toMatchObject({ buttonLabel: "Фрагмент 2 · стр. 73", evidenceId: "evidence-2" });
    expect(labels.get("c3")).toMatchObject({ buttonLabel: "Фрагмент 3 · стр. 73", evidenceId: "evidence-3" });
    expect(labels.get("c4")).toMatchObject({ buttonLabel: "Фрагмент 4 · стр. 73", evidenceId: "evidence-4" });
    expect(new Set([...labels.values()].map((item) => item.buttonLabel))).toHaveLength(4);
  });

  it("groups duplicate document metadata without collapsing same-titled distinct versions", () => {
    const citations = [
      citation({ citation_id: "c1", evidence_id: "evidence-a1", source_url: "minio://documents/same-title-v1.pdf", version_label: "версия 1" }),
      citation({ citation_id: "c2", evidence_id: "evidence-a2", source_url: "minio://documents/same-title-v1.pdf", version_label: "версия 1", pdf_pages: [74] }),
      citation({ citation_id: "c3", evidence_id: "evidence-b1", source_url: "minio://documents/same-title-v2.pdf", version_label: "версия 2" }),
    ];

    const groups = citationSourceGroups(citations);

    expect(groups).toHaveLength(2);
    const [versionOne, versionTwo] = groups;
    expect(versionOne).toBeDefined();
    expect(versionTwo).toBeDefined();
    expect(groups.map((group) => group.documentTitle)).toEqual(["MSI Expert Test Document", "MSI Expert Test Document"]);
    expect(groups.map((group) => group.versionLabel)).toEqual(["версия 1", "версия 2"]);
    expect(versionOne?.fragments.map((fragment) => fragment.evidenceId)).toEqual(["evidence-a1", "evidence-a2"]);
    expect(versionTwo?.fragments.map((fragment) => fragment.evidenceId)).toEqual(["evidence-b1"]);
  });
});
