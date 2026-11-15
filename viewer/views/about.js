import { el } from "./dom.js";

export function renderAbout(root) {
  const page = el("div", { class: "page about-page" });
  page.append(
    el("div", { class: "about-hero" },
      el("h1", {}, "EXLake.ai"),
      el("p", { class: "about-lead" }, "Automating SAS & T-SQL Legacy Code Migration to PySpark & Azure Lakehouse"),
      el("blockquote", { class: "quote-box" },
        "Relying entirely on LLMs to translate legacy code produces silent hallucinations. EXLake.ai combines deterministic AST parsing, Delta Lake Medallion contracts, and autonomous agentic repair loops to guarantee 100% mathematical parity."
      ),
    ),
    el("div", { class: "about-grid" },
      el("div", { class: "about-card" },
        el("h2", {}, "Core Value Proposition"),
        el("p", {}, "Enterprise financial, actuarial, and insurance reporting relies on decades-old on-premise T-SQL ETL jobs and complex SAS statistical programs. EXLake.ai automates their end-to-end modernization to cloud-native PySpark running on Azure Databricks Lakehouse with zero manual code re-writes."),
      ),
      el("div", { class: "about-card" },
        el("h2", {}, "3-Tier Anti-Hallucination Routing"),
        el("p", {}, "Every legacy script is triaged by an AST parser before calling any LLM:"),
        el("ul", { style: "padding-left:1.25rem;margin-top:.5rem;display:flex;flex-direction:column;gap:.35rem;" },
          el("li", {}, el("strong", {}, "Tier A (Deterministic · $0 Cost): "), "Standard relational queries transpiled 100% deterministically via sqlglot AST."),
          el("li", {}, el("strong", {}, "Tier B (Hybrid · ~$0.015): "), "CTEs, Windowing, and PROC MEANS synthesized via AST skeletons enriched with PySpark DataFrames via Meta LLM / Claude skill."),
          el("li", {}, el("strong", {}, "Tier C (Agentic · ~$0.045): "), "Procedural scripts (WHILE, MERGE, RETAIN macros) routed to autonomous Meta LLM / Claude skill repair loops."),
        ),
      ),
      el("div", { class: "about-card" },
        el("h2", {}, "Autonomous Agentic Repair Loop (Meta LLM / Claude Skill)"),
        el("p", {}, "When a generated PySpark pipeline encounters syntax, runtime, or numerical divergence against the legacy oracle, EXLake.ai bundles three diagnostic artifacts (Legacy Source, Failed PySpark Code, Difference Report) and feeds them into the Meta LLM / Claude Repair Agent. Operable via the Claude Code /modernize slash skill or headless benchmark runner, the agent repairs dialect divergence traps across up to 3 self-healing iterations."),
      ),
      el("div", { class: "about-card" },
        el("h2", {}, "4-Point Mathematical Parity Gate"),
        el("p", {}, "Cutover to the cloud requires mathematical proof. Every migrated pipeline is reconciled against baseline data across 4 strict gates:"),
        el("ul", { style: "padding-left:1.25rem;margin-top:.5rem;display:flex;flex-direction:column;gap:.35rem;" },
          el("li", {}, el("strong", {}, "Gate 1: "), "Row count matching with zero delta."),
          el("li", {}, el("strong", {}, "Gate 2: "), "Schema and column-type contract enforcement."),
          el("li", {}, el("strong", {}, "Gate 3: "), "Exact Decimal control totals (preventing floating-point rounding drift)."),
          el("li", {}, el("strong", {}, "Gate 4: "), "Cell-level record-by-record SHA-256 hash parity."),
        ),
      ),
      el("div", { class: "about-card", style: "grid-column: 1 / -1;" },
        el("h2", {}, "Open Source & Reproducibility"),
        el("p", {}, "All benchmark suites, transpilation engines, reconciliation algorithms, and Docker execution sandboxes are fully open-source and reproducible offline."),
        el("p", { style: "margin-top:.75rem;" },
          el("a", { class: "about-link", href: "https://github.com/sLaYerUnleAsheD/birdie-pod", target: "_blank", rel: "noopener" }, "→ GitHub Repository: https://github.com/sLaYerUnleAsheD/birdie-pod")
        ),
      ),
    ),
  );
  root.append(page);
}
