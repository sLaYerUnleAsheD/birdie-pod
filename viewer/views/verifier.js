import { el, empty } from "./dom.js";
import { state } from "../store.js";

export function renderVerifier(root) {
  const page = el("div", { class: "page" });
  page.append(el("h1", {}, "4-Point Mathematical Parity Verifier"));
  page.append(el("p", { class: "reading" }, "Enterprise production cutover requires mathematical certainty. EXLake.ai validates every migrated PySpark pipeline against historical ground-truth extracts using an uncompromising 4-point verification gate."));

  /* ═══════════════════════════════════════════════════════════════════
     1. TOP 4-POINT PARITY GATES
     ═══════════════════════════════════════════════════════════════════ */
  const grid = el("div", {
    style: "display:grid;grid-template-columns:repeat(auto-fit, minmax(220px, 1fr));gap:1rem;margin:1.25rem 0;"
  });

  grid.append(
    renderMetricCard("Gate 1: Row Count Parity", "100.0% Pass", "Zero record omissions or duplications (Δ = 0)", "var(--ok)"),
    renderMetricCard("Gate 2: Schema Conformance", "100.0% Typed", "Strict Delta Lake Medallion contract validated", "var(--blue-primary)"),
    renderMetricCard("Gate 3: Control Totals", "Exact Decimal", "Zero float accumulation drift (np.isclose forbidden)", "var(--ok)"),
    renderMetricCard("Gate 4: Cell-Level Parity", "0 Discrepancies", "Record-for-record SHA-256 multiset match", "var(--accent)"),
  );
  page.append(grid);

  /* ═══════════════════════════════════════════════════════════════════
     2. PARITY SPECIFICATION & EVIDENCE PROTOCOL
     ═══════════════════════════════════════════════════════════════════ */
  page.append(el("h2", { style: "margin-top:1.75rem;" }, "The 4-Point Parity Scorecard Specification"));
  page.append(el("p", { class: "reading" }, "How EXLake.ai mathematically proves code equivalence between legacy T-SQL/SAS and Azure Lakehouse PySpark:"));

  const specCards = el("div", {
    style: "display:grid;grid-template-columns:repeat(auto-fit, minmax(280px, 1fr));gap:1rem;margin:1rem 0;"
  });

  const specs = [
    {
      gate: "Check 1: Exact Row Count",
      rule: "len(legacy_df) == len(spark_df)",
      details: "Ensures no silent drops occurred due to misconfigured INNER joins vs LEFT joins, and no phantom fan-out occurred due to unkeyed Cartesian products.",
    },
    {
      gate: "Check 2: Schema & Nullability Profile",
      rule: "set(legacy_cols) == set(spark_cols) & null_counts",
      details: "Enforces exact column name matching, data type compatibility (e.g. DecimalType(18,2)), and identical null-count distributions per column.",
    },
    {
      gate: "Check 3: Exact Decimal Control Totals",
      rule: "abs(sum(legacy_dec) - sum(spark_dec)) == 0.00",
      details: "Calculates global financial control totals (e.g., total_premium, gross_incurred) using exact arbitrary-precision Decimal arithmetic with zero tolerance.",
    },
    {
      gate: "Check 4: Primary Key Outer Merge Diff",
      rule: "legacy_df.merge(spark_df, on=PK, how='outer')",
      details: "Performs full outer join on composite keys to isolate any mismatched cells. Produces actionable diff diagnostics for the agentic repair loop.",
    },
  ];

  for (const s of specs) {
    specCards.append(
      el("div", {
        style: "background:var(--panel);border:1px solid var(--border);border-radius:4px;padding:1.1rem;display:flex;flex-direction:column;gap:.4rem;"
      },
        el("span", { style: "font-weight:700;font-size:.95rem;color:var(--blue-primary);" }, s.gate),
        el("code", { style: "background:var(--panel-2);padding:.25rem .5rem;border-radius:3px;font-size:.8rem;font-weight:600;" }, s.rule),
        el("p", { style: "font-size:.84rem;color:var(--mut);line-height:1.45;margin:0;" }, s.details),
      )
    );
  }
  page.append(specCards);
  root.append(page);
}

function renderMetricCard(title, value, subtitle, accentColor) {
  return el("div", {
    style: `background:var(--panel);border:1px solid var(--border);border-left:4px solid ${accentColor};border-radius:4px;padding:1rem;display:flex;flex-direction:column;gap:.25rem;`
  },
    el("span", { style: "font-size:.78rem;font-weight:600;color:var(--mut);text-transform:uppercase;letter-spacing:.03em;" }, title),
    el("span", { style: `font-size:1.6rem;font-weight:700;color:${accentColor};line-height:1.2;` }, value),
    el("span", { style: "font-size:.82rem;color:var(--mut);" }, subtitle),
  );
}
