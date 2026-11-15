import { state, subscribe, boot, parseHash, setPage, ensureRun, openProgramPath, closeCompare } from "./store.js?v=exlake_v2";
import { clear } from "./views/dom.js";
import { renderExplorer } from "./views/explorer.js";
import { renderResults } from "./views/results.js";
import { renderFailures } from "./views/failures.js";
import { renderVerifier } from "./views/verifier.js";
import { renderAbout } from "./views/about.js";
import { renderModernize } from "./views/modernize.js?v=exlake_v2";

const main = document.getElementById("main");

function syncNav() {
  document.querySelectorAll("[data-nav]").forEach((a) => {
    a.setAttribute("aria-current", a.dataset.nav === state.route.page ? "page" : "false");
  });

  const lead = document.querySelector(".provenance-lead");
  const desc = document.querySelector(".provenance-desc");
  const badgeText = document.querySelector(".provenance-badge span:last-child");

  if (state.route.page === "modernize") {
    if (lead) lead.textContent = "EXLake.ai: Agentic Data Platform Modernisation";
    if (desc) desc.textContent = "3-Tier deterministic routing (sqlglot AST + Meta LLM), Medallion Delta Lake contracts, and mathematical 4-point output reconciliation.";
    if (badgeText) badgeText.textContent = "100% Record-for-Record Parity";
  } else {
    if (lead) lead.textContent = "Agents can write the code. Nobody can trust it without evidence.";
    if (desc) desc.textContent = "EXLake.ai runs compiler oracles, diffs every record, and executes autonomous repair loops until 100% equivalence is proven.";
    if (badgeText) badgeText.textContent = "Deterministic Parity Gate";
  }
}

function render() {
  syncNav();
  const page = state.route.page;
  clear(main);
  if (page === "modernize") { renderModernize(main); }
  else if (page === "results") { renderResults(main); }
  else if (page === "failures") { renderFailures(main); }
  else if (page === "verifier") { renderVerifier(main); }
  else if (page === "about") { renderAbout(main); }
  else { renderExplorer(main); }
}

subscribe(render);

window.addEventListener("hashchange", async () => {
  const next = parseHash();
  const needRun = next.page === "explorer" && next.runId && !state.runs.has(next.runId);
  const switchRun = next.runId && next.runId !== state.route.runId;
  state.route = next;
  if (needRun || switchRun) {
    const meta = (state.index?.runs || []).find((r) => r.id === next.runId);
    openProgramPath(meta);
    await ensureRun(next.runId, { reset: switchRun });
  } else {
    render();
  }
});

document.addEventListener("keydown", (e) => {
  if (e.key === "Escape" && state.compare.open) closeCompare();
});

document.querySelectorAll("[data-nav]").forEach((a) => {
  a.addEventListener("click", (e) => {
    e.preventDefault();
    setPage(a.dataset.nav);
  });
});

boot();
