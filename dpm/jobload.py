"""Load a legacy job: source text plus meta.yaml."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

try:
    import yaml
except ImportError:
    yaml = None

ROOT = Path(__file__).resolve().parent
JOBS = ROOT / "jobs"
DATA = ROOT / "data"
EXPECTED = ROOT / "expected"


def _safe_yaml_load(text: str) -> dict:
    if yaml is not None:
        return yaml.safe_load(text)
    
    # Robust lightweight fallback parser for meta.yaml
    result: dict = {}
    stack: list[tuple[int, dict]] = [(-1, result)]
    
    for raw_line in text.splitlines():
        if not raw_line.strip() or raw_line.strip().startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip())
        line = raw_line.strip()
        
        while len(stack) > 1 and indent <= stack[-1][0]:
            stack.pop()
            
        current = stack[-1][1]
        
        if ":" in line:
            key, val = line.split(":", 1)
            key = key.strip()
            val = val.strip()
            if not val:
                sub: dict = {}
                current[key] = sub
                stack.append((indent, sub))
            else:
                if val.startswith("[") and val.endswith("]"):
                    val_items = [v.strip().strip("'\"") for v in val[1:-1].split(",") if v.strip()]
                    current[key] = val_items
                else:
                    current[key] = val.strip("'\"")
    return result


@dataclass
class Job:
    id: str
    root: Path
    source: str
    meta: dict

    @property
    def dialect(self) -> str:
        return self.meta.get("dialect", "tsql")

    @property
    def oracle_kind(self) -> str:
        return self.meta.get("oracle_kind", "sqlserver")

    @property
    def inputs(self) -> dict:
        return self.meta.get("inputs") or {}

    @property
    def outputs(self) -> dict:
        return self.meta.get("outputs") or {}


def load_job(path_or_id: str | Path, jobs_dir: str | Path | None = None) -> Job:
    p = Path(path_or_id)
    if not p.exists() or not p.is_dir():
        base = Path(jobs_dir) if jobs_dir else JOBS
        root = base / str(path_or_id)
    else:
        root = p
    if root.is_file():
        root = root.parent
    meta_path = root / "meta.yaml"
    text = meta_path.read_text(encoding="utf-8") if meta_path.exists() else "{}"
    meta = _safe_yaml_load(text)
    src = next((p for p in (root / "job.sql", root / "job.sas") if p.exists()), None)
    source_code = src.read_text(encoding="utf-8") if src else ""
    return Job(id=meta.get("id") or root.name, root=root, source=source_code, meta=meta)


def list_jobs(jobs_root: Path | str | None = None) -> list[str]:
    base = Path(jobs_root) if jobs_root else JOBS
    return sorted([d.name for d in base.iterdir() if d.is_dir() and (d / "meta.yaml").exists()])


def all_jobs(jobs_root: Path | None = None) -> list[Job]:
    base = jobs_root or JOBS
    found = []
    for meta in sorted(base.glob("*/meta.yaml")):
        found.append(load_job(meta.parent))
    return found
