from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


@dataclass
class RowResult:
    date_key: str
    version: int
    created: bool
    refreshed: bool
    bumped: bool
    reason: str


ROOT = Path(__file__).resolve().parents[1]
CSV_PATH = ROOT / "content_generation.csv"
PUZZLES_DIR = ROOT / "puzzles"
MANIFEST_PATH = ROOT / "manifest.json"


DATEKEY_COL = "dateKey (YYYY-MM-DD)"
CATEGORY_COL = "category"
TAGS_COL = "tags (semicolon-separated)"
ANSWER_CANONICAL_COL = "answerCanonical"
ANSWER_ALIASES_COL = "answerAliases (comma-separated)"
SOLUTION_EXPLANATION_COL = "solutionExplanation"
VERSION_COL = "version"
CONTENT_HASH_COL = "contentHash"


@dataclass
class CsvTable:
    header: List[str]
    rows: List[Dict[str, str]]
    delimiter: str


def _utc_now_iso_z() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _strip_wrapping_quotes(text: str) -> str:
    s = text.strip()
    if len(s) >= 2 and ((s[0] == s[-1]) and s[0] in ("\"", "'")):
        return s[1:-1].strip()

    # Clean up common malformed cases from the source TSV, e.g. trailing "'" only.
    if s.endswith("'") and not s.startswith("'"):
        # Often the generator wraps the whole value in single quotes, but sometimes
        # only the trailing quote remains (especially after punctuation).
        if len(s) >= 2 and s[-2] in ".!?":
            s = s[:-1].rstrip()
        elif s.count("'") == 1:
            s = s[:-1].rstrip()

    if s.startswith("'") and not s.endswith("'"):
        if s.count("'") == 1:
            s = s[1:].lstrip()

    return s


def _split_semicolon_list(text: str) -> List[str]:
    s = _strip_wrapping_quotes(text)
    if not s:
        return []
    parts = [p.strip() for p in s.split(";")]
    return [p for p in parts if p]


def _split_comma_list(text: str) -> List[str]:
    s = _strip_wrapping_quotes(text)
    if not s:
        return []
    parts = [p.strip() for p in s.split(",")]
    return [p for p in parts if p]


def _detect_delimiter(first_line: str) -> str:
    # Your file is effectively a TSV, despite the .csv extension.
    if "\t" in first_line:
        return "\t"
    return ","


def read_csv_table(path: Path) -> CsvTable:
    raw = path.read_text(encoding="utf-8", errors="replace")
    lines = raw.splitlines()
    if not lines:
        raise ValueError(f"Empty CSV/TSV: {path}")

    delimiter = _detect_delimiter(lines[0])

    with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        header = list(reader.fieldnames or [])
        rows: List[Dict[str, str]] = []
        for row in reader:
            # Normalize None -> "" for missing fields.
            rows.append({k: (v if v is not None else "") for k, v in row.items()})

    if not header:
        raise ValueError(f"No header detected in: {path}")

    return CsvTable(header=header, rows=rows, delimiter=delimiter)


def write_csv_table(path: Path, table: CsvTable) -> None:
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    updated_path = path.with_name(path.stem + ".updated" + path.suffix)

    def _write(to_path: Path) -> None:
        with to_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=table.header,
                delimiter=table.delimiter,
                quoting=csv.QUOTE_MINIMAL,
                lineterminator="\n",
            )
            writer.writeheader()
            for row in table.rows:
                writer.writerow({k: row.get(k, "") for k in table.header})

    try:
        _write(tmp_path)
        os.replace(tmp_path, path)
    except PermissionError:
        # Common on Windows if the CSV is open in Excel/Sheets.
        # Fall back to writing a new file so generation can still succeed.
        try:
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
        except Exception:
            pass

        _write(updated_path)
        print(
            f"WARNING: Could not overwrite {path} (file may be open). Wrote updated CSV to {updated_path} instead.",
            flush=True,
        )


def _require_columns(table: CsvTable, required: Iterable[str]) -> None:
    missing = [c for c in required if c not in table.header]
    if missing:
        raise ValueError(f"Missing required column(s): {missing}. Present columns: {table.header}")


def _parse_int(value: str, default: int = 1) -> int:
    s = _strip_wrapping_quotes(value)
    if not s:
        return default
    try:
        return int(s)
    except ValueError:
        return default


def _hint_columns() -> List[Tuple[str, str]]:
    cols: List[Tuple[str, str]] = []
    for i in range(1, 6):
        cols.append((f"hint{i}", f"hint{i}Explanation"))
    return cols


def _row_has_any_content(row: Dict[str, str]) -> bool:
    # Avoid processing trailing blank rows.
    for v in row.values():
        if (v or "").strip():
            return True
    return False


def build_puzzle_object(row: Dict[str, str], version: int) -> Dict[str, Any]:
    date_key = _strip_wrapping_quotes(row.get(DATEKEY_COL, ""))
    category = _strip_wrapping_quotes(row.get(CATEGORY_COL, ""))
    tags = _split_semicolon_list(row.get(TAGS_COL, ""))
    answer_canonical = _strip_wrapping_quotes(row.get(ANSWER_CANONICAL_COL, ""))
    answer_aliases = _split_comma_list(row.get(ANSWER_ALIASES_COL, ""))
    solution_explanation = _strip_wrapping_quotes(row.get(SOLUTION_EXPLANATION_COL, ""))

    hints: List[Dict[str, Any]] = []
    for idx, (hint_col, expl_col) in enumerate(_hint_columns()):
        hint_text = _strip_wrapping_quotes(row.get(hint_col, ""))
        if not hint_text:
            continue
        hint_obj: Dict[str, Any] = {
            "id": f"h{idx + 1}",
            "index": idx,
            "text": hint_text,
        }
        expl = _strip_wrapping_quotes(row.get(expl_col, ""))
        if expl:
            hint_obj["explanationText"] = expl
        hints.append(hint_obj)

    puzzle = {
        "id": date_key,
        "dateKey": date_key,
        "version": version,
        "category": category,
        "tags": tags,
        "answerCanonical": answer_canonical,
        "answerAliases": answer_aliases,
        "hints": hints,
        "solutionExplanation": solution_explanation,
    }

    return puzzle


def compute_content_hash(puzzle_obj: Dict[str, Any]) -> str:
    # Stable hash based on puzzle content excluding `version`.
    content = dict(puzzle_obj)
    content.pop("version", None)

    # Ensure stable ordering for lists that are semantically ordered.
    # - hints: keep order by index
    if isinstance(content.get("hints"), list):
        content["hints"] = sorted(content["hints"], key=lambda h: h.get("index", 0))

    canonical = json.dumps(content, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return _sha256_text(canonical)


def write_puzzle_json(path: Path, puzzle_obj: Dict[str, Any]) -> str:
    text = json.dumps(puzzle_obj, ensure_ascii=False, indent=2)
    path.write_text(text + "\n", encoding="utf-8")
    return _sha256_text(text + "\n")


def load_manifest(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def save_manifest(path: Path, manifest: Dict[str, Any]) -> None:
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def ensure_manifest_shape(manifest: Dict[str, Any]) -> None:
    if "puzzles" not in manifest or not isinstance(manifest["puzzles"], dict):
        manifest["puzzles"] = {}
    if "meta" not in manifest or not isinstance(manifest["meta"], dict):
        manifest["meta"] = {}


def generate(
    csv_path: Path,
    puzzles_dir: Path,
    manifest_path: Path,
    *,
    dry_run: bool,
) -> None:
    table = read_csv_table(csv_path)

    _require_columns(
        table,
        [
            DATEKEY_COL,
            CATEGORY_COL,
            TAGS_COL,
            ANSWER_CANONICAL_COL,
            ANSWER_ALIASES_COL,
            SOLUTION_EXPLANATION_COL,
            VERSION_COL,
        ],
    )

    # Add `contentHash` column if missing.
    if CONTENT_HASH_COL not in table.header:
        table.header.append(CONTENT_HASH_COL)

    results: List[RowResult] = []
    skipped_blank_rows = 0
    skipped_missing_date = 0

    puzzles_dir.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest(manifest_path)
    ensure_manifest_shape(manifest)

    puzzles_manifest: Dict[str, Any] = manifest["puzzles"]

    any_csv_updates = False
    any_manifest_updates = False

    for row in table.rows:
        if not _row_has_any_content(row):
            skipped_blank_rows += 1
            continue

        date_key = _strip_wrapping_quotes(row.get(DATEKEY_COL, ""))
        if not date_key:
            skipped_missing_date += 1
            continue

        current_version = _parse_int(row.get(VERSION_COL, ""), default=1)
        stored_hash = _strip_wrapping_quotes(row.get(CONTENT_HASH_COL, ""))

        # Build puzzle using current version first (version itself is excluded from hashing).
        puzzle_obj = build_puzzle_object(row, version=current_version)
        computed_hash = compute_content_hash(puzzle_obj)

        # Decide whether to bump version.
        bumped = False
        if stored_hash and stored_hash != computed_hash:
            current_version += 1
            bumped = True
            puzzle_obj = build_puzzle_object(row, version=current_version)

        # If no stored hash, initialize it (no bump).
        if not stored_hash:
            any_csv_updates = True

        # Persist updates into the row.
        if bumped:
            row[VERSION_COL] = str(current_version)
            any_csv_updates = True
        row[CONTENT_HASH_COL] = computed_hash

        filename = f"{date_key}.v{current_version}.json"
        puzzle_path = puzzles_dir / filename
        url = f"/puzzles/{filename}"

        existed_before = puzzle_path.exists()

        # Write puzzle json and compute sha
        sha256 = _sha256_text(json.dumps(puzzle_obj, ensure_ascii=False, indent=2) + "\n")

        if not dry_run:
            sha256 = write_puzzle_json(puzzle_path, puzzle_obj)

        new_manifest_entry = {
            "version": current_version,
            "url": url,
            "sha256": sha256,
        }

        existing_manifest_entry = puzzles_manifest.get(date_key)
        if existing_manifest_entry != new_manifest_entry:
            any_manifest_updates = True
        puzzles_manifest[date_key] = new_manifest_entry

        created = (not existed_before) and (not dry_run)
        refreshed = existed_before and (not dry_run)
        if dry_run:
            # In dry-run, treat a missing output file as "would create".
            created = not existed_before
            refreshed = existed_before

        reason = ""
        if bumped:
            reason = "content changed -> version bumped"
        elif not stored_hash:
            reason = "initialized contentHash"
        else:
            reason = "no content change"

        results.append(
            RowResult(
                date_key=date_key,
                version=current_version,
                created=created,
                refreshed=refreshed,
                bumped=bumped,
                reason=reason,
            )
        )

    # Only update the manifest timestamp if the manifest contents changed.
    # This keeps runs with no effective changes from churning manifest.json.
    if any_manifest_updates:
        manifest["meta"]["generatedAtUtc"] = _utc_now_iso_z()

    if not dry_run:
        if any_manifest_updates:
            save_manifest(manifest_path, manifest)
        if any_csv_updates:
            write_csv_table(csv_path, table)

    # ---- Logging summary ----
    prefix = "DRY RUN" if dry_run else "OK"
    created_keys = [r for r in results if r.created]
    bumped_keys = [r for r in results if r.bumped]
    refreshed_keys = [r for r in results if r.refreshed and not r.created]

    if results:
        print(f"[{prefix}] Processed {len(results)} puzzle row(s).", flush=True)
    else:
        print(f"[{prefix}] No puzzle rows processed.", flush=True)

    if created_keys:
        print(f"[{prefix}] Created {len(created_keys)} new puzzle file(s):", flush=True)
        for r in created_keys:
            print(f"  - {r.date_key}.v{r.version}.json", flush=True)
    else:
        print(f"[{prefix}] No new puzzle files created.", flush=True)

    if bumped_keys:
        print(f"[{prefix}] Version bumps ({len(bumped_keys)}):", flush=True)
        for r in bumped_keys:
            print(f"  - {r.date_key} -> v{r.version} ({r.reason})", flush=True)

    if refreshed_keys and not dry_run:
        print(f"[{prefix}] Refreshed existing puzzle file(s): {len(refreshed_keys)}", flush=True)

    if skipped_blank_rows:
        print(f"[{prefix}] Skipped blank row(s): {skipped_blank_rows}", flush=True)
    if skipped_missing_date:
        print(f"[{prefix}] Skipped row(s) missing {DATEKEY_COL}: {skipped_missing_date}", flush=True)

    if any_csv_updates:
        print(f"[{prefix}] CSV updated: {csv_path.name}", flush=True)
    else:
        print(f"[{prefix}] CSV unchanged.", flush=True)

    if any_manifest_updates:
        if dry_run:
            print(f"[{prefix}] Manifest would be updated: {manifest_path.name}", flush=True)
        else:
            print(f"[{prefix}] Manifest updated: {manifest_path.name}", flush=True)
    else:
        print(f"[{prefix}] Manifest unchanged.", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate per-day puzzle JSONs from content_generation.csv and update manifest.json."
    )
    parser.add_argument("--csv", default=str(CSV_PATH), help="Path to content_generation.csv")
    parser.add_argument("--puzzles", default=str(PUZZLES_DIR), help="Puzzles output directory")
    parser.add_argument("--manifest", default=str(MANIFEST_PATH), help="Path to manifest.json")
    parser.add_argument("--dry-run", action="store_true", help="Compute changes but don't write files")

    args = parser.parse_args()

    generate(
        Path(args.csv),
        Path(args.puzzles),
        Path(args.manifest),
        dry_run=bool(args.dry_run),
    )


if __name__ == "__main__":
    main()
