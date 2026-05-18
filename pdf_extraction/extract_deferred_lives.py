"""
Extract deferred-lives (terminated-vested / deferred-vested participant) data
from the saved top-100 Form 5500 filings.

For each plan in form5500_chunked_{year}/ this pulls:
  * Schedule SB, line 3 funding-target / participant-count breakdown
        - retired, terminated-vested, active, total
        - for each: headcount, vested funding target, total funding target
    => the terminated-vested row is the core "deferred lives" data point.
  * Schedule SB, line 26b -- Schedule of Projection of Expected Benefit Payments
        - the year-by-year projected payment stream for the Terminated Vested column
          (best-effort column identification; see `tv_payment_confidence`).
  * Assumed retirement age for terminated-vested participants (best-effort, from
    the SB attachment prose) plus the raw snippet for manual review.

It then joins plan identifiers, industry, total liability and the Form 5500
line 6 participant counts (RTD_SEP_PARTCP_FUT_CNT etc.) from
data_output/yearly/db_plans_{year}.parquet.

Coverage notes:
  * Line 3b parses cleanly for every saved filing (it is rendered in the Schedule
    SB form text itself).
  * The line 26b payment table is in the SB attachments. A handful of the top-100
    filings ship those attachments as scanned images with no extractable text
    (JPMorgan, State Farm, Disney, Pfizer, J&J, US Steel, ...), so ``ebp_table_found``
    is False for them -- they would need OCR.
  * ``tv_payment_col_method`` records how the terminated-vested payment column was
    identified: 'header' (read from a cleanly-rendered column-name row) or 'heuristic'
    (inferred from the annuitant column's monotonic decline plus the line 3 funding
    targets). ``tv_payment_confidence`` is high / medium / low accordingly.

Reads:  pdf_extraction/form5500_chunked_{year}/*_chunks.json
        data_output/yearly/db_plans_{year}.parquet
Writes: data_output/deferred_lives_top100_{year}.csv          (one row per plan)
        data_output/deferred_lives_payment_streams_{year}.csv (one row per plan-year)

Usage:
    python pdf_extraction/extract_deferred_lives.py --year 2024
"""

import argparse
import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent


# =============================================================================
# HELPERS
# =============================================================================

def extract_ein_pn_from_filename(filename: str) -> Tuple[Optional[str], Optional[str]]:
    """SPONSOR_EIN_PN_YEAR_chunks.json -> ('123456789', '001')."""
    m = re.search(r"(\d{9})_(\d{3})_\d{4}", filename)
    if m:
        return m.group(1), m.group(2)
    return None, None


def _num(token: str) -> Optional[int]:
    """'9,120,413,895' / '755, 182,329' / '9120413895' -> int."""
    if token is None:
        return None
    cleaned = re.sub(r"[,\s]", "", token)
    if not cleaned or not cleaned.lstrip("-").isdigit():
        return None
    return int(cleaned)


def _schedule_text(chunks: List[dict], schedule_types: Tuple[str, ...]) -> str:
    return "\n".join(c.get("raw_text", "") for c in chunks if c.get("schedule_type") in schedule_types)


# =============================================================================
# SCHEDULE SB -- LINE 3 FUNDING TARGET / PARTICIPANT COUNT BREAKDOWN
# =============================================================================

# A "number" token in line 3: digits with optional embedded thousands commas.
_N = r"(\d[\d,]*\d|\d)"

# Anchored on the row label; captures the next three number tokens (count, vested FT, total FT).
_RE_LINE3_RETIRED = re.compile(
    r"retired participants and beneficiaries receiving payment[ .…]*\.?\s*" + _N + r"\s+" + _N + r"\s+" + _N,
    re.IGNORECASE,
)
_RE_LINE3_TERM = re.compile(
    r"[Ff]or\s*terminated vested participants[ .…]*\.?\s*" + _N + r"\s+" + _N + r"\s+" + _N,
    re.IGNORECASE,
)
_RE_LINE3_ACTIVE = re.compile(
    r"[Ff]or\s*active participants[ .…]*\.?\s*" + _N + r"\s+" + _N + r"\s+" + _N,
    re.IGNORECASE,
)
_RE_LINE3_TOTAL = re.compile(
    r"\bd\s+Total[ .…]*\.?\s*" + _N + r"\s+" + _N + r"\s+" + _N,
    re.IGNORECASE,
)


def parse_line3(chunks: List[dict]) -> Dict[str, Optional[int]]:
    """Parse Schedule SB line 3 breakdown. Prefers the SCHEDULE_SB chunk, falls
    back to the SB_ATTACHMENTS copy. Returns counts / vested FT / total FT for
    retired, terminated-vested, active and total, plus a validation flag."""
    out: Dict[str, Optional[int]] = {
        k: None
        for k in (
            "retired_count", "retired_vested_ft", "retired_total_ft",
            "term_vested_count", "term_vested_vested_ft", "term_vested_total_ft",
            "active_count", "active_vested_ft", "active_total_ft",
            "total_count", "total_vested_ft", "total_total_ft",
        )
    }
    out["line3_source"] = None
    out["line3_validates"] = None

    for src, types in (("SCHEDULE_SB", ("SCHEDULE_SB",)), ("SB_ATTACHMENTS", ("SB_ATTACHMENTS",))):
        text = _schedule_text(chunks, types)
        if not text:
            continue
        # Repair OCR artefacts like "755, 182,329" -> "755,182,329".
        text = re.sub(r"(?<=\d),\s+(?=\d)", ",", text)
        m_ret = _RE_LINE3_RETIRED.search(text)
        m_term = _RE_LINE3_TERM.search(text)
        m_act = _RE_LINE3_ACTIVE.search(text)
        m_tot = _RE_LINE3_TOTAL.search(text)
        if not (m_ret and m_term and m_act):
            continue

        vals = {
            "retired": [_num(g) for g in m_ret.groups()],
            "term_vested": [_num(g) for g in m_term.groups()],
            "active": [_num(g) for g in m_act.groups()],
        }
        out["retired_count"], out["retired_vested_ft"], out["retired_total_ft"] = vals["retired"]
        out["term_vested_count"], out["term_vested_vested_ft"], out["term_vested_total_ft"] = vals["term_vested"]
        out["active_count"], out["active_vested_ft"], out["active_total_ft"] = vals["active"]
        if m_tot:
            out["total_count"], out["total_vested_ft"], out["total_total_ft"] = [_num(g) for g in m_tot.groups()]

        # Validation: row counts / total funding targets should add up.
        out["line3_source"] = src
        out["line3_validates"] = _line3_adds_up(out)
        # Prefer the SCHEDULE_SB parse; only keep going to attachments if it didn't validate.
        if out["line3_validates"] or src == "SB_ATTACHMENTS":
            break
    return out


def _line3_adds_up(d: Dict[str, Optional[int]]) -> Optional[bool]:
    checks = []
    for piece in ("count", "total_ft", "vested_ft"):
        parts = [d.get(f"retired_{piece}"), d.get(f"term_vested_{piece}"), d.get(f"active_{piece}")]
        total = d.get(f"total_{piece}")
        if total is None or any(p is None for p in parts):
            continue
        # Allow small rounding noise (a few dollars / participants).
        checks.append(abs(sum(parts) - total) <= max(5, round(0.0005 * total)))
    if not checks:
        return None
    return all(checks)


# =============================================================================
# SCHEDULE SB -- LINE 26b PROJECTION OF EXPECTED BENEFIT PAYMENTS
# =============================================================================

_RE_EBP_TITLE = re.compile(r"Expected Benefit Payments", re.IGNORECASE)

# A data row: a row label (an explicit plan year 2020-2089, or a "Current Plan Year [+ N]"
# relative label) followed by exactly four amounts -- each optionally "$"-prefixed.
_AMT = r"\$?\s*([\d,]{2,})"
_RE_EBP_ROW = re.compile(
    r"(?:\b(20[2-9]\d|208\d)\b|Current Plan Year(?:\s*\+\s*(\d+))?|(?:CPY|PY)\s*\+\s*(\d+))"
    r"\s+" + _AMT + r"\s+" + _AMT + r"\s+" + _AMT + r"\s+" + _AMT
)


def _ebp_candidate_rows(window: str, base_year: int) -> List[Tuple[int, int, int, int, int, int]]:
    """Yield (position, year, n1, n2, n3, n4) for every row-shaped match."""
    rows = []
    for mm in _RE_EBP_ROW.finditer(window):
        yr_abs, off1, off2, n1, n2, n3, n4 = mm.groups()
        if yr_abs is not None:
            yr = int(yr_abs)
        else:
            off = off1 or off2
            yr = base_year + (int(off) if off else 0)
        nums = [_num(n1), _num(n2), _num(n3), _num(n4)]
        if any(v is None for v in nums):
            continue
        rows.append((mm.start(), yr, *nums))
    return rows


def _row_validates(a: int, b: int, c: int, d: int) -> bool:
    return abs((a + b + c) - d) <= max(5, round(0.001 * max(d, 1)))


def _consecutive_runs(years: List[int]) -> List[List[int]]:
    runs: List[List[int]] = []
    for y in sorted(set(years)):
        if runs and y == runs[-1][-1] + 1:
            runs[-1].append(y)
        else:
            runs.append([y])
    return runs


def _pick_ebp_run(rows: List[Tuple[int, int, int, int, int, int]], plan_year: int):
    """From validating row-shaped matches anywhere in the document, de-dupe by year
    and return the consecutive-year run that best looks like the line 26b table:
    prefer the run that starts at/near the plan year; otherwise the longest run."""
    by_year = {}
    for r in rows:
        by_year.setdefault(r[1], r)  # first occurrence wins
    runs = _consecutive_runs(list(by_year))
    if not runs:
        return []
    near = [run for run in runs if abs(run[0] - plan_year) <= 2 and len(run) >= 3]
    chosen = max(near, key=len) if near else max(runs, key=len)
    return [by_year[y] for y in chosen]


_RE_ACTIVE_HDR = re.compile(r"\bActive\b", re.IGNORECASE)
_RE_TERM_HDR = re.compile(r"\bTerminated\b|\bVested\b", re.IGNORECASE)
_RE_RETIRED_HDR = re.compile(r"\bRetired\b|\bRetiree\b|\bBeneficiaries\b|\bReceiving\b", re.IGNORECASE)


def _header_column_order(header_text: str) -> Optional[Tuple[str, str, str]]:
    """If the column-name row is rendered cleanly -- i.e. the three column names
    appear in order between the last "Plan Year" label and the data -- return their
    order as a tuple drawn from {'active','term','retired'}. Otherwise None (the
    multi-line headers that text extraction jumbles fall through to the heuristic)."""
    if not header_text:
        return None
    m = list(re.finditer(r"Plan Year", header_text, re.IGNORECASE))
    if not m:
        return None
    seg = header_text[m[-1].end(): m[-1].end() + 160]
    pa = _RE_ACTIVE_HDR.search(seg)
    pt = _RE_TERM_HDR.search(seg)
    pr = _RE_RETIRED_HDR.search(seg)
    if not (pa and pt and pr):
        return None
    positions = sorted([("active", pa.start()), ("term", pt.start()), ("retired", pr.start())], key=lambda x: x[1])
    order = tuple(name for name, _ in positions)
    # sanity: the three must be distinct positions
    if len({pa.start(), pt.start(), pr.start()}) != 3:
        return None
    return order  # type: ignore[return-value]


def parse_line26b(chunks: List[dict], plan_year: int,
                  active_ft: Optional[int] = None, term_vested_ft: Optional[int] = None) -> Dict:
    """Parse the line 26b expected-benefit-payment table and identify the
    terminated-vested column. ``active_ft`` / ``term_vested_ft`` are the line 3
    funding targets for the active and terminated-vested groups -- used to tell
    the active and terminated-vested payment columns apart. Column identification
    is best-effort -- see ``tv_payment_confidence``."""
    out = {
        "ebp_table_found": False,
        "ebp_first_year": None,
        "ebp_last_year": None,
        "ebp_n_years": 0,
        "tv_payment_col_index": None,      # 0..2 -- which of the three non-total columns is TV
        "tv_payment_col_method": None,     # 'header' | 'heuristic'
        "tv_payment_confidence": None,     # 'high' | 'medium' | 'low'
        "tv_payments_next10": None,        # sum of TV column, first 10 projected years
        "tv_payments_total": None,         # sum of TV column over the whole projection
        "ebp_header_snippet": None,
        "_stream": [],                     # list of dicts: {year, active, term_vested, retired, total}
    }
    text = _schedule_text(chunks, ("SB_ATTACHMENTS", "SCHEDULE_SB"))
    if not text:
        return out
    text = re.sub(r"(?<=\d),\s+(?=\d)", ",", text)  # repair "1, 234,567" -> "1,234,567"
    # Only look downstream of the first "Expected Benefit Payments" mention so that
    # row-shaped matches elsewhere in the document are out of scope.
    first_title = _RE_EBP_TITLE.search(text)
    if not first_title:
        return out
    scope = text[first_title.start():]
    offset = first_title.start()

    # Collect row-shaped matches, keep those whose components sum to the total
    # (this discards stray amortization-base / schedule-of-bases rows), and pick the
    # consecutive-year run that best matches the line 26b table.
    cand = [r for r in _ebp_candidate_rows(scope, plan_year) if _row_validates(*r[2:])]
    run = _pick_ebp_run(cand, plan_year)
    if len(run) < 3:
        return out
    header_anchor = offset + run[0][0]  # position of first data row in `text`

    rows = [(r[1], r[2], r[3], r[4], r[5]) for r in run]  # (year, n1, n2, n3, total)
    out["ebp_table_found"] = True
    out["ebp_first_year"] = rows[0][0]
    out["ebp_last_year"] = rows[-1][0]
    out["ebp_n_years"] = len(rows)
    header_text = text[max(0, header_anchor - 400): header_anchor]
    out["ebp_header_snippet"] = re.sub(r"\s+", " ", header_text).strip()

    cols = [[r[1] for r in rows], [r[2] for r in rows], [r[3] for r in rows]]
    sums = [sum(c) for c in cols]

    def monotonicity(series: List[int]) -> float:
        pairs = list(zip(series, series[1:]))
        if not pairs:
            return 0.0
        return sum(1 for a, b in pairs if b <= a + 0.01 * max(a, 1)) / len(pairs)

    starts_near_plan_year = abs(rows[0][0] - plan_year) <= 2
    hdr_order = _header_column_order(header_text)  # e.g. ('active','term','retired') or None

    if hdr_order is not None:
        idx = {name: i for i, name in enumerate(hdr_order)}
        retired_idx, active_idx, tv_idx = idx["retired"], idx["active"], idx["term"]
        out["tv_payment_col_method"] = "header"
        out["tv_payment_confidence"] = "high" if (starts_near_plan_year and len(rows) >= 10) else "medium"
    else:
        # No clean header row. Retired (annuitant) stream only ever shrinks -- pick
        # the most monotonically non-increasing column; tie-break on largest year-1
        # value. Then tell active vs terminated-vested apart by lining up the bigger
        # payment stream with the bigger line 3 funding target.
        mono = [monotonicity(c) for c in cols]
        first_row = [rows[0][1], rows[0][2], rows[0][3]]
        retired_idx = max(range(3), key=lambda i: (mono[i], first_row[i]))
        a_idx, b_idx = (i for i in range(3) if i != retired_idx)
        ft_decisive = False
        if active_ft and term_vested_ft and min(active_ft, term_vested_ft) > 0:
            ft_decisive = max(active_ft, term_vested_ft) / min(active_ft, term_vested_ft) >= 1.15
            bigger = a_idx if sums[a_idx] >= sums[b_idx] else b_idx
            smaller = b_idx if bigger == a_idx else a_idx
            active_idx, tv_idx = (bigger, smaller) if active_ft >= term_vested_ft else (smaller, bigger)
        else:
            active_idx = a_idx if sums[a_idx] >= sums[b_idx] else b_idx
            tv_idx = b_idx if active_idx == a_idx else a_idx
        out["tv_payment_col_method"] = "heuristic"
        retired_is_unique = mono[retired_idx] >= 0.95 and all(mono[i] < 0.9 for i in (a_idx, b_idx))
        if retired_is_unique and ft_decisive and starts_near_plan_year and len(rows) >= 10:
            out["tv_payment_confidence"] = "high"
        elif mono[retired_idx] >= 0.90 and starts_near_plan_year:
            out["tv_payment_confidence"] = "medium"
        else:
            out["tv_payment_confidence"] = "low"

    out["tv_payment_col_index"] = tv_idx
    tv_series = cols[tv_idx]
    out["tv_payments_next10"] = sum(tv_series[:10])
    out["tv_payments_total"] = sum(tv_series)

    ordered = {retired_idx: "retired", active_idx: "active", tv_idx: "term_vested"}
    for (yr, a, b, c, d) in rows:
        triple = {ordered[0]: a, ordered[1]: b, ordered[2]: c}
        out["_stream"].append({
            "year": yr,
            "active": triple["active"],
            "term_vested": triple["term_vested"],
            "retired": triple["retired"],
            "total": d,
        })
    return out


# =============================================================================
# ASSUMED RETIREMENT AGE FOR TERMINATED-VESTED PARTICIPANTS
# =============================================================================

_RE_TV_AGE = [
    re.compile(r"[Tt]erminated[ -]?vested(?:\s+participants?| members?)?[^.\n]{0,60}?\bage\s*(\d{2})\b"),
    re.compile(r"[Tt]erminated[ -]?vested(?:\s+participants?| members?)?[^.\n]{0,30}?\b(\d{2})\b"),
    re.compile(r"\bage\s*(\d{2})\b[^.\n]{0,40}?[Tt]erminated[ -]?vested"),
    re.compile(r"[Vv]ested\s+terminated(?:\s+participants?)?[^.\n]{0,60}?\bage\s*(\d{2})\b"),
]


def parse_tv_retirement_age(chunks: List[dict]) -> Dict[str, Optional[object]]:
    out = {"tv_retirement_age": None, "tv_retirement_age_snippet": None}
    text = _schedule_text(chunks, ("SB_ATTACHMENTS",))
    if not text:
        return out
    for rx in _RE_TV_AGE:
        m = rx.search(text)
        if m:
            age = int(m.group(1))
            if 50 <= age <= 72:
                out["tv_retirement_age"] = age
                lo, hi = max(0, m.start() - 120), min(len(text), m.end() + 120)
                out["tv_retirement_age_snippet"] = re.sub(r"\s+", " ", text[lo:hi]).strip()
                break
    return out


# =============================================================================
# PARQUET JOIN
# =============================================================================

_PARQUET_COLS = [
    "EIN", "PLAN_NUMBER", "PLAN_NAME", "SPONSOR_DFE_NAME", "BUSINESS_CODE",
    "INDUSTRY_SECTOR", "ACK_ID_5500", "ACK_ID_SB",
    "ACTIVE_COUNT", "RETIREE_COUNT", "TOTAL_PARTICIPANTS",
    "ACT_LIABILITY", "RET_LIABILITY", "TERM_LIABILITY", "TOTAL_LIABILITY",
    "TOT_PARTCP_BOY_CNT", "TOT_ACTIVE_PARTCP_CNT",
    "RTD_SEP_PARTCP_RCVG_CNT", "RTD_SEP_PARTCP_FUT_CNT", "SEP_PARTCP_PARTL_VSTD_CNT",
    "MORTALITY_CODE", "ACTUARY_FIRM_NAME",
]


def load_parquet_lookup(year: int) -> pd.DataFrame:
    path = REPO_ROOT / "data_output" / "yearly" / f"db_plans_{year}.parquet"
    if not path.exists():
        raise SystemExit(f"Parquet not found: {path}")
    df = pd.read_parquet(path)
    cols = [c for c in _PARQUET_COLS if c in df.columns]
    df = df[cols].copy()
    df["EIN"] = df["EIN"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(9)
    df["PLAN_NUMBER"] = df["PLAN_NUMBER"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(3)
    return df.drop_duplicates(subset=["EIN", "PLAN_NUMBER"])


# =============================================================================
# DRIVER
# =============================================================================

def build(year: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    chunk_dir = REPO_ROOT / "pdf_extraction" / f"form5500_chunked_{year}"
    chunk_files = sorted(chunk_dir.glob("*_chunks.json"))
    if not chunk_files:
        raise SystemExit(f"No chunk files in {chunk_dir}")

    lookup = load_parquet_lookup(year)
    rows: List[dict] = []
    streams: List[dict] = []

    for cf in chunk_files:
        ein, pn = extract_ein_pn_from_filename(cf.name)
        try:
            chunks = json.loads(cf.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            print(f"  ! could not read {cf.name}: {exc}")
            continue

        rec: Dict = {"chunk_file": cf.name, "EIN": ein, "PLAN_NUMBER": pn, "PLAN_YEAR": year}
        rec.update(parse_line3(chunks))
        l26 = parse_line26b(chunks, year,
                            active_ft=rec.get("active_total_ft"),
                            term_vested_ft=rec.get("term_vested_total_ft"))
        stream = l26.pop("_stream")
        rec.update(l26)
        rec.update(parse_tv_retirement_age(chunks))
        rows.append(rec)
        for s in stream:
            streams.append({"EIN": ein, "PLAN_NUMBER": pn, "PLAN_YEAR": year, **s})

    df = pd.DataFrame(rows)
    if not df.empty:
        df["EIN"] = df["EIN"].astype(str).str.zfill(9)
        df["PLAN_NUMBER"] = df["PLAN_NUMBER"].astype(str).str.zfill(3)
        df = df.merge(lookup, on=["EIN", "PLAN_NUMBER"], how="left")

        # Derived "deferred lives" metrics.
        tv_ct = df["term_vested_count"]
        tv_ft = df["term_vested_total_ft"]
        df["deferred_vested_count_sb"] = tv_ct
        df["deferred_vested_funding_target_sb"] = tv_ft
        df["deferred_vested_count_5500"] = df.get("RTD_SEP_PARTCP_FUT_CNT")
        df["deferred_share_of_headcount"] = (tv_ct / df["total_count"]).where(df["total_count"] > 0)
        df["deferred_share_of_funding_target"] = (tv_ft / df["total_total_ft"]).where(df["total_total_ft"] > 0)
        df["avg_funding_target_per_deferred_life"] = (tv_ft / tv_ct).where(tv_ct > 0)
        # Retired (annuitant) comparison points, useful context for de-risking work.
        df["retired_share_of_headcount"] = (df["retired_count"] / df["total_count"]).where(df["total_count"] > 0)
        df["annuitant_plus_deferred_share_ft"] = (
            (df["retired_total_ft"] + tv_ft) / df["total_total_ft"]
        ).where(df["total_total_ft"] > 0)

        # Column ordering: identifiers first, then deferred-lives headline metrics.
        head = [
            "EIN", "PLAN_NUMBER", "PLAN_YEAR", "SPONSOR_DFE_NAME", "PLAN_NAME",
            "INDUSTRY_SECTOR", "BUSINESS_CODE", "ACTUARY_FIRM_NAME",
            "deferred_vested_count_sb", "deferred_vested_funding_target_sb",
            "deferred_vested_count_5500",
            "deferred_share_of_headcount", "deferred_share_of_funding_target",
            "avg_funding_target_per_deferred_life",
            "tv_payments_next10", "tv_payments_total", "tv_payment_confidence",
            "annuitant_plus_deferred_share_ft",
        ]
        ordered_cols = [c for c in head if c in df.columns] + [c for c in df.columns if c not in head]
        df = df[ordered_cols].sort_values("deferred_vested_funding_target_sb", ascending=False, na_position="last")

    streams_df = pd.DataFrame(streams)
    return df, streams_df


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--year", type=int, default=2024)
    ap.add_argument("--out-dir", type=Path, default=REPO_ROOT / "data_output")
    args = ap.parse_args()

    df, streams_df = build(args.year)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    main_path = args.out_dir / f"deferred_lives_top100_{args.year}.csv"
    stream_path = args.out_dir / f"deferred_lives_payment_streams_{args.year}.csv"
    df.to_csv(main_path, index=False)
    streams_df.to_csv(stream_path, index=False)

    print(f"Wrote {len(df)} plans -> {main_path}")
    print(f"Wrote {len(streams_df)} plan-year payment rows -> {stream_path}")
    if not df.empty:
        n_l3 = df["term_vested_count"].notna().sum()
        n_l3_ok = (df["line3_validates"] == True).sum()  # noqa: E712
        n_ebp = df["ebp_table_found"].sum()
        n_ebp_hi = (df["tv_payment_confidence"] == "high").sum()
        n_age = df["tv_retirement_age"].notna().sum()
        print(f"  line 3b parsed: {n_l3}/{len(df)}  (validated: {n_l3_ok})")
        print(f"  line 26b table found: {n_ebp}/{len(df)}  (TV column high-confidence: {n_ebp_hi})")
        print(f"  TV retirement age found: {n_age}/{len(df)}")


if __name__ == "__main__":
    main()
