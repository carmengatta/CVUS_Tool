"""Compile terminated-vested ("deferred vested") actuarial assumptions from the
saved top-100 Form 5500 / Schedule SB attachments.

For every plan whose SB attachments contain extractable text this:
  1. Builds an "assumptions corpus" -- the text of the Statement of Actuarial
     Assumptions / Methods section(s) plus the Summary of Plan Provisions
     section(s) (best-effort boundaries; falls back to the whole SB-attachments
     text when those headers aren't found).
  2. Harvests "evidence blocks": every passage that mentions a terminated-vested
     term (and the lines that follow a sub-header that does), excluding the
     line 3b / line 26b boilerplate. Each block is tagged with the assumption
     categories it touches.
  3. Parses what is reliably machine-readable into a per-plan structured row
     (assumed benefit-commencement age, lump-sum availability / election split,
     normal form of payment, married %, spouse age difference, lump-sum / 417(e)
     basis flag, mortality basis, cash-balance / PPA-VPA component, ...).

Reads:  pdf_extraction/form5500_chunked_{year}/*_chunks.json
        data_output/yearly/db_plans_{year}.parquet
Writes: data_output/tv_assumptions_evidence_{year}.csv   (one row per plan x evidence block)
        data_output/tv_assumptions_top100_{year}.csv     (one row per plan, structured fields)

Note: the prose is messy and varies a lot by actuarial firm, so the structured
fields are best-effort and frequently blank -- the evidence file is the workhorse;
it carries the raw text so nothing is lost. The ~22 plans whose SB attachments are
scanned images (no extractable text) are flagged and would need OCR.

Usage:
    python pdf_extraction/extract_tv_assumptions.py --year 2024
"""

import argparse
import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent

# A terminated-vested participant, written every way the actuaries write it.
TV_TERMS = (
    r"terminated[\s\-]+vested|vested[\s\-]+terminated"
    r"|deferred[\s\-]+vested|vested[\s\-]+deferred"
    r"|separated[\s\-]+vested|vested[\s\-]+separat\w+"
    r"|vested[\s\-]+termination"
    r"|deferred[\s\-]+(?:member|participant|pension(?:er)?|annuitant)"
    r"|term(?:\.|inated)?\s+vest(?:ed)?\s+(?:participant|member|employee)"
)
_RE_TV = re.compile(TV_TERMS, re.IGNORECASE)

# Category -> pattern that, if found inside an evidence block, tags it with that category.
CATEGORY_PATTERNS: List[Tuple[str, re.Pattern]] = [
    ("commencement_age", re.compile(r"\bretire(?:s|ment|d)?\b|\bcommenc\w*|\bbegin(?:s|ning)?\s+benefit|\bpayable\s+at\b|\bdefer\w*\s+to\s+age|\bbenefit commencement\b|\bage\s+\d{2}\b", re.I)),
    ("form_of_payment", re.compile(r"\bform of payment\b|\boptional form|\bnormal form\b|\blump[\s\-]?sum\b|\bjoint and survivor\b|\bJ&S\b|\bsingle life\b|\blife annuity\b|\bcertain (?:and|&) life\b|\bsocial security (?:level|leveling)\b", re.I)),
    ("lump_sum_basis", re.compile(r"\b417\(e\)\b|\bapplicable mortalit\w+|\bapplicable interest\b|\bsegment rate\b|\blump[\s\-]?sum.{0,40}(?:rate|mortalit|basis|interest)", re.I)),
    ("spouse_assumption", re.compile(r"\bspouse\b|\bmarried\b|\bmarriage\b|\bsurviving spouse\b|\bhusband\b|\bbeneficiar", re.I)),
    ("mortality", re.compile(r"\bmortalit\w+|Pr[ieI][\s\-]?20\d\d|RP[\s\-]?20\d\d|RPH[\s\-]?20\d\d|\bgenerational\b|MP[\s\-]?20\d\d|\bscale\s+MP\b|\bMILES\b|\bstatic\b", re.I)),
    ("cash_balance_ppa", re.compile(r"\bcash[\s\-]?balance\b|\baccount balance\b|\bPPA/VPA\b|\bunannuitized\b|\binterest credit\w*|\bcrediting rate\b|\bpay credit\b|\bhypothetical account\b", re.I)),
    ("death_benefit", re.compile(r"\bdeath benefit\b|\bpre-?retirement death\b|\bQPSA\b|\bpre-?retirement survivor\b", re.I)),
    ("expenses", re.compile(r"\bexpense load\b|\badministrative expense|\bPBGC premium", re.I)),
    ("data_treatment", re.compile(r"\bmissing\b|\blocated after\b|\bdeemed\b|\breasonable attempt|\bdata (?:was|were) (?:provided|supplied|reviewed)", re.I)),
]

# Boilerplate that mentions "terminated vested" but is NOT an assumption.
_RE_BOILERPLATE = re.compile(
    r"[Ff]or\s*terminated[\s\-]+vested participants[\s.…]+[\d,]"   # SB line 3b row
    r"|Active Participants?\s+Terminated[\s\-]+Vested\s+Retired"   # SB line 26b header
    r"|Terminated[\s\-]+Vested\s+Retired Participants",
    re.IGNORECASE,
)


def extract_ein_pn_from_filename(filename: str) -> Tuple[Optional[str], Optional[str]]:
    m = re.search(r"(\d{9})_(\d{3})_\d{4}", filename)
    return (m.group(1), m.group(2)) if m else (None, None)


def _norm_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def _sb_attachments_text(chunks: List[dict]) -> str:
    txt = "\n".join(c.get("raw_text", "") for c in chunks if c.get("schedule_type") == "SB_ATTACHMENTS")
    # Repair OCR'd thousands separators ("1, 234" -> "1,234") for cleaner number reads.
    return re.sub(r"(?<=\d),\s+(?=\d)", ",", txt)


def _categorize(block: str) -> List[str]:
    return [name for name, pat in CATEGORY_PATTERNS if pat.search(block)]


def _harvest_evidence(corpus: str) -> List[Dict]:
    """Return de-duplicated evidence blocks: passages mentioning a terminated-vested
    term, with a window of context."""
    blocks: List[Tuple[int, int]] = []
    for m in _RE_TV.finditer(corpus):
        start = max(0, m.start() - 150)
        # extend forward to a likely block end
        tail = corpus[m.end(): m.end() + 1100]
        # cut at the next document/section marker if it appears in the tail
        cut = re.search(r"Plan Name:|SCHEDULE SB ATTACHMENTS|Schedule SB,?\s*(?:Part|[Ll]ine)|Proprietary and Confidential|Page \d+ of \d+", tail)
        end = m.end() + (cut.start() if cut else len(tail))
        blocks.append((start, end))
    # merge overlapping windows
    blocks.sort()
    merged: List[List[int]] = []
    for s, e in blocks:
        if merged and s <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], e)
        else:
            merged.append([s, e])
    out: List[Dict] = []
    seen = set()
    for s, e in merged:
        raw = corpus[s:e]
        text = _norm_ws(raw)
        if len(text) < 20 or _RE_BOILERPLATE.search(text):
            continue
        key = re.sub(r"[^a-z0-9]", "", text.lower())[:400]
        if key in seen:
            continue
        seen.add(key)
        cats = _categorize(text)
        out.append({"categories": ",".join(cats) if cats else "general", "text": text})
    return out


# ---------------------------------------------------------------------------
# Structured parsing from the harvested blocks
# ---------------------------------------------------------------------------

def _parse_structured(blocks: List[Dict]) -> Dict:
    out: Dict = {
        "tv_commencement_age": None,
        "tv_commencement_age_note": None,
        "tv_immediate_if_past_age": None,
        "tv_lump_sum_available": None,
        "tv_lump_sum_election": None,
        "tv_normal_form": None,
        "married_pct": None,
        "spouse_age_diff_note": None,
        "lump_sum_417e_basis": None,
        "has_cash_balance_or_ppa_vpa": None,
        "cash_balance_crediting_rate": None,
        "mortality_table": None,
        "mortality_generational": None,
        "categories_present": None,
        "n_tv_evidence_blocks": len(blocks),
    }
    all_text = " || ".join(b["text"] for b in blocks)
    cats = sorted({c for b in blocks for c in b["categories"].split(",")})
    out["categories_present"] = ",".join(cats)

    # --- benefit commencement / retirement age ---
    age_block = next((b["text"] for b in blocks if "commencement_age" in b["categories"]), None)
    if age_block:
        # "retire at age 65", "commence ... at age 60", "begin benefits at age 64", "deferred to age 62"
        ages = re.findall(r"(?:age|at age|age of|by age|to age)\s*(\d{2})\b", age_block)
        ages = [int(a) for a in ages if 50 <= int(a) <= 72]
        # also patterns like "Terminated Vested Participants Age 64" or "Terminated vested participants 63."
        if not ages:
            m = re.search(r"(?:" + TV_TERMS + r")[^.\n]{0,40}?\b(\d{2})\b", age_block, re.I)
            if m and m.group(1) and 50 <= int(m.group(1)) <= 72:
                ages = [int(m.group(1))]
        if ages:
            # most-mentioned age wins; ties -> the lowest (earliest assumed commencement)
            from collections import Counter
            cnt = Counter(ages)
            best = sorted(cnt, key=lambda a: (-cnt[a], a))[0]
            out["tv_commencement_age"] = best
        out["tv_commencement_age_note"] = age_block[:500]
        if re.search(r"immediately,?\s*if (?:over|older than|past)|or immediately if", age_block, re.I):
            out["tv_immediate_if_past_age"] = True

    # --- lump-sum availability & election split ---
    if re.search(r"lump[\s\-]?sum", all_text, re.I):
        out["tv_lump_sum_available"] = True
        frags: List[str] = []
        for b in blocks:
            t = b["text"]
            # "50% of [the remaining] terminated/deferred vested ... elect a lump sum / annuity"
            frags += re.findall(
                r"\d{1,3}(?:\.\d+)?\s*%\s*of\s+(?:the\s+)?(?:remaining\s+)?(?:terminated[\s\-]+vested|deferred[\s\-]+vested|vested[\s\-]+terminated)[^.;]{0,80}?(?:elect|assumed to (?:elect|take|receive|commence))[^.;]{0,40}?(?:lump[\s\-]?sum|annuity|single (?:sum|life)|joint and survivor)",
                t, re.I)
            # "X% Immediate/Deferred Lump Sum / Annuity" election-table rows
            frags += re.findall(r"\d{1,3}(?:\.\d+)?\s*%\s*(?:Immediate|Deferred(?:\s+to\s+Age\s*\d{2})?)\s+(?:Lump\s+Sum|Annuity)", t, re.I)
        if frags:
            # de-dupe preserving order
            seen_f, uniq = set(), []
            for f in frags:
                k = _norm_ws(f).lower()
                if k not in seen_f:
                    seen_f.add(k); uniq.append(_norm_ws(f))
            out["tv_lump_sum_election"] = "; ".join(uniq[:12])

    # --- normal form of payment (only if a recognizable form is named) ---
    _FORM_KW = r"(single life annuity|life annuity(?: with[^.;]{0,40})?|\d{1,3}\s*%?\s*joint and survivor[^.;]{0,30}|joint and (?:\d{1,3}%? )?survivor[^.;]{0,30}|\d{1,2}[\s\-]?year certain (?:and|&) life[^.;]{0,20}|certain (?:and|&) life|level income|social security level\w*)"
    for b in blocks:
        t = b["text"]
        m = re.search(r"normal form(?:\s+of payment)?[^.;]{0,40}?" + _FORM_KW, t, re.I)
        if m:
            out["tv_normal_form"] = _norm_ws(m.group(1))
            break

    # --- married % and spouse age difference ---
    sp_block = next((b["text"] for b in blocks if "spouse_assumption" in b["categories"]), None)
    if sp_block:
        # "% Married 85%" / "85% married" / "85% of participants are assumed married" -- % adjacent to the word
        mm = (re.search(r"%\s*married\D{0,20}?(\d{1,3})\s*%", sp_block, re.I)
              or re.search(r"(\d{1,3})\s*%\s*(?:of\s+(?:participants|members|employees|males|men)\s+)?(?:are\s+)?(?:assumed\s+(?:to be\s+)?)?married\b", sp_block, re.I)
              or re.search(r"\bmarried\b\s*[:=]?\s*(\d{1,3})\s*%", sp_block, re.I))
        if mm:
            v = int(mm.group(1))
            if 0 <= v <= 100:
                out["married_pct"] = v
        _agew = r"(?:\d+|one|two|three|four|five|six)"
        sm = (re.search(r"(?:males?|men|husbands?)\s+(?:are\s+)?(?:assumed\s+(?:to be\s+)?)?" + _agew + r"\s+years?\s+(?:older|younger)\b[^.;]{0,30}", sp_block, re.I)
              or re.search(r"(?:female\s+)?spouses?\s+(?:are\s+|is\s+)?(?:assumed\s+(?:to be\s+)?)?" + _agew + r"\s+years?\s+(?:older|younger)\b[^.;]{0,30}", sp_block, re.I)
              or re.search(r"spouse[^.;]{0,30}?" + _agew + r"\s+years?\s+(?:older|younger)\s+than\b[^.;]{0,20}", sp_block, re.I))
        if sm:
            out["spouse_age_diff_note"] = _norm_ws(sm.group(0))

    # --- lump-sum / 417(e) basis flag ---
    if re.search(r"417\(e\)|applicable mortalit|applicable (?:segment )?interest|prescribed (?:lump[\s\-]?sum )?(?:rate|mortalit)", all_text, re.I):
        out["lump_sum_417e_basis"] = True

    # --- cash balance / PPA-VPA component ---
    if re.search(r"cash[\s\-]?balance|account balance|PPA/VPA|unannuitized|hypothetical account", all_text, re.I):
        out["has_cash_balance_or_ppa_vpa"] = True
        cr = re.search(r"(?:interest credit\w*|crediting rate)[^.;]{0,60}?(\d{1,2}(?:\.\d+)?\s*%)", all_text, re.I)
        if cr:
            out["cash_balance_crediting_rate"] = _norm_ws(cr.group(0))

    # --- mortality basis (rough) ---
    mt = re.search(r"\b(Pr[ieI][\s\-]?2012|Pr[ieI][\s\-]?2021|PriH[\s\-]?2012|RP[\s\-]?2014|RP[\s\-]?2006|RPH[\s\-]?2014|MILES|IRS[\s\-]?prescribed|417\(e\)\s+table)\b", all_text, re.I)
    if mt:
        out["mortality_table"] = _norm_ws(mt.group(1))
    if re.search(r"generational", all_text, re.I):
        out["mortality_generational"] = True
    elif re.search(r"\bstatic\b", all_text, re.I):
        out["mortality_generational"] = False

    return out


# ---------------------------------------------------------------------------
# Parquet join
# ---------------------------------------------------------------------------

_PARQUET_COLS = [
    "EIN", "PLAN_NUMBER", "PLAN_NAME", "SPONSOR_DFE_NAME", "INDUSTRY_SECTOR",
    "ACTUARY_FIRM_NAME", "ACK_ID_5500", "TERM_LIABILITY", "TOTAL_LIABILITY",
    "RTD_SEP_PARTCP_FUT_CNT",
]


def _load_parquet_lookup(year: int) -> pd.DataFrame:
    path = REPO_ROOT / "data_output" / "yearly" / f"db_plans_{year}.parquet"
    if not path.exists():
        raise SystemExit(f"Parquet not found: {path}")
    df = pd.read_parquet(path)
    cols = [c for c in _PARQUET_COLS if c in df.columns]
    df = df[cols].copy()
    df["EIN"] = df["EIN"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(9)
    df["PLAN_NUMBER"] = df["PLAN_NUMBER"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(3)
    return df.drop_duplicates(subset=["EIN", "PLAN_NUMBER"])


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def build(year: int):
    chunk_dir = REPO_ROOT / "pdf_extraction" / f"form5500_chunked_{year}"
    chunk_files = sorted(chunk_dir.glob("*_chunks.json"))
    if not chunk_files:
        raise SystemExit(f"No chunk files in {chunk_dir}")
    lookup = _load_parquet_lookup(year)

    plan_rows: List[Dict] = []
    evidence_rows: List[Dict] = []
    for cf in chunk_files:
        ein, pn = extract_ein_pn_from_filename(cf.name)
        try:
            chunks = json.loads(cf.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            print(f"  ! could not read {cf.name}: {exc}")
            continue
        sb_text = _sb_attachments_text(chunks)
        has_text = bool(sb_text.strip())

        # Harvest from the full SB-attachments text -- the terminated-vested anchor
        # plus the boilerplate filter do the narrowing; section headers are too
        # inconsistent to rely on for boundaries.
        blocks = _harvest_evidence(sb_text) if has_text else []
        struct = _parse_structured(blocks) if blocks else {"n_tv_evidence_blocks": 0}

        rec: Dict = {
            "chunk_file": cf.name, "EIN": ein, "PLAN_NUMBER": pn, "PLAN_YEAR": year,
            "has_extractable_sb_text": has_text,
        }
        rec.update(struct)
        plan_rows.append(rec)
        for i, b in enumerate(blocks, 1):
            evidence_rows.append({
                "EIN": ein, "PLAN_NUMBER": pn, "PLAN_YEAR": year,
                "block_no": i, "categories": b["categories"], "text": b["text"],
            })

    plans = pd.DataFrame(plan_rows)
    if not plans.empty:
        plans["EIN"] = plans["EIN"].astype(str).str.zfill(9)
        plans["PLAN_NUMBER"] = plans["PLAN_NUMBER"].astype(str).str.zfill(3)
        plans = plans.merge(lookup, on=["EIN", "PLAN_NUMBER"], how="left")
        head = ["EIN", "PLAN_NUMBER", "PLAN_YEAR", "SPONSOR_DFE_NAME", "PLAN_NAME",
                "INDUSTRY_SECTOR", "ACTUARY_FIRM_NAME", "has_extractable_sb_text",
                "n_tv_evidence_blocks", "categories_present",
                "tv_commencement_age", "tv_immediate_if_past_age",
                "tv_lump_sum_available", "tv_lump_sum_election", "tv_normal_form",
                "married_pct", "spouse_age_diff_note", "lump_sum_417e_basis",
                "has_cash_balance_or_ppa_vpa", "cash_balance_crediting_rate",
                "mortality_table", "mortality_generational",
                "tv_commencement_age_note", "TERM_LIABILITY", "RTD_SEP_PARTCP_FUT_CNT", "ACK_ID_5500"]
        ordered = [c for c in head if c in plans.columns] + [c for c in plans.columns if c not in head]
        plans = plans[ordered].sort_values(["has_extractable_sb_text", "n_tv_evidence_blocks"], ascending=[False, False])

    evidence = pd.DataFrame(evidence_rows)
    if not evidence.empty:
        evidence["EIN"] = evidence["EIN"].astype(str).str.zfill(9)
        evidence["PLAN_NUMBER"] = evidence["PLAN_NUMBER"].astype(str).str.zfill(3)
        ev_lookup = lookup[["EIN", "PLAN_NUMBER", "SPONSOR_DFE_NAME", "PLAN_NAME"]]
        evidence = evidence.merge(ev_lookup, on=["EIN", "PLAN_NUMBER"], how="left")
        evidence = evidence[["EIN", "PLAN_NUMBER", "SPONSOR_DFE_NAME", "PLAN_NAME",
                             "PLAN_YEAR", "block_no", "categories", "text"]]
    return plans, evidence


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--year", type=int, default=2024)
    ap.add_argument("--out-dir", type=Path, default=REPO_ROOT / "data_output")
    args = ap.parse_args()

    plans, evidence = build(args.year)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    plans_path = args.out_dir / f"tv_assumptions_top100_{args.year}.csv"
    ev_path = args.out_dir / f"tv_assumptions_evidence_{args.year}.csv"
    plans.to_csv(plans_path, index=False)
    evidence.to_csv(ev_path, index=False)

    print(f"Wrote {len(plans)} plans -> {plans_path}")
    print(f"Wrote {len(evidence)} evidence blocks -> {ev_path}")
    if not plans.empty:
        n_text = int(plans["has_extractable_sb_text"].sum())
        n_blocks = int(plans["n_tv_evidence_blocks"].gt(0).sum())
        print(f"  plans with extractable SB-attachment text: {n_text}/{len(plans)}")
        print(f"  plans with >=1 terminated-vested evidence block: {n_blocks}/{len(plans)}")
        for col in ["tv_commencement_age", "tv_lump_sum_available", "has_cash_balance_or_ppa_vpa", "married_pct", "lump_sum_417e_basis"]:
            print(f"  {col} populated: {int(plans[col].notna().sum())}")


if __name__ == "__main__":
    main()
