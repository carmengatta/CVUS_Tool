"""
Download Form 5500 PDFs from the EFAST2 public S3 bucket.

The EFAST2 disclosure system serves filings as:
    https://efast2-filings-public.s3.amazonaws.com/prd/YYYY/MM/DD/{ACK_ID}.pdf

Where YYYY/MM/DD is derived from the leading timestamp of the ACK_ID.

Inputs:
    --plans path/to/plans.csv  (columns: ein, plan_number)
    --year  2024
OR
    --ein 134994650 --plan 001 --year 2024  (single plan)
OR
    --refill  (download the 5 plans known to have scanned/missing FS sections)

The script looks up ACK_ID_5500 from data_output/yearly/db_plans_{year}.parquet
and writes PDFs to pdf_extraction/form5500_pdfs_{year}/.

Usage:
    python pdf_extraction/download_pdfs.py --refill --year 2024
    python pdf_extraction/download_pdfs.py --ein 134994650 --plan 001 --year 2024
    python pdf_extraction/download_pdfs.py --plans my_targets.csv --year 2024
"""

import argparse
import re
import sys
import time
from pathlib import Path

import pandas as pd
import requests

S3_BASE = "https://efast2-filings-public.s3.amazonaws.com/prd"
HEADERS = {"User-Agent": "Form5500_Tool/1.0 (mortality benchmarking research)"}

# Plans whose Financial Statements section is scanned-image and needs OCR
REFILL_TARGETS = [
    ("134994650", "001"),  # JPMorgan Chase
    ("061059331", "001"),  # Cigna Holding
    ("360698440", "300"),  # Abbott Laboratories
    ("954545390", "014"),  # TWDC / Disney
    ("370533100", "001"),  # State Farm
]


def s3_url_from_ack(ack_id: str) -> str:
    """Construct the S3 URL from an ACK_ID (first 8 chars are YYYYMMDD)."""
    return f"{S3_BASE}/{ack_id[:4]}/{ack_id[4:6]}/{ack_id[6:8]}/{ack_id}.pdf"


def safe_filename(sponsor: str, ein: str, pn: str, year: int) -> str:
    """Match the existing filename convention: SPONSOR_EIN_PN_YEAR.pdf."""
    name = re.sub(r"[^A-Za-z0-9 ]+", "", sponsor or "UNKNOWN")
    name = re.sub(r"\s+", "_", name.strip()).upper()
    return f"{name}_{ein}_{pn}_{year}.pdf"


def lookup_filings(year: int, targets: list) -> pd.DataFrame:
    """Look up ACK_ID + sponsor name for each (ein, plan_number) target."""
    parquet = Path(__file__).parent.parent / "data_output" / "yearly" / f"db_plans_{year}.parquet"
    if not parquet.exists():
        sys.exit(f"Parquet not found: {parquet}")

    db = pd.read_parquet(parquet, columns=["EIN", "PLAN_NUMBER", "SPONSOR_DFE_NAME", "ACK_ID_5500"])
    db["EIN"] = db["EIN"].astype(str).str.zfill(9)
    db["PLAN_NUMBER"] = db["PLAN_NUMBER"].astype(str).str.zfill(3)

    rows = []
    for ein, pn in targets:
        ein = str(ein).zfill(9)
        pn = str(pn).zfill(3)
        m = db[(db["EIN"] == ein) & (db["PLAN_NUMBER"] == pn)]
        if m.empty:
            print(f"  ! no match in parquet: ein={ein} pn={pn}")
            continue
        r = m.iloc[0]
        rows.append({
            "ein": ein,
            "plan_number": pn,
            "sponsor": r["SPONSOR_DFE_NAME"],
            "ack_id": r["ACK_ID_5500"],
        })
    return pd.DataFrame(rows)


def download(url: str, dest: Path, chunk_size: int = 1 << 20) -> int:
    """Stream a file to disk; return bytes written."""
    with requests.get(url, headers=HEADERS, timeout=120, stream=True) as r:
        r.raise_for_status()
        size = 0
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    size += len(chunk)
        return size


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--year", type=int, default=2024)
    p.add_argument("--refill", action="store_true",
                   help="Download the 5 plans known to have scanned/missing FS sections")
    p.add_argument("--ein", help="Single plan: EIN")
    p.add_argument("--plan", help="Single plan: plan number")
    p.add_argument("--plans", help="CSV file with columns ein, plan_number")
    p.add_argument("--out-dir", help="Override output directory")
    p.add_argument("--force", action="store_true", help="Re-download even if file exists")
    args = p.parse_args()

    if args.refill:
        targets = REFILL_TARGETS
    elif args.ein and args.plan:
        targets = [(args.ein, args.plan)]
    elif args.plans:
        df = pd.read_csv(args.plans, dtype=str)
        targets = list(df[["ein", "plan_number"]].itertuples(index=False, name=None))
    else:
        sys.exit("Specify one of: --refill, --ein/--plan, or --plans")

    out_dir = Path(args.out_dir) if args.out_dir else Path(__file__).parent / f"form5500_pdfs_{args.year}"
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"Output dir: {out_dir}\n")

    filings = lookup_filings(args.year, targets)
    if filings.empty:
        sys.exit("No filings to download.")

    total_bytes = 0
    for i, row in enumerate(filings.itertuples(index=False), start=1):
        fname = safe_filename(row.sponsor, row.ein, row.plan_number, args.year)
        dest = out_dir / fname
        url = s3_url_from_ack(row.ack_id)

        if dest.exists() and not args.force:
            print(f"[{i}/{len(filings)}] {fname} — already exists ({dest.stat().st_size:,} bytes), skip")
            continue

        print(f"[{i}/{len(filings)}] {row.sponsor[:50]}")
        print(f"           {url}")
        start = time.time()
        try:
            size = download(url, dest)
            total_bytes += size
            print(f"           -> {fname} ({size:,} bytes, {time.time()-start:.1f}s)")
        except requests.HTTPError as e:
            print(f"           ! HTTP {e.response.status_code}: {e}")
        except Exception as e:
            print(f"           ! {type(e).__name__}: {e}")

    print(f"\nDone. Downloaded {total_bytes / 1e6:.1f} MB total.")


if __name__ == "__main__":
    main()
