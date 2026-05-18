# Private Pension Mortality Benchmark — Session Notes

**Goal:** Build a Club Vita-style mortality benchmark for the top 93 US private DB plans (2024 plan year), parallel to `references/2025-us-public-plan-mortality-benchmarking-results.pdf`.

---

## What's done

| Step | Output |
|---|---|
| Form 5500 PDF text extraction + schedule chunking | `pdf_extraction/extract_form5500_text.py` → `pdf_extraction/form5500_txt_2024/*.txt` (99 files), `_extraction_metadata_2024.csv`, `_actuarial_mortality_snippets_2024.csv` |
| Mortality field parser (regex) | `pdf_extraction/parse_mortality.py` → `_parsed_mortality_2024.csv`, `_plan_mortality_summary_2024.csv` (93 plans rolled up) |
| Profiled the 93 plans | 88% Pri-2012 (by participants), 61% MP-2021, 28% sponsor-adjusted, 10 substitute-mortality plans — perfect match with Schedule SB Code 3 |
| Review workbook for manual gap-fill | `pdf_extraction/form5500_txt_2024/_mortality_review_2024.xlsx` (3 sheets: Stats, Plan Review, Gaps Only=58 plans). Side-by-side: parsed fields + actual mortality language ±500 chars context, with empty `CORRECTED_*` columns to fill |
| EFAST2 PDF download script | `pdf_extraction/download_pdfs.py` — pulls from EFAST2's public S3 bucket via ACK_ID. Used to redownload the 5 PDFs with scanned/missing FS sections |
| Empty-page audit | `pdf_extraction/form5500_txt_2024/_extraction_quality_2024.csv` |

## Where we got stuck

JPMorgan's review file shows mortality language from **Schedule SB Part V (funding valuation)**, not the **Financial Statements / ASC 715 disclosure** that we want. Diagnosis: pages 79–93 of JPMorgan's PDF (and similar runs in 4 other plans) are scanned images — pdfplumber returns empty text.

5 plans are affected (already redownloaded into `pdf_extraction/form5500_pdfs_2024/`):
- JPMorgan Chase (163 empty pages)
- Cigna Holding (40)
- Abbott Laboratories (9)
- Disney / TWDC Enterprises 18 (9)
- State Farm (6)

To read the scanned pages we need OCR, which needs **Tesseract + Poppler** installed. UAC for the Tesseract install was cancelled in the last session — nothing got installed yet.

---

## Resume here

### 1. Install Tesseract + Poppler (two UAC prompts)
```powershell
winget install --id UB-Mannheim.TesseractOCR
winget install --id oschwartz10612.Poppler
```
Both are official upstream packages. Reversible via `winget uninstall`.

### 2. Add OCR fallback to the extraction pipeline
Recommended: use **OCRmyPDF** (cleanest — produces a text-layered PDF that the existing `extract_form5500_text.py` can re-ingest with no code changes).
```powershell
pip install ocrmypdf
```
Then loop over the 5 redownloaded PDFs:
```powershell
ocrmypdf --skip-text input.pdf input.pdf  # in-place; only OCRs pages without text
```
The `--skip-text` flag means it leaves digital-text pages alone and only OCRs the scanned ones, so it's safe to run on any PDF.

### 3. Re-extract text + re-parse
```powershell
python pdf_extraction/extract_form5500_text.py --year 2024
python pdf_extraction/parse_mortality.py --year 2024
```
(or scope to just the 5 affected plans by moving the others out of `form5500_pdfs_2024/` temporarily)

### 4. Rebuild the review workbook
```powershell
python pdf_extraction/build_mortality_review.py --year 2024
```
The Gaps Only sheet should now show real Financial Statements mortality language for the 5 fixed plans.

### 5. Manually fill the `CORRECTED_*` columns
Open `pdf_extraction/form5500_txt_2024/_mortality_review_2024.xlsx` → **Gaps Only** sheet (58 plans). Read each plan's `mortality_language` cell, fill in `CORRECTED_collar`, `CORRECTED_projection_method`, etc. where the parser missed.

### 6. Tighten `parse_mortality.py`
Hand the corrected workbook back to Claude — the regex tweaks get folded into the parser so next year's run is cleaner out of the gate. (This is Task #5.)

### 7. (Future) Build the LE@65 calculator
The headline scatter chart in the Club Vita deck (life expectancy @ 65, M vs F) requires evaluating the mortality table for each plan. Either:
- Use Club Vita's internal LE engine and feed it `_plan_mortality_summary_2024.csv` as input, OR
- Build a ~50-line standalone LE function from SOA's published Pri-2012 base rates + MP-2021 improvement matrix

---

## Key files

| Path | Purpose |
|---|---|
| `pdf_extraction/extract_form5500_text.py` | PDF → text + schedule chunking + FS mortality snippet capture |
| `pdf_extraction/parse_mortality.py` | Regex parser (the file to tighten in step 6) |
| `pdf_extraction/build_mortality_review.py` | Builds the Excel review workbook |
| `pdf_extraction/download_pdfs.py` | Pulls PDFs from EFAST2 S3 by ACK_ID |
| `pdf_extraction/form5500_txt_2024/_plan_mortality_summary_2024.csv` | 93-plan rolled-up parser output |
| `pdf_extraction/form5500_txt_2024/_mortality_review_2024.xlsx` | The review workbook |
| `pdf_extraction/form5500_txt_2024/_extraction_quality_2024.csv` | Empty-page audit |
| `pdf_extraction/form5500_pdfs_2024/` | 5 redownloaded source PDFs (rest of the directory was deleted from the working tree earlier) |
| `references/2025-us-public-plan-mortality-benchmarking-results.pdf` | Club Vita public report (template to mirror) |
| `substitute_mortality_analysis.md` / `.xlsx` | 2019→2024 longitudinal substitute-mortality analysis (already done; standalone section of the deck) |
| `mortality_outreach_opportunities.md` | Outreach tiers derived from the substitute-mortality work |

## Report structure (proposed)

Direct mirror of Club Vita's public deck, with private-specific swaps:

| Public deck cut | Private deck equivalent |
|---|---|
| Plan type (General/Teachers/Public Safety) | **Collar** (White/Blue/Mixed/None) |
| Region (NE/MW/SE/SW/W) | **Industry sector** (Mfg / Fin&Ins / Info / Utilities / etc.) |
| Pub-2010 G/T/S baseline reference | **Pri-2012 + RP-2014 + RP-2006** baselines |
| 4 sub-pies: amounts-weighted, scaling, set fwd/back, above/below median | Drop set-fwd/back & above/below median (uncommon in private). Replace with **substitute-mortality status** and **company-modified table Y/N** |

Plus a **Substitute Mortality Spotlight** and **Actuary Firm Influence** section — both are private-only stories you don't have in the public report.

---

*Last updated: end of conversation right before terminal close. To resume, say "pick up where we left off in SESSION_NOTES.md" and Claude will start with step 1 above.*
