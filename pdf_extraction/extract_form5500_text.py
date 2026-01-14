"""
Extract text from Form 5500 PDFs and chunk by schedule.

Outputs:
1. Raw text files: form5500_txt_{year}/{filename}.txt
2. Chunked by schedule: form5500_chunked_{year}/{filename}_chunks.json
3. Metadata CSV: form5500_txt_{year}/_extraction_metadata.csv

Usage:
    python extract_form5500_text.py --year 2024
    python extract_form5500_text.py --year 2024 --dry-run
"""

import os
import re
import json
import pdfplumber
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple, Optional


# =============================================================================
# TEXT EXTRACTION
# =============================================================================

def extract_pdf_text(pdf_path: Path) -> Tuple[str, int, List[str]]:
    """
    Extract text from PDF, page by page.
    
    Returns:
        Tuple of (full_text, page_count, list_of_page_texts)
    """
    pages = []
    page_texts = []
    
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            pages.append(f"\n\n--- PAGE {i} ---\n{text}")
            page_texts.append(text)
    
    full_text = "\n".join(pages)
    return full_text, len(page_texts), page_texts


# =============================================================================
# ACTUARIAL MORTALITY EXTRACTION (for accounting purposes)
# =============================================================================

# Keywords for mortality basis (ASC 715 / FAS 87 disclosure)
MORTALITY_KEYWORDS = [
    r'mortality\s+(basis|table|assumption)',
    r'pri-?\d{4}',  # Pri-2012, PRI2012, etc.
    r'mp-?\d{4}',   # MP-2021 mortality improvement scale
    r'rp-?\d{4}',   # RP-2014, etc.
]

def find_mortality_in_financial_statements(pdf_path: Path, page_texts: List[str]) -> Dict:
    """
    Find mortality basis ONLY within the Financial Statements section.
    This is the accounting disclosure (ASC 715) - not Schedule SB.
    
    Returns dict with:
    - pages: list of page numbers with mortality in Financial Statements
    - snippets: layout-preserved text excerpts
    """
    # First, identify which pages belong to FINANCIAL_STATEMENTS chunk
    financial_stmt_pages = set()
    current_schedule = 'PREAMBLE'
    
    for page_num, page_text in enumerate(page_texts, start=1):
        detected = detect_schedule_type(page_text)
        if detected != 'UNKNOWN':
            current_schedule = detected
        
        if current_schedule == 'FINANCIAL_STATEMENTS':
            financial_stmt_pages.add(page_num)
    
    # Now search only those pages for mortality
    mortality_pages = []
    extracted_snippets = []
    
    with pdfplumber.open(pdf_path) as pdf:
        for page_num in sorted(financial_stmt_pages):
            page = pdf.pages[page_num - 1]  # 0-indexed
            text = page.extract_text() or ""
            text_lower = text.lower()
            
            # Check for mortality keywords
            has_mortality = any(re.search(kw, text_lower) for kw in MORTALITY_KEYWORDS)
            
            if has_mortality:
                mortality_pages.append(page_num)
                # Extract with layout preservation for table-heavy pages
                try:
                    layout_text = page.extract_text(layout=True, x_tolerance=3, y_tolerance=3) or ""
                    lines = layout_text.split('\n')
                    for j, line in enumerate(lines):
                        if re.search(r'mortality', line.lower()):
                            # Get context: 2 lines before, 6 lines after
                            start = max(0, j - 2)
                            end = min(len(lines), j + 7)
                            snippet = '\n'.join(lines[start:end])
                            extracted_snippets.append({
                                'page': page_num,
                                'text': snippet
                            })
                            break  # One snippet per page
                except Exception:
                    pass
    
    return {
        'mortality_pages': mortality_pages,
        'mortality_snippets': extracted_snippets,
        'financial_stmt_page_range': format_page_range(list(financial_stmt_pages)) if financial_stmt_pages else None,
    }


# =============================================================================
# SCHEDULE DETECTION & CHUNKING
# =============================================================================

# Patterns to detect schedule boundaries
# These check the header area of a page to detect schedule type
# Priority order matters - more specific patterns should be checked first
SCHEDULE_PATTERNS = {
    # SB Attachments - actuarial detail pages (check BEFORE main SB)
    'SB_ATTACHMENTS': [
        r'SCHEDULE\s*SB\s+ATTACHMENTS',
        r'Schedule\s*SB\s+Attachments?',
        r'Schedule\s*SB\s+Attachment\s+\(Form',  # "Schedule SB Attachment (Form 5500)"
    ],
    # Main Schedule SB form pages
    'SCHEDULE_SB': [
        r'SCHEDULE\s*SB\s+Single-Employer',
        r'Schedule\s*SB\s+\(Form\s*5500\)',
        r'SCHEDULE\s*SB\s*\(Form\s*5500\)',
        r'Sch\s*edule\s*SB\s+\(Form\s*5500\)',  # OCR variant
    ],
    # Schedule of Assets (often 100+ pages of investment holdings)
    # Must be specific to actual asset listings, not TOC references
    'SCHEDULE_OF_ASSETS': [
        r'SCHEDULE\s*H.*LINE\s*4[iI]\s*[-–—]\s*SCHEDULE\s+OF\s+ASSETS',  # "SCHEDULE H, LINE 4i - SCHEDULE OF ASSETS"
        r'Schedule\s*H.*line\s*4[iI]\s*[-–—]\s*Schedule\s+of\s+Assets',
    ],
    # Financial Statements section (audited financials)
    'FINANCIAL_STATEMENTS': [
        r'Financial\s+Statements?\s+and\s+Supplemental',
        r'Financial\s+Statements?\s+and\s+Report\s+of\s+Independent',
        r'Combining\s+Financial\s+Statements?',
        r'Notes?\s+to\s+(Combining\s+)?Financial\s+Statements?',
        r'Independent\s+Auditor.?s?\s+Report',
        r'Certified\s+Public\s+Accountants?',
        r'Report\s+of\s+Independent.*Accountants?',
        r'Statements?\s+of\s+(Net\s+)?Assets?\s+Available\s+for\s+Benefits?',
        r'SCHEDULE\s*H.*LINE\s*4[jJ]\s*[-–—]\s*SCHEDULE\s+OF\s+REPORTABLE',  # Reportable transactions
        r'Schedule\s*H.*line\s*4[jJ].*schedule\s+of\s+reportable',
        r'^Contents\s*\n\s*Page',  # Table of contents within financial section
    ],
    'SCHEDULE_A': [
        r'SCHEDULE\s*A\s+Insurance',
        r'Schedule\s*A\s+Insurance',
        r'SCHEDULE\s*A\s*\n.*Insurance\s+Information',
    ],
    'SCHEDULE_R': [
        r'SCHEDULE\s*R\s+Retirement',
        r'Schedule\s*R\s+Retirement',
        r'SCHEDULE\s*R\s*\n.*Retirement\s+Plan\s+Information',
    ],
    'SCHEDULE_H': [
        r'SCHEDULE\s*H\s+Financial',
        r'Schedule\s*H\s+Financial',
        r'SCHEDULE\s*H\s*\n.*Financial\s+Information',
        r'Large\s+Plan\s+Financial\s+Information',
    ],
    'SCHEDULE_I': [
        r'SCHEDULE\s*I\s+Small',
        r'Schedule\s*I\s+Small',
        r'Small\s+Plan\s+Financial\s+Information',
    ],
    'SCHEDULE_C': [
        r'SCHEDULE\s*C\s+Service',
        r'Schedule\s*C\s+Service',
        r'Service\s+Provider\s+Information',
    ],
    'SCHEDULE_D': [
        r'SCHEDULE\s*D\s+DFE',
        r'Schedule\s*D\s+DFE',
        r'DFE/Participating\s+Plan\s+Information',
    ],
    'SCHEDULE_G': [
        r'SCHEDULE\s*G\s+Financial',
        r'Schedule\s*G\s+Financial',
        r'Financial\s+Transaction\s+Schedules',
    ],
    'SCHEDULE_MB': [
        r'SCHEDULE\s*MB',
        r'Schedule\s*MB',
        r'Multiemployer\s+Defined\s+Benefit\s+Plan',
    ],
    'FORM_5500': [
        r'Form\s*5500\s+Annual',
        r'Annual\s+Return/Report\s+of\s+Employee\s+Benefit\s+Plan',
    ],
}


def detect_schedule_type(page_text: str) -> str:
    """
    Detect which schedule a page belongs to based on content patterns.
    Checks the first ~1000 chars (header area) for schedule identification.
    Returns the schedule type or 'UNKNOWN'.
    """
    # Focus on the header area of the page
    header_text = page_text[:1000] if len(page_text) > 1000 else page_text
    
    # Check schedules in priority order
    # SB_ATTACHMENTS must be checked BEFORE SCHEDULE_SB (more specific first)
    # SCHEDULE_OF_ASSETS checked early since it's a distinct section
    priority_order = [
        'SB_ATTACHMENTS', 'SCHEDULE_SB', 'SCHEDULE_OF_ASSETS', 'FINANCIAL_STATEMENTS',
        'SCHEDULE_A', 'SCHEDULE_H', 'SCHEDULE_R', 
        'SCHEDULE_I', 'SCHEDULE_C', 'SCHEDULE_D', 'SCHEDULE_G', 
        'SCHEDULE_MB', 'FORM_5500'
    ]
    
    for schedule_type in priority_order:
        if schedule_type not in SCHEDULE_PATTERNS:
            continue
        for pattern in SCHEDULE_PATTERNS[schedule_type]:
            if re.search(pattern, header_text, re.IGNORECASE | re.DOTALL):
                return schedule_type
    
    return 'UNKNOWN'


def chunk_by_schedule(page_texts: List[str], filename: str) -> List[Dict]:
    """
    Group pages into chunks by schedule type.
    Consolidates all pages of the same schedule type into ONE chunk.
    
    Special handling:
    - Scanned Schedule SB pages (wet signature copies) that appear AFTER the 
      SB_ATTACHMENTS section are folded into SB_ATTACHMENTS, not SCHEDULE_SB.
    
    Returns list of chunk dictionaries with:
    - chunk_id, schedule_type, raw_text, page_range, page_count, char_count
    """
    # First pass: tag each page with its detected schedule type
    page_tags = []
    for page_num, page_text in enumerate(page_texts, start=1):
        detected = detect_schedule_type(page_text)
        page_tags.append({
            'page_num': page_num,
            'detected': detected,
            'text': page_text
        })
    
    # Second pass: handle scanned SB pages after SB_ATTACHMENTS
    # Once we've seen SB_ATTACHMENTS, any subsequent SCHEDULE_SB is likely
    # a scanned wet-signature copy and belongs with attachments
    seen_sb_attachments = False
    for tag in page_tags:
        if tag['detected'] == 'SB_ATTACHMENTS':
            seen_sb_attachments = True
        elif tag['detected'] == 'SCHEDULE_SB' and seen_sb_attachments:
            # Reclassify scanned SB as part of attachments
            tag['detected'] = 'SB_ATTACHMENTS'
    
    # Third pass: propagate schedule types to UNKNOWN pages
    # UNKNOWN pages belong to the most recent detected schedule
    current_schedule = 'PREAMBLE'
    for tag in page_tags:
        if tag['detected'] != 'UNKNOWN':
            current_schedule = tag['detected']
            tag['schedule_type'] = current_schedule
        else:
            tag['schedule_type'] = current_schedule
    
    # Fourth pass: consolidate by schedule type (one chunk per type)
    schedule_pages = {}  # {schedule_type: [(page_num, text), ...]}
    for tag in page_tags:
        stype = tag['schedule_type']
        if stype not in schedule_pages:
            schedule_pages[stype] = []
        schedule_pages[stype].append((tag['page_num'], tag['text']))
    
    # Build chunks in document order (by first page of each schedule)
    schedule_order = []
    for stype, pages in schedule_pages.items():
        first_page = min(p[0] for p in pages)
        schedule_order.append((first_page, stype))
    schedule_order.sort()
    
    # Create chunk objects
    chunks = []
    base_name = Path(filename).stem
    
    for _, stype in schedule_order:
        pages = schedule_pages[stype]
        page_nums = sorted([p[0] for p in pages])
        texts = [p[1] for p in sorted(pages, key=lambda x: x[0])]
        raw_text = "\n\n".join(texts)
        
        # Format page range (handles non-contiguous pages)
        page_range = format_page_range(page_nums)
        
        chunks.append({
            'chunk_id': f"{base_name}_{stype}",
            'schedule_type': stype,
            'page_range': page_range,
            'page_count': len(pages),
            'raw_text': raw_text,
            'char_count': len(raw_text),
        })
    
    return chunks


def format_page_range(page_nums: List[int]) -> str:
    """
    Format a list of page numbers into a readable range string.
    E.g., [1,2,3,5,6,8] -> "1-3, 5-6, 8"
    """
    if not page_nums:
        return ""
    
    page_nums = sorted(set(page_nums))
    ranges = []
    start = page_nums[0]
    end = page_nums[0]
    
    for num in page_nums[1:]:
        if num == end + 1:
            end = num
        else:
            ranges.append(f"{start}-{end}" if start != end else str(start))
            start = end = num
    
    ranges.append(f"{start}-{end}" if start != end else str(start))
    return ", ".join(ranges)


# =============================================================================
# MAIN EXTRACTION PIPELINE
# =============================================================================

def extract_all_pdfs(
    pdf_dir: Path,
    txt_dir: Path,
    chunk_dir: Path,
    year: int,
    dry_run: bool = False
) -> pd.DataFrame:
    """
    Extract text from all PDFs in directory.
    
    Returns metadata DataFrame.
    """
    pdf_files = sorted([f for f in os.listdir(pdf_dir) if f.endswith('.pdf')])
    print(f"Found {len(pdf_files)} PDF files in {pdf_dir}\n")
    
    if not dry_run:
        txt_dir.mkdir(parents=True, exist_ok=True)
        chunk_dir.mkdir(parents=True, exist_ok=True)
    
    metadata = []
    actuarial_data = []  # Collect actuarial assumption snippets
    success_count = 0
    error_count = 0
    
    for i, pdf_file in enumerate(pdf_files, start=1):
        pdf_path = pdf_dir / pdf_file
        txt_file = pdf_file.replace('.pdf', '.txt')
        chunk_file = pdf_file.replace('.pdf', '_chunks.json')
        
        print(f"[{i}/{len(pdf_files)}] Processing: {pdf_file}")
        
        try:
            # Extract text
            full_text, page_count, page_texts = extract_pdf_text(pdf_path)
            
            # Chunk by schedule
            chunks = chunk_by_schedule(page_texts, pdf_file)
            schedule_types = [c['schedule_type'] for c in chunks]
            
            # Find mortality basis in Financial Statements only (for accounting)
            mortality_info = find_mortality_in_financial_statements(pdf_path, page_texts)
            mortality_pages = mortality_info.get('mortality_pages', [])
            
            if dry_run:
                mort_str = f", mortality in FS pages {mortality_pages}" if mortality_pages else ""
                print(f"    → {page_count} pages, {len(chunks)} chunks: {schedule_types}{mort_str}")
            else:
                # Write raw text
                txt_path = txt_dir / txt_file
                txt_path.write_text(full_text, encoding='utf-8')
                
                # Write chunks
                chunk_path = chunk_dir / chunk_file
                chunk_path.write_text(json.dumps(chunks, indent=2), encoding='utf-8')
                
                print(f"    → {page_count} pages, {len(chunks)} chunks, mortality in FS: {mortality_pages or 'none'}")
            
            # Collect mortality snippets for separate output
            for snippet in mortality_info.get('mortality_snippets', []):
                actuarial_data.append({
                    'pdf_file': pdf_file,
                    'page': snippet['page'],
                    'mortality_text': snippet['text'],
                })
            
            # Collect metadata
            metadata.append({
                'pdf_file': pdf_file,
                'txt_file': txt_file,
                'chunk_file': chunk_file,
                'page_count': page_count,
                'chunk_count': len(chunks),
                'schedules_found': ', '.join(sorted(set(schedule_types))),
                'fs_mortality_pages': ', '.join(map(str, mortality_pages)) if mortality_pages else None,
                'raw_text_chars': len(full_text),
                'pdf_size_kb': round(pdf_path.stat().st_size / 1024, 1),
                'status': 'SUCCESS',
                'error': None,
                'extracted_at': datetime.now().isoformat(),
            })
            success_count += 1
            
        except Exception as e:
            print(f"    ✗ ERROR: {e}")
            metadata.append({
                'pdf_file': pdf_file,
                'txt_file': None,
                'chunk_file': None,
                'page_count': None,
                'chunk_count': None,
                'schedules_found': None,
                'fs_mortality_pages': None,
                'raw_text_chars': None,
                'pdf_size_kb': round(pdf_path.stat().st_size / 1024, 1) if pdf_path.exists() else None,
                'status': 'ERROR',
                'error': str(e),
                'extracted_at': datetime.now().isoformat(),
            })
            error_count += 1
    
    # Create metadata DataFrame
    meta_df = pd.DataFrame(metadata)
    
    # Summary
    print("\n" + "=" * 60)
    if dry_run:
        print("DRY RUN COMPLETE - No files written")
    else:
        print("EXTRACTION COMPLETE")
        
        # Save metadata
        meta_path = txt_dir / f"_extraction_metadata_{year}.csv"
        meta_df.to_csv(meta_path, index=False)
        print(f"\nMetadata saved to: {meta_path}")
        
        # Save actuarial/mortality snippets for accounting purposes
        if actuarial_data:
            actuarial_df = pd.DataFrame(actuarial_data)
            actuarial_path = txt_dir / f"_actuarial_mortality_snippets_{year}.csv"
            actuarial_df.to_csv(actuarial_path, index=False)
            print(f"Actuarial snippets saved to: {actuarial_path}")
            print(f"  → {len(actuarial_data)} mortality excerpts from {actuarial_df['pdf_file'].nunique()} PDFs")
    
    print(f"\nSuccess: {success_count}")
    print(f"Errors:  {error_count}")
    print(f"Total:   {len(pdf_files)}")
    
    # Schedule summary
    if success_count > 0:
        all_schedules = meta_df['schedules_found'].dropna().str.split(', ').explode()
        schedule_counts = all_schedules.value_counts()
        print(f"\nSchedules detected across all PDFs:")
        for sched, count in schedule_counts.items():
            print(f"  {sched}: {count} files")
        
        # Mortality detection summary
        mortality_found = meta_df['fs_mortality_pages'].notna().sum()
        print(f"\nMortality basis found in Financial Statements: {mortality_found}/{success_count} PDFs")
    
    return meta_df


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Extract text from Form 5500 PDFs")
    parser.add_argument("--year", type=int, default=2024, help="Filing year")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing files")
    args = parser.parse_args()
    
    # Paths
    script_dir = Path(__file__).parent
    pdf_dir = script_dir / f"form5500_pdfs_{args.year}"
    txt_dir = script_dir / f"form5500_txt_{args.year}"
    chunk_dir = script_dir / f"form5500_chunked_{args.year}"
    
    if not pdf_dir.exists():
        print(f"Error: PDF directory not found: {pdf_dir}")
        exit(1)
    
    extract_all_pdfs(pdf_dir, txt_dir, chunk_dir, args.year, dry_run=args.dry_run)
