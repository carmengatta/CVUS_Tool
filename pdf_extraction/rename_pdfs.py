"""
Rename Form 5500 PDFs from ACK_ID format to human-readable format.

Format: {SPONSOR}_{EIN}_{PN}_{YEAR}.pdf
Example: RTX_CORPORATION_060570975_041_2024.pdf
"""

import os
import re
import pandas as pd
from pathlib import Path


def sanitize_filename(name: str, max_length: int = 50) -> str:
    """
    Sanitize sponsor name for use in filename.
    - Remove/replace special characters
    - Replace spaces with underscores
    - Truncate if too long
    """
    if pd.isna(name) or not name:
        return "UNKNOWN_SPONSOR"
    
    # Convert to uppercase
    name = str(name).upper().strip()
    
    # Remove common suffixes to shorten
    suffixes_to_remove = [
        ", OPERATING AS GE AEROSPACE",
        " AND CONSOLIDATED SUBSIDIARIES",
        ", INC.", " INC.", " INC",
        ", LLC", " LLC",
        ", L.P.", " L.P.",
        " CORPORATION", " CORP.",
        " COMPANY", " CO.",
        " LIMITED", " LTD.",
    ]
    for suffix in suffixes_to_remove:
        name = name.replace(suffix.upper(), "")
    
    # Replace problematic characters
    name = re.sub(r'[<>:"/\\|?*\']', '', name)  # Remove invalid filename chars
    name = re.sub(r'[,.\-&]+', '_', name)        # Replace punctuation with underscore
    name = re.sub(r'\s+', '_', name)             # Replace whitespace with underscore
    name = re.sub(r'_+', '_', name)              # Collapse multiple underscores
    name = name.strip('_')                        # Remove leading/trailing underscores
    
    # Truncate if too long
    if len(name) > max_length:
        name = name[:max_length].rstrip('_')
    
    return name if name else "UNKNOWN_SPONSOR"


def rename_pdfs(pdf_dir: str, data_path: str, year: int = 2024, dry_run: bool = False):
    """
    Rename PDFs from ACK_ID to SPONSOR_EIN_PN_YEAR format.
    
    Parameters
    ----------
    pdf_dir : str
        Directory containing the PDFs
    data_path : str
        Path to the parquet file with plan data
    year : int
        Filing year for the filename
    dry_run : bool
        If True, only print what would happen without renaming
    """
    # Load data
    df = pd.read_parquet(data_path)
    print(f"Loaded {len(df)} records from {data_path}")
    
    # Get PDF files
    pdf_files = [f for f in os.listdir(pdf_dir) if f.endswith('.pdf')]
    print(f"Found {len(pdf_files)} PDF files in {pdf_dir}\n")
    
    # Build mapping
    rename_log = []
    renamed_count = 0
    skipped_count = 0
    
    for pdf_file in pdf_files:
        # Extract ACK_ID from filename (remove .pdf and any " (1)" duplicates)
        ack_id = pdf_file.replace('.pdf', '').replace(' (1)', '')
        
        # Find matching record
        match = df[df['ACK_ID_SB'] == ack_id]
        
        if match.empty:
            print(f"⚠ No match for: {pdf_file}")
            skipped_count += 1
            rename_log.append({
                'old_name': pdf_file,
                'new_name': None,
                'status': 'NO_MATCH'
            })
            continue
        
        # Get plan info
        row = match.iloc[0]
        sponsor = sanitize_filename(row['SPONSOR_DFE_NAME'])
        ein = str(row['EIN']).strip()
        pn = str(row['PLAN_NUMBER']).strip().zfill(3)
        
        # Build new filename
        new_name = f"{sponsor}_{ein}_{pn}_{year}.pdf"
        
        # Check for duplicates (same sponsor might have multiple plans)
        old_path = Path(pdf_dir) / pdf_file
        new_path = Path(pdf_dir) / new_name
        
        # Handle duplicate filenames
        if new_path.exists() and old_path != new_path:
            # Add a suffix to make unique
            base = f"{sponsor}_{ein}_{pn}_{year}"
            counter = 2
            while (Path(pdf_dir) / f"{base}_{counter}.pdf").exists():
                counter += 1
            new_name = f"{base}_{counter}.pdf"
            new_path = Path(pdf_dir) / new_name
        
        rename_log.append({
            'old_name': pdf_file,
            'new_name': new_name,
            'sponsor': row['SPONSOR_DFE_NAME'],
            'ein': ein,
            'plan_number': pn,
            'retiree_count': row.get('RETIREE_COUNT', None),
            'status': 'RENAMED'
        })
        
        if dry_run:
            print(f"  {pdf_file}")
            print(f"  → {new_name}\n")
        else:
            os.rename(old_path, new_path)
            renamed_count += 1
    
    # Summary
    print("=" * 60)
    if dry_run:
        print(f"DRY RUN COMPLETE - No files were renamed")
        print(f"Would rename: {len(rename_log) - skipped_count} files")
    else:
        print(f"RENAME COMPLETE")
        print(f"Renamed: {renamed_count} files")
    print(f"Skipped (no match): {skipped_count} files")
    
    # Save log
    log_df = pd.DataFrame(rename_log)
    log_path = Path(pdf_dir) / f"_rename_log_{year}.csv"
    log_df.to_csv(log_path, index=False)
    print(f"\nRename log saved to: {log_path}")
    
    return rename_log


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Rename Form 5500 PDFs")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without renaming")
    parser.add_argument("--year", type=int, default=2024, help="Filing year")
    args = parser.parse_args()
    
    # Paths
    script_dir = Path(__file__).parent
    pdf_dir = script_dir / f"form5500_pdfs_{args.year}"
    data_path = script_dir.parent / "data_output" / "yearly" / f"db_plans_{args.year}.parquet"
    
    if not pdf_dir.exists():
        print(f"Error: PDF directory not found: {pdf_dir}")
        exit(1)
    
    if not data_path.exists():
        print(f"Error: Data file not found: {data_path}")
        exit(1)
    
    rename_pdfs(str(pdf_dir), str(data_path), year=args.year, dry_run=args.dry_run)
