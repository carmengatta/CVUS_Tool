"""
Parse structured mortality fields from raw Form 5500 Financial Statement snippets.

Reads:  form5500_txt_{year}/_actuarial_mortality_snippets_{year}.csv
Writes: form5500_txt_{year}/_parsed_mortality_{year}.csv          (one row per snippet)
        form5500_txt_{year}/_plan_mortality_summary_{year}.csv    (one row per plan)

Usage:
    python pdf_extraction/parse_mortality.py --year 2024
"""

import re
import argparse
import pandas as pd
from pathlib import Path
from typing import Optional, Tuple, Dict, List


# =============================================================================
# FILENAME PARSING
# =============================================================================

def extract_ein_pn_from_filename(filename: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Parse EIN and plan number from PDF filename.
    Pattern: SPONSOR_EIN_PN_YEAR.pdf  (e.g., "3M_410417775_002_2024.pdf")
    EIN is 9 digits, PN is 3 digits.
    """
    m = re.search(r'(\d{9})_(\d{3})_\d{4}\.pdf$', filename, re.IGNORECASE)
    if m:
        return m.group(1), m.group(2)
    return None, None


# =============================================================================
# BASE TABLE EXTRACTION
# =============================================================================

# Ordered patterns — more specific first
_BASE_TABLE_PATTERNS = [
    # RPH-2015 / RPH-2014 (rare)
    (r'RPH[\s-]*20(\d{2})', lambda m: f"RPH-20{m.group(1)}"),
    # RP-2006 (Boeing)
    (r'RP[\s-]*2006', lambda _: "RP-2006"),
    # RP-2014 / "Retirement Plan 2014"
    (r'(?:RP[\s-]*2014|Retirement\s+Plan\s+2014)', lambda _: "RP-2014"),
    # PriH-2012 (Caterpillar variant)
    (r'PriH[\s-]*2012', lambda _: "PriH-2012"),
    # Pri-2012 (dominant — many spelling variants: PRI-2012, Pri 2012, Pri-2012, Pre-2012 typo, etc.)
    (r'Pr[ieI][\s-]*2012', lambda _: "Pri-2012"),
    # Pri-2021 (Pacific Gas & Electric)
    (r'Pr[iI][\s-]*2021', lambda _: "Pri-2021"),
    # MILES (Mercer Industry Longevity Experience Study)
    (r'(?:MILES|Mercer\s+Industry\s+Longevity\s+Experience\s+Study)', lambda _: "MILES"),
    # IRS prescribed / 430(h)(3) tables
    (r'(?:IRS|Section\s+430\(h\)\(3\))\s+(?:prescribed|published)?\s*(?:generational\s+)?mortality\s+table',
     lambda _: "IRS Prescribed"),
    (r'prescribed\s+(?:IRS\s+)?(?:separate\s+)?(?:static\s+)?(?:annuitant|mortality)',
     lambda _: "IRS Prescribed"),
    # 417(e) tables (lump-sum equivalence — flag but don't treat as primary)
    (r'(?:IRC\s+(?:section\s+)?)?417\(e\)(?:\(3\))?\s+mortality', lambda _: "417(e)"),
    # Legacy tables
    (r'1971\s+(?:Group\s+Annuity|GAM|TPF&C)', lambda _: "1971 GAM"),
    (r'(?:71\s+GAM|GA71)', lambda _: "1971 GAM"),
    (r'1983\s+(?:Group\s+Annuity|GAM|GATT|Individual\s+Annuity|IAM)', lambda _: "1983 GAM"),
    (r'(?:1951\s+Group\s+Annuity|GAM.*Scale\s+C)', lambda _: "1951 GAM"),
    (r'1994\s+Uninsured\s+Pensioner', lambda _: "1994 UP"),
    (r'UP[\s-]*1984', lambda _: "UP-1984"),
    (r'UP[\s-]*84\b', lambda _: "UP-1984"),
    (r'1984\s+UNISEX\s+Pension\s+Mortality', lambda _: "UP-1984"),
]


def normalize_base_table(text: str) -> Optional[str]:
    """Extract and normalize the primary base mortality table from snippet text."""
    for pattern, resolver in _BASE_TABLE_PATTERNS:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            return resolver(m)
    # Custom / experience-based
    if re.search(r'(?:company|plan|sponsor)\s*(?:specific|custom)\s*mortality', text, re.IGNORECASE):
        return "Custom"
    if re.search(r'(?:experience[\s-]*based|based\s+on\s+(?:actual\s+)?(?:company|plan)\s+experience)', text, re.IGNORECASE):
        return "Custom"
    return None


# =============================================================================
# COLLAR ADJUSTMENT
# =============================================================================

def extract_collar_adjustment(text: str) -> Optional[str]:
    """Detect collar adjustment type."""
    t = text.lower()

    # "without collar or amount adjustments" / "no collar or amount"
    if re.search(r'(?:without|no)\s+collar\s+(?:or|and)\s+amount\s+adjust', t):
        return "No Collar"
    # "no collar adjustment" / "no collar"
    if re.search(r'no\s+collar(?:\s+adjust)?', t):
        return "No Collar"

    # Mixed / blended collar
    if re.search(r'(?:mixed\s+collar|blend(?:ed)?\s+\d+%?\s*white\s+collar\s+and\s+\d+%?\s*blue\s+collar'
                 r'|\d+%\s*white\s+collar\s+and\s+\d+%\s*blue\s+collar)', t):
        return "Mixed Collar"
    # Check for both white and blue mentioned together
    if re.search(r'white\s+collar', t) and re.search(r'blue\s+collar', t):
        return "Mixed Collar"

    # White collar
    if re.search(r'white\s+collar', t):
        return "White Collar"
    # Blue collar
    if re.search(r'blue\s+collar', t):
        return "Blue Collar"

    # Amounts-weighted (Chevron pattern: "amounts weighted base rates")
    if re.search(r'amounts?[\s-]*weight', t):
        return "Amount-Weighted"

    return None


# =============================================================================
# SCALING FACTOR
# =============================================================================

def extract_scaling_factor(text: str) -> Tuple[Optional[float], Optional[str]]:
    """
    Extract mortality scaling factor and its raw description.

    Patterns handled:
    - "90% Pri-2012"  / "90% of the Pri-2012"       → 0.90
    - "with a 0.94 multiplier"                       → 0.94
    - "increased 3.0%"  / "increased by 3%"          → 1.03
    - "decreased 1.0%"  / "reduced by 5%"            → 0.95 / 0.99
    - "5% decrease"  / "9% increase"                 → 0.95 / 1.09
    - "20% load"                                     → 1.20
    - "102% for females and 104% for males"          → uses average or first
    - "mortality multiplier"                         → direct multiplier
    """
    t = text

    # Pattern: "X% of [table]" or "X% Pri-" or "X% PRI-" (percentage applied to table)
    # Allow intervening words like "of the Amounts-Weighted Pri-2012"
    # Use (?<![.\d]) to prevent matching partial decimals like "0.31%"
    m = re.search(r'(?<![.\d])(\d{2,3}(?:\.\d+)?)\s*%\s+(?:of\s+(?:the\s+)?)?(?:[\w-]+\s+){0,4}(?:Pr[iI]|RP|RPH|PRI)',
                  t, re.IGNORECASE)
    if m:
        pct = float(m.group(1))
        if 50 <= pct <= 150 and pct != 100:
            raw = m.group(0).strip()
            return round(pct / 100, 4), raw

    # Pattern: "with a X multiplier" / "multiplier of X" / "0.94 multiplier"
    # Handles both "0.94 multiplier" and "multiplier of 1.04"
    # Exclude "with a 0.75% long-term" which is an improvement rate, not a table multiplier
    m = re.search(r'(?:with\s+a\s+|multiplier\s+(?:of\s+)?)(\d+\.\d+)\s*(?:multiplier)?', t, re.IGNORECASE)
    if not m:
        m = re.search(r'(\d+\.\d+)\s*\)\s*(?:multiplier)?', t, re.IGNORECASE)  # "(1.04)" in parens
    if m:
        factor = float(m.group(1))
        # Check this isn't a long-term improvement rate (e.g., "with a 0.75% long-term")
        after_match = t[m.end():m.end()+40].lower()
        is_improvement_rate = bool(re.search(r'^\s*%?\s*(?:long[\s-]*term|improvement|convergence)', after_match))
        if 0.5 <= factor <= 1.5 and factor != 1.0 and not is_improvement_rate:
            return round(factor, 4), m.group(0).strip()

    # Pattern: "weighted N%" (e.g., "weighted 102%")
    m = re.search(r'weighted\s+(\d{2,3}(?:\.\d+)?)\s*%', t, re.IGNORECASE)
    if m:
        pct = float(m.group(1))
        if 50 <= pct <= 150 and pct != 100:
            return round(pct / 100, 4), m.group(0).strip()

    # Pattern: "increased [by] N%" / "increased N.N%"
    m = re.search(r'increased\s+(?:by\s+)?(\d+(?:\.\d+)?)\s*%', t, re.IGNORECASE)
    if m:
        pct = float(m.group(1))
        if 0 < pct <= 50:
            return round(1.0 + pct / 100, 4), m.group(0).strip()

    # Pattern: "decreased [by] N%" / "decreased N.N%"
    m = re.search(r'decreased\s+(?:by\s+)?(\d+(?:\.\d+)?)\s*%', t, re.IGNORECASE)
    if m:
        pct = float(m.group(1))
        if 0 < pct <= 50:
            return round(1.0 - pct / 100, 4), m.group(0).strip()

    # Pattern: "reduced [by] N%"
    m = re.search(r'reduced\s+(?:by\s+)?(\d+(?:\.\d+)?)\s*%', t, re.IGNORECASE)
    if m:
        pct = float(m.group(1))
        if 0 < pct <= 50:
            return round(1.0 - pct / 100, 4), m.group(0).strip()

    # Pattern: "N% decrease" / "N% reduction"
    m = re.search(r'(\d+(?:\.\d+)?)\s*%\s+(?:decrease|reduction)', t, re.IGNORECASE)
    if m:
        pct = float(m.group(1))
        if 0 < pct <= 50:
            return round(1.0 - pct / 100, 4), m.group(0).strip()

    # Pattern: "N% increase"
    m = re.search(r'(\d+(?:\.\d+)?)\s*%\s+increase', t, re.IGNORECASE)
    if m:
        pct = float(m.group(1))
        if 0 < pct <= 50:
            return round(1.0 + pct / 100, 4), m.group(0).strip()

    # Pattern: "N% load" (additive — e.g. "20% load" means 1.20)
    m = re.search(r'(\d+(?:\.\d+)?)\s*%\s+load', t, re.IGNORECASE)
    if m:
        pct = float(m.group(1))
        if 0 < pct <= 50:
            return round(1.0 + pct / 100, 4), m.group(0).strip()

    # Pattern: "N% for females and M% for males" or vice versa
    m = re.search(r'(\d{2,3})\s*%\s+for\s+(?:females?|women)\s+and\s+(\d{2,3})\s*%\s+for\s+(?:males?|men)',
                  t, re.IGNORECASE)
    if m:
        f_pct, m_pct = float(m.group(1)), float(m.group(2))
        if 50 <= f_pct <= 150 and 50 <= m_pct <= 150:
            avg = (f_pct + m_pct) / 2
            return round(avg / 100, 4), m.group(0).strip()
    # Reverse order: males then females
    m = re.search(r'(\d{2,3})\s*%\s+for\s+(?:males?|men)\s+and\s+(\d{2,3})\s*%\s+for\s+(?:females?|women)',
                  t, re.IGNORECASE)
    if m:
        m_pct, f_pct = float(m.group(1)), float(m.group(2))
        if 50 <= f_pct <= 150 and 50 <= m_pct <= 150:
            avg = (f_pct + m_pct) / 2
            return round(avg / 100, 4), m.group(0).strip()

    # Pattern: sponsor-modified table (Shell Modified, DMBA-adjusted, etc.)
    m = re.search(r'(\w+[\s-]+(?:Modified|adjusted|specific))\s+(?:Pr[iI]|RP|PRI)',
                  t, re.IGNORECASE)
    if m:
        return None, m.group(0).strip()  # Flag the description but no numeric factor

    return None, None


# =============================================================================
# IMPROVEMENT SCALE
# =============================================================================

def extract_improvement_scale(text: str) -> Optional[str]:
    """Extract and normalize the mortality improvement scale."""
    t = text

    # IRS modified MP-YYYY
    m = re.search(r'IRS[\s-]*(?:modified|developed)?\s*(?:projection\s+)?(?:Scale\s+)?MP[\s-]*(\d{4})',
                  t, re.IGNORECASE)
    if m:
        return f"IRS Modified MP-{m.group(1)}"

    # MIM-YYYY (less common)
    m = re.search(r'MIM[\s-]*(\d{4})', t, re.IGNORECASE)
    if m:
        return f"MIM-{m.group(1)}"

    # MMP-YYYY (Mercer variant)
    m = re.search(r'MMP[\s-]*(\d{4})', t, re.IGNORECASE)
    if m:
        return f"MMP-{m.group(1)}"

    # Buck Modified MP-YYYY
    m = re.search(r'Buck\s+Modified\s+(?:Projection\s+)?(?:Scale\s+)?MP[\s-]*(\d{4})', t, re.IGNORECASE)
    if m:
        return f"Buck Modified MP-{m.group(1)}"

    # RPEC Scale MP-YYYY
    m = re.search(r'RPEC\s+Scale\s+MP[\s-]*(\d{4})', t, re.IGNORECASE)
    if m:
        return f"RPEC MP-{m.group(1)}"

    # Aon improvement scale (custom, not standard MP)
    m = re.search(r'(?:Aon|AON)(?:\'s)?\s+(?:improvement\s+scale|scale)', t, re.IGNORECASE)
    if m:
        return "Aon Custom Scale"

    # AIG improvement scale
    m = re.search(r'AIG\s+improvement\s+scale', t, re.IGNORECASE)
    if m:
        return "AIG Custom Scale"

    # MSS scale (FCA/Stellantis)
    m = re.search(r'(?:Scale\s+)?MSS[\s-]*(\d{4})', t, re.IGNORECASE)
    if m:
        return f"MSS-{m.group(1)}"

    # Standard MP-YYYY (most common — must come after special variants)
    # Also handle "MP2021" without separator
    m = re.search(r'(?:(?:Projection\s+)?Scale\s+)?MP[\s-]?(\d{4})', t, re.IGNORECASE)
    if m:
        return f"MP-{m.group(1)}"

    # IRS generational mortality table (prescribed, no explicit MP scale)
    if re.search(r'IRS\s+(?:\d{4}\s+)?(?:Generational\s+)?Mortality\s+Table', t, re.IGNORECASE):
        return "IRS Prescribed"

    # "generational improvement" with no named scale
    if re.search(r'generational\s+(?:mortality\s+)?improvement', t, re.IGNORECASE):
        return "Generational (unspecified)"

    return None


# =============================================================================
# PROJECTION METHOD
# =============================================================================

def extract_projection_method(text: str) -> Tuple[Optional[str], Optional[int]]:
    """
    Extract projection method and base year.
    Returns (method, base_year).
    """
    t = text.lower()

    # IRS prescribed generational
    if re.search(r'irs\s+(?:\d{4}\s+)?(?:prescribed\s+)?generational', t):
        return "IRS Prescribed Generational", None

    # "projected generationally from YYYY"
    m = re.search(r'projected?\s+(?:forward\s+)?generationally\s+(?:from\s+)?(\d{4})?', t)
    if m:
        base_year = int(m.group(1)) if m.group(1) else None
        return "Generational", base_year

    # "generational projection from YYYY"
    m = re.search(r'(?:fully\s+)?generational\s+(?:mortality\s+)?(?:projection|improvement)\s*(?:from\s+)?(\d{4})?', t)
    if m:
        base_year = int(m.group(1)) if m.group(1) else None
        return "Generational", base_year

    # "with generational Scale MP-YYYY" (implied generational)
    if re.search(r'(?:with|using)\s+(?:fully\s+)?generational\s+(?:scale|mp)', t):
        return "Generational", None

    # "projected from YYYY using Scale" (generational implied)
    m = re.search(r'projected?\s+(?:forward\s+)?from\s+(\d{4})\s+(?:using|with)\s+(?:scale\s+)?mp', t)
    if m:
        return "Generational", int(m.group(1))

    # "projected with ... Generational Scale"
    if re.search(r'projected\s+(?:with|using)\s+(?:fully\s+)?generational\s+scale', t):
        return "Generational", None

    # "Fully Generational Scale MP-YYYY"
    if re.search(r'fully\s+generational\s+(?:scale\s+)?mp', t):
        return "Generational", None

    # "generational" anywhere as fallback
    if re.search(r'generational', t):
        return "Generational", None

    # Static projection to a specific year
    m = re.search(r'(?:static|projected?\s+to)\s+(?:the\s+year\s+)?(\d{4})', t)
    if m:
        return "Static", int(m.group(1))

    # "separate static annuitant"
    if re.search(r'static\s+(?:annuitant|mortality)', t):
        return "Static", None

    return None, None


# =============================================================================
# COVID ADJUSTMENT
# =============================================================================

def detect_covid_adjustment(text: str) -> bool:
    """Detect COVID-related mortality adjustments."""
    t = text.lower()
    patterns = [
        r'covid[\s-]*(?:endemic|pandemic|related|19)',
        r'(?:aon|aon\'s?)[\s,]+(?:\w+\s+){0,5}endemic',
        r'endemic\s+(?:mortality|adjustment|version|effects?\s+of\s+covid)',
        r'excess\s+(?:future\s+)?mortality',
        r'irs[\s-]*developed\s+adjust',
        r'(?:near[\s-]*term|long[\s-]*term)\s+(?:and\s+long[\s-]*term\s+)?endemic\s+effects',
        r'adjustments?\s+to\s+reflect\s+(?:anticipated\s+)?(?:near|expected\s+excess)',
    ]
    return any(re.search(p, t) for p in patterns)


# =============================================================================
# WEIGHTING
# =============================================================================

def extract_weighting(text: str) -> Optional[str]:
    """Detect weighting method."""
    t = text.lower()
    if re.search(r'amounts?[\s-]*weight', t):
        return "Amounts-Weighted"
    if re.search(r'benefits?[\s-]*weight', t):
        return "Benefits-Weighted"
    if re.search(r'aggregate\s+rates?', t):
        return "Aggregate"
    return None


# =============================================================================
# COMPANY-SPECIFIC ADJUSTMENT DETECTION
# =============================================================================

def detect_company_adjustment(text: str) -> bool:
    """
    Detect whether the mortality table was adjusted based on company/plan
    experience, credibility studies, or other sponsor-specific modifications.
    """
    t = text.lower()
    # Also check a version with spaces inserted (OCR sometimes strips spaces)
    t_spaced = re.sub(r'([a-z])([A-Z])', r'\1 \2', text).lower()
    patterns = [
        r'(?:company|plan|sponsor|corporate|corporation)[\'s]*\s*(?:specific|custom|rating)',
        r'(?:adjusted|modified)\s+(?:based\s+on|for|to\s+reflect)\s+(?:actual\s+)?(?:company|plan|sponsor|the\s+(?:company|plan|bank|corporation))',
        r'(?:company|plan|sponsor|corporation|bank)[\'s]*\s+(?:mortality\s+)?experience',
        r'(?:experience|credibility)\s+(?:stud|analy)',
        r'(?:derived|based)\s+(?:from|on)\s+(?:the\s+)?mortality\s+experience',
        r'(?:mortality\s+)?(?:multiplier|ratio)\s+(?:derived|used\s+to\s+develop)',
        r'wage\s+class',
        r'rating\s+factors?',
        r'custom\s+mortality',
        r'(?:plan|sponsor)[\s-]*specific\s+mortality',
        r'(?:MILES|Mercer\s+Industry\s+Longevity)',
        # Sponsor-specific prefixed tables (order: sponsor name then descriptor)
        r'(?:shell|dmba|eaton|kodak|goodyear|aig|prudential)[\s-]*(?:modified|adjusted|specific)',
        # Also reversed: descriptor then sponsor
        r'(?:modified|adjusted)\s+(?:shell|dmba|eaton|kodak|goodyear|aig|prudential)',
        # DMBA-adjusted, Shell Modified (hyphenated)
        r'(?:dmba|shell)[\s-]+(?:modified|adjusted)',
        r'based\s+on\s+(?:a\s+)?(?:review|study)\s+of\s+(?:plan\s+)?experience',
        r'reflect\s+(?:the\s+)?(?:company|plan|bank|corporation|sponsor)[\'s]*\s+(?:recent\s+)?(?:mortality|experience)',
        # Prudential/sponsor + specific experience
        r'(?:prudential|eaton|company|plan)\s+specific\s+(?:experience|mortality)',
        # Substitute mortality table (IRS approved company-specific)
        r'substitute\s+mortality\s+table',
    ]
    return any(re.search(p, t) for p in patterns) or any(re.search(p, t_spaced) for p in patterns)


# =============================================================================
# LEGACY TABLE DETECTION (for filtering plan summary)
# =============================================================================

# These tables indicate legacy equivalence / lump-sum conversion provisions,
# not the primary valuation mortality basis.
_LEGACY_TABLE_NAMES = {"1971 GAM", "1983 GAM", "1951 GAM", "UP-1984", "1994 UP", "417(e)"}


def is_primary_valuation(base_table: Optional[str], text: str) -> bool:
    """
    Heuristic: return True if this snippet describes the PRIMARY valuation
    mortality (not a legacy equivalence table or lump-sum provision).
    """
    if base_table in _LEGACY_TABLE_NAMES:
        return False

    t = text.lower()
    # Exclude legacy conversion provisions
    legacy_signals = [
        r'optional\s+form',
        r'actuarially\s+equivalent\s+to\s+the\s+normal\s+form',
        r'converting\s+account\s+balances?\s+to\s+annuit',
        r'lump[\s-]*sum\s+(?:conversion|mortality)',
        r'417\(e\)',
    ]
    # If the snippet is ONLY about lump sums / legacy, not primary
    if any(re.search(p, t) for p in legacy_signals):
        # But if it also mentions a modern table, it may be mixed — keep it
        if base_table and base_table not in _LEGACY_TABLE_NAMES:
            return True
        return False

    return True


# =============================================================================
# SNIPPET PARSER (orchestrator)
# =============================================================================

def parse_snippet(pdf_file: str, page: int, mortality_text: str) -> Dict:
    """Parse a single mortality text snippet into structured fields."""
    ein, pn = extract_ein_pn_from_filename(pdf_file)
    base_table = normalize_base_table(mortality_text)
    collar = extract_collar_adjustment(mortality_text)
    scaling_factor, scaling_desc = extract_scaling_factor(mortality_text)
    improvement_scale = extract_improvement_scale(mortality_text)
    proj_method, proj_base_year = extract_projection_method(mortality_text)
    covid = detect_covid_adjustment(mortality_text)
    weighting = extract_weighting(mortality_text)
    company_adj = detect_company_adjustment(mortality_text)
    primary = is_primary_valuation(base_table, mortality_text)

    return {
        'pdf_file': pdf_file,
        'page': page,
        'ein': ein,
        'plan_number': pn,
        'base_table': base_table,
        'collar_adjustment': collar,
        'scaling_factor': scaling_factor,
        'scaling_description': scaling_desc,
        'improvement_scale': improvement_scale,
        'projection_method': proj_method,
        'projection_base_year': proj_base_year,
        'covid_adjustment': covid,
        'weighting': weighting,
        'company_adjusted': company_adj,
        'is_primary': primary,
        'raw_snippet': mortality_text,
    }


# =============================================================================
# BATCH PROCESSING
# =============================================================================

def parse_all_snippets(csv_path: Path) -> pd.DataFrame:
    """Read raw snippets CSV and parse each row into structured fields."""
    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df)} raw snippets from {csv_path}")

    records = []
    for _, row in df.iterrows():
        rec = parse_snippet(
            pdf_file=str(row['pdf_file']),
            page=int(row['page']),
            mortality_text=str(row.get('mortality_text', '')),
        )
        records.append(rec)

    parsed = pd.DataFrame(records)
    return parsed


def detect_substitute_mortality(txt_dir: Path) -> Tuple[set, set]:
    """
    Scan full extracted text files for IRS-approved substitute mortality table
    usage (Schedule SB, Line 23 / IRC 430(h)(3)(C)).

    Distinguishes between:
    - Current users: plans actively using substitute tables
    - Former users: plans that transitioned AWAY from substitute tables this year

    Excludes false positives where "substitute mortality" appears only in:
    - IRC statutory citations (e.g., "or the substitute mortality tables under IRC §430")
    - Schedule SB form field text (unchecked checkbox labels)

    Returns (current_set, former_set) of (ein, plan_number) tuples.
    """
    sub_pattern = re.compile(r'substitute\s+mortality', re.IGNORECASE)
    # "changed from ... substitute mortality tables to ..." indicates dropped
    dropped_pattern = re.compile(
        r'changed\s+from\s+(?:the\s+|using\s+)?(?:IRS\s+approved\s+)?'
        r'(?:(?:Section|[^\n]{0,20})\s+)?(?:430\s+)?'
        r'(?:plan[\s-]*specific\s+)?substitute\s+mortality',
        re.IGNORECASE
    )
    # IRC citation pattern: "or the substitute mortality tables under IRC"
    # This is a legal reference, not a declaration of usage
    irc_citation_pattern = re.compile(
        r'(?:or\s+the\s+)?substitute\s+mortality\s+tables?\s+'
        r'(?:under|pursuant\s+to|provided\s+by)\s+(?:IRC|Internal\s+Revenue\s+Code)',
        re.IGNORECASE
    )
    ein_pn_pattern = re.compile(r'(\d{9})_(\d{3})_\d{4}')

    current_plans = set()
    former_plans = set()

    for txt_file in txt_dir.glob('*.txt'):
        if txt_file.name.startswith('_'):
            continue
        text = txt_file.read_text(encoding='utf-8', errors='replace')
        matches = list(sub_pattern.finditer(text))
        if not matches:
            continue

        # Check if ALL matches are IRC citations (false positive)
        non_citation_found = False
        for match in matches:
            # Get surrounding context (200 chars before and after)
            start = max(0, match.start() - 200)
            end = min(len(text), match.end() + 200)
            context = text[start:end]
            if not irc_citation_pattern.search(context):
                non_citation_found = True
                break

        if not non_citation_found:
            # All matches were IRC citations — skip this file
            continue

        m = ein_pn_pattern.search(txt_file.stem)
        if m:
            key = (m.group(1), m.group(2))
            if dropped_pattern.search(text):
                former_plans.add(key)
            else:
                current_plans.add(key)

    # Ensure no plan is in both sets (former takes precedence)
    current_plans -= former_plans

    return current_plans, former_plans


def build_plan_summary(parsed_df: pd.DataFrame, txt_dir: Optional[Path] = None) -> pd.DataFrame:
    """
    Consolidate parsed snippets to one row per plan (EIN + plan_number).
    Picks the primary valuation entry, preferring rows where is_primary=True.
    For plans with multiple primary snippets, takes the first (lowest page).
    """
    # Filter to primary valuation rows only
    primary = parsed_df[parsed_df['is_primary']].copy()

    # For plans with NO primary rows, fall back to all rows
    all_plans = set(zip(parsed_df['ein'], parsed_df['plan_number']))
    primary_plans = set(zip(primary['ein'], primary['plan_number']))
    missing = all_plans - primary_plans

    if missing:
        fallback = parsed_df[
            parsed_df.apply(lambda r: (r['ein'], r['plan_number']) in missing, axis=1)
        ]
        primary = pd.concat([primary, fallback], ignore_index=True)

    # Sort by page so we pick the first mention
    primary = primary.sort_values(['ein', 'plan_number', 'page'])

    # Consolidate company_adjusted and covid_adjustment across ALL snippets per plan
    # (these flags should be True if ANY snippet for the plan has them)
    plan_flags = primary.groupby(['ein', 'plan_number']).agg(
        company_adjusted_any=('company_adjusted', 'any'),
        covid_adjustment_any=('covid_adjustment', 'any'),
    ).reset_index()

    # Deduplicate: one row per (ein, plan_number)
    summary = primary.drop_duplicates(subset=['ein', 'plan_number'], keep='first').copy()

    # Override company_adjusted and covid_adjustment with the consolidated values
    summary = summary.merge(plan_flags, on=['ein', 'plan_number'], how='left')
    summary['company_adjusted'] = summary['company_adjusted_any']
    summary['covid_adjustment'] = summary['covid_adjustment_any']
    summary = summary.drop(columns=['company_adjusted_any', 'covid_adjustment_any'])

    # Add substitute mortality flags from Schedule SB full-text scan
    if txt_dir is not None:
        current_sub, former_sub = detect_substitute_mortality(txt_dir)
        summary['substitute_mortality'] = summary.apply(
            lambda r: (r['ein'], r['plan_number']) in current_sub, axis=1
        )
        summary['former_substitute_mortality'] = summary.apply(
            lambda r: (r['ein'], r['plan_number']) in former_sub, axis=1
        )
    else:
        summary['substitute_mortality'] = False
        summary['former_substitute_mortality'] = False

    # Drop helper columns not needed in summary
    cols_to_keep = [
        'pdf_file', 'ein', 'plan_number', 'base_table', 'collar_adjustment',
        'scaling_factor', 'scaling_description', 'improvement_scale',
        'projection_method', 'projection_base_year', 'covid_adjustment',
        'weighting', 'company_adjusted', 'substitute_mortality',
        'former_substitute_mortality',
    ]
    summary = summary[[c for c in cols_to_keep if c in summary.columns]].reset_index(drop=True)
    return summary


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Parse structured mortality fields from Form 5500 snippets")
    parser.add_argument("--year", type=int, default=2024, help="Filing year (default: 2024)")
    args = parser.parse_args()

    script_dir = Path(__file__).parent
    txt_dir = script_dir / f"form5500_txt_{args.year}"
    snippets_csv = txt_dir / f"_actuarial_mortality_snippets_{args.year}.csv"

    if not snippets_csv.exists():
        print(f"Error: Snippets file not found: {snippets_csv}")
        print(f"Run extract_form5500_text.py --year {args.year} first.")
        return

    # Parse
    parsed_df = parse_all_snippets(snippets_csv)

    # Write parsed output
    parsed_path = txt_dir / f"_parsed_mortality_{args.year}.csv"
    parsed_df.to_csv(parsed_path, index=False)
    print(f"\nParsed mortality saved to: {parsed_path}")
    print(f"  {len(parsed_df)} rows")

    # Build and write plan summary (pass txt_dir for substitute mortality scan)
    summary_df = build_plan_summary(parsed_df, txt_dir=txt_dir)
    summary_path = txt_dir / f"_plan_mortality_summary_{args.year}.csv"
    summary_df.to_csv(summary_path, index=False)
    print(f"\nPlan summary saved to: {summary_path}")
    print(f"  {len(summary_df)} plans")

    # Quick stats
    print(f"\n{'='*60}")
    print("PARSING SUMMARY")
    print(f"{'='*60}")

    if 'base_table' in parsed_df.columns:
        primary_rows = parsed_df[parsed_df['is_primary']]
        print(f"\nBase tables (primary valuation rows):")
        for val, cnt in primary_rows['base_table'].value_counts(dropna=False).items():
            print(f"  {val or '(none)'}: {cnt}")

    if 'improvement_scale' in parsed_df.columns:
        print(f"\nImprovement scales (all rows):")
        for val, cnt in parsed_df['improvement_scale'].value_counts(dropna=False).items():
            print(f"  {val or '(none)'}: {cnt}")

    if 'collar_adjustment' in parsed_df.columns:
        print(f"\nCollar adjustments (primary):")
        for val, cnt in primary_rows['collar_adjustment'].value_counts(dropna=False).items():
            print(f"  {val or '(none)'}: {cnt}")

    if 'projection_method' in parsed_df.columns:
        print(f"\nProjection methods (primary):")
        for val, cnt in primary_rows['projection_method'].value_counts(dropna=False).items():
            print(f"  {val or '(none)'}: {cnt}")

    covid_count = parsed_df['covid_adjustment'].sum()
    print(f"\nCOVID adjustments detected: {covid_count}")

    scaling_count = parsed_df['scaling_factor'].notna().sum()
    print(f"Scaling factors detected: {scaling_count}")

    company_adj_count = primary_rows['company_adjusted'].sum()
    print(f"Company-adjusted tables (primary): {int(company_adj_count)}")

    if 'substitute_mortality' in summary_df.columns:
        sub_mort_count = summary_df['substitute_mortality'].sum()
        former_sub_count = summary_df['former_substitute_mortality'].sum()
        print(f"IRS substitute mortality tables (current): {int(sub_mort_count)}")
        if former_sub_count > 0:
            print(f"Former substitute mortality (dropped this year): {int(former_sub_count)} ** OUTREACH OPPORTUNITY **")

    # Flag any legacy tables that leaked into summary
    if 'base_table' in summary_df.columns:
        legacy_in_summary = summary_df[summary_df['base_table'].isin(_LEGACY_TABLE_NAMES)]
        if len(legacy_in_summary) > 0:
            print(f"\nWARNING: {len(legacy_in_summary)} legacy tables in plan summary:")
            for _, row in legacy_in_summary.iterrows():
                print(f"  {row['pdf_file']}: {row['base_table']}")


if __name__ == "__main__":
    main()
