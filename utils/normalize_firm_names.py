"""
Actuarial Firm Name Normalization

Consolidates variations of major actuarial/consulting firm names into canonical forms.
This addresses data quality issues from inconsistent Form 5500 user input.
"""

import re
from typing import Optional

# Canonical firm name mappings
# Each key is a tuple of patterns (case-insensitive) that map to the canonical name
FIRM_NORMALIZATION_RULES = [
    # Big 4 Accounting
    (r'\bERNST\b.*\bYOUNG\b|^EY\b', 'Ernst & Young LLP'),
    (r'\bDELOITTE\b', 'Deloitte Consulting LLP'),
    (r'\bKPMG\b', 'KPMG LLP'),
    (r'\bPRICEWATERHOUSE\b|^PWC\b', 'PricewaterhouseCoopers LLP'),
    
    # Major Actuarial Consultancies
    (r'\bMERCER\b', 'Mercer'),
    (r'\bAON\b', 'Aon'),
    (r'\bWILLIS.*TOWERS.*WATSON\b|\bWTW\b|WILLS\s*TOWERS|WILLIS\s*TOWER\s', 'Willis Towers Watson'),
    (r'\bMILLIMAN\b', 'Milliman'),
    (r'\bBUCK\s*(GLOBAL|CONSULTANTS)?\b', 'Buck Global LLC'),
    (r'\bSEGAL\b', 'Segal'),
    (r'\bCONDUENT\b', 'Conduent'),
    (r'\bNYHART\b', 'Nyhart'),
    (r'\bOCTOBER\s*THREE\b', 'October Three Consulting'),
    (r'\bCHEIRON\b', 'Cheiron'),
    (r'\bGABRIEL\s*ROEDER\s*SMITH\b|\bGRS\b', 'Gabriel Roeder Smith & Company'),
    (r'\bCAVANAUGH\b', 'Cavanaugh Macdonald Consulting'),
    (r'\bFIDELITY\b', 'Fidelity Investments'),
    (r'\bEMPOWER\b', 'Empower'),
    (r'\bPRINCIPAL\b', 'Principal Financial Group'),
    (r'\bVOYA\b', 'Voya Financial'),
    (r'\bPRUDENTIAL\b', 'Prudential'),
    (r'\bMETLIFE\b', 'MetLife'),
    (r'\bMASS\s*MUTUAL\b|\bMASSMUTUAL\b', 'MassMutual'),
    (r'\bTIAA\b', 'TIAA'),
    (r'\bVANGUARD\b', 'Vanguard'),
]

# Compile patterns for efficiency
_COMPILED_RULES = [(re.compile(pattern, re.IGNORECASE), canonical) 
                   for pattern, canonical in FIRM_NORMALIZATION_RULES]


def normalize_firm_name(firm_name: Optional[str]) -> Optional[str]:
    """
    Normalize an actuarial firm name to its canonical form.
    
    Args:
        firm_name: Raw firm name from Form 5500 data
        
    Returns:
        Canonical firm name if a match is found, otherwise the original name (cleaned)
    """
    if firm_name is None or not isinstance(firm_name, str):
        return firm_name
    
    # Basic cleaning
    cleaned = firm_name.strip()
    if not cleaned:
        return None
    
    # Try each normalization rule
    for pattern, canonical in _COMPILED_RULES:
        if pattern.search(cleaned):
            return canonical
    
    # No match - return cleaned original (title case for consistency)
    # But preserve original if it looks intentional (all caps firm names are common)
    return cleaned


def normalize_firm_names_series(series):
    """
    Normalize a pandas Series of firm names.
    
    Args:
        series: pandas Series containing firm names
        
    Returns:
        pandas Series with normalized firm names
    """
    return series.apply(normalize_firm_name)


def get_canonical_firm_list():
    """
    Get list of all canonical firm names used in normalization.
    
    Returns:
        List of canonical firm names
    """
    return [canonical for _, canonical in FIRM_NORMALIZATION_RULES]


if __name__ == "__main__":
    # Test the normalization
    test_names = [
        "ERNST & YOUNG L.L.P",
        "ERNST & YOUNG LLP",
        "MERCER (US) INC.",
        "MERCER, INC.",
        "AON CONSULTING, INC.",
        "AON COUNSULTING, INC.",  # Typo
        "WILLIS TOWERS WATSON US LLC",
        "WILLS TOWERS WATSON US LLC",  # Typo
        "MILLIMAN INC.",
        "BUCK GLOBAL LLC",
        "SEGAL CONSULTING",
        "THE SEGAL COMPANY",
        "DELOITTE CONSULTING LLP",
        "PWC US CONSULTING LLP",
        "PRICEWATERHOUSECOOPERS LLP",
        "SOME SMALL LOCAL FIRM",  # Should pass through unchanged
    ]
    
    print("Firm Name Normalization Test:")
    print("-" * 60)
    for name in test_names:
        normalized = normalize_firm_name(name)
        changed = " ✓" if name != normalized else ""
        print(f"{name:40} -> {normalized}{changed}")
