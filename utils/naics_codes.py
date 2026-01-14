"""
NAICS Code Lookup and Industry Classification

Provides mapping from NAICS codes to industry names and sectors.
Used to enrich Form 5500 data with human-readable industry classifications.
"""

from typing import Optional, Tuple

# NAICS 2-digit sector codes
NAICS_SECTORS = {
    "11": "Agriculture, Forestry, Fishing and Hunting",
    "21": "Mining, Quarrying, and Oil and Gas Extraction",
    "22": "Utilities",
    "23": "Construction",
    "31": "Manufacturing",
    "32": "Manufacturing",
    "33": "Manufacturing",
    "42": "Wholesale Trade",
    "44": "Retail Trade",
    "45": "Retail Trade",
    "48": "Transportation and Warehousing",
    "49": "Transportation and Warehousing",
    "51": "Information",
    "52": "Finance and Insurance",
    "53": "Real Estate and Rental and Leasing",
    "54": "Professional, Scientific, and Technical Services",
    "55": "Management of Companies and Enterprises",
    "56": "Administrative and Support Services",
    "61": "Educational Services",
    "62": "Health Care and Social Assistance",
    "71": "Arts, Entertainment, and Recreation",
    "72": "Accommodation and Food Services",
    "81": "Other Services",
    "92": "Public Administration",
}

# Common 6-digit NAICS codes found in Form 5500 filings
NAICS_CODES = {
    # Manufacturing (31-33)
    "325100": "Basic Chemical Manufacturing",
    "325400": "Pharmaceutical and Medicine Manufacturing",
    "325410": "Pharmaceutical and Medicine Manufacturing",
    "326100": "Plastics Product Manufacturing",
    "331100": "Iron and Steel Mills",
    "331110": "Iron and Steel Mills and Ferroalloy Manufacturing",
    "332000": "Fabricated Metal Product Manufacturing",
    "332900": "Other Fabricated Metal Product Manufacturing",
    "333100": "Agricultural Machinery Manufacturing",
    "333200": "Industrial Machinery Manufacturing",
    "333400": "HVAC and Commercial Refrigeration Equipment",
    "334100": "Computer and Peripheral Equipment Manufacturing",
    "334400": "Semiconductor and Electronic Component Manufacturing",
    "334500": "Electronic Instrument Manufacturing",
    "335100": "Electric Lighting Equipment Manufacturing",
    "336100": "Motor Vehicle Manufacturing",
    "336110": "Automobile and Light Truck Manufacturing",
    "336300": "Motor Vehicle Parts Manufacturing",
    "336400": "Aerospace Product and Parts Manufacturing",
    "336410": "Aerospace Product and Parts Manufacturing",
    "336411": "Aircraft Manufacturing",
    "336412": "Aircraft Engine and Parts Manufacturing",
    "339900": "Other Miscellaneous Manufacturing",
    
    # Utilities (22)
    "221100": "Electric Power Generation and Distribution",
    "221110": "Electric Power Generation",
    "221120": "Electric Power Transmission",
    "221200": "Natural Gas Distribution",
    "221300": "Water and Sewage Systems",
    "221310": "Water Supply and Irrigation Systems",
    
    # Finance and Insurance (52)
    "522100": "Depository Credit Intermediation",
    "522110": "Commercial Banking",
    "522130": "Credit Unions",
    "522200": "Nondepository Credit Intermediation",
    "523100": "Securities and Commodity Contracts",
    "523110": "Investment Banking",
    "523900": "Other Financial Investment Activities",
    "524100": "Insurance Carriers",
    "524110": "Direct Life Insurance Carriers",
    "524113": "Direct Life Insurance Carriers",
    "524114": "Health and Medical Insurance Carriers",
    "524126": "Direct Property and Casualty Insurance",
    "524140": "Reinsurance Carriers",
    "524150": "Insurance Agencies and Brokerages",
    "524200": "Insurance Agencies and Brokerages",
    "524210": "Insurance Agencies and Brokerages",
    "525100": "Insurance and Employee Benefit Funds",
    "525110": "Pension Funds",
    
    # Health Care (62)
    "621000": "Ambulatory Health Care Services",
    "621100": "Offices of Physicians",
    "621111": "Offices of Physicians",
    "621112": "Offices of Physicians, Mental Health",
    "621400": "Outpatient Care Centers",
    "621500": "Medical and Diagnostic Laboratories",
    "622000": "Hospitals",
    "622100": "General Medical and Surgical Hospitals",
    "622110": "General Medical and Surgical Hospitals",
    "623000": "Nursing and Residential Care Facilities",
    "623100": "Nursing Care Facilities",
    "623110": "Nursing Care Facilities",
    "624000": "Social Assistance",
    "624100": "Individual and Family Services",
    
    # Professional Services (54)
    "541100": "Legal Services",
    "541110": "Offices of Lawyers",
    "541200": "Accounting and Bookkeeping Services",
    "541210": "Accounting Services",
    "541211": "Offices of CPAs",
    "541300": "Architectural and Engineering Services",
    "541310": "Architectural Services",
    "541330": "Engineering Services",
    "541400": "Specialized Design Services",
    "541500": "Computer Systems Design",
    "541510": "Computer Systems Design Services",
    "541600": "Management and Technical Consulting",
    "541610": "Management Consulting Services",
    "541700": "Scientific Research and Development",
    "541710": "Physical and Biological Research",
    "541800": "Advertising and Related Services",
    "541900": "Other Professional Services",
    "541990": "All Other Professional Services",
    
    # Management of Companies (55)
    "551100": "Management of Companies",
    "551110": "Management of Companies and Enterprises",
    "551111": "Offices of Bank Holding Companies",
    "551112": "Offices of Other Holding Companies",
    "551114": "Corporate Subsidiary Management",
    
    # Educational Services (61)
    "611000": "Educational Services",
    "611100": "Elementary and Secondary Schools",
    "611110": "Elementary and Secondary Schools",
    "611300": "Colleges and Universities",
    "611310": "Colleges, Universities, and Professional Schools",
    "611400": "Business Schools and Training",
    "611600": "Other Schools and Instruction",
    
    # Transportation (48-49)
    "481000": "Air Transportation",
    "481100": "Scheduled Air Transportation",
    "481110": "Scheduled Passenger Air Transportation",
    "482000": "Rail Transportation",
    "482100": "Rail Transportation",
    "482110": "Rail Transportation",
    "483000": "Water Transportation",
    "484000": "Truck Transportation",
    "484100": "General Freight Trucking",
    "484110": "General Freight Trucking, Local",
    "484120": "General Freight Trucking, Long-Distance",
    "485000": "Transit and Ground Passenger Transportation",
    "486000": "Pipeline Transportation",
    "486100": "Pipeline Transportation of Crude Oil",
    "486200": "Pipeline Transportation of Natural Gas",
    "488000": "Support Activities for Transportation",
    "491000": "Postal Service",
    "492000": "Couriers and Messengers",
    "493000": "Warehousing and Storage",
    
    # Retail Trade (44-45)
    "441000": "Motor Vehicle and Parts Dealers",
    "442000": "Furniture and Home Furnishings Stores",
    "443000": "Electronics and Appliance Stores",
    "444000": "Building Material and Garden Stores",
    "445000": "Food and Beverage Stores",
    "445100": "Grocery Stores",
    "445110": "Supermarkets and Grocery Stores",
    "446000": "Health and Personal Care Stores",
    "447000": "Gasoline Stations",
    "448000": "Clothing and Accessories Stores",
    "452000": "General Merchandise Stores",
    "452100": "Department Stores",
    "452110": "Department Stores",
    "453000": "Miscellaneous Store Retailers",
    "454000": "Nonstore Retailers",
    
    # Wholesale Trade (42)
    "423000": "Merchant Wholesalers, Durable Goods",
    "424000": "Merchant Wholesalers, Nondurable Goods",
    "425000": "Wholesale Electronic Markets",
    
    # Information (51)
    "511000": "Publishing Industries",
    "511100": "Newspaper Publishers",
    "511200": "Software Publishers",
    "512000": "Motion Picture and Sound Recording",
    "515000": "Broadcasting",
    "515100": "Radio and Television Broadcasting",
    "517000": "Telecommunications",
    "517100": "Wired Telecommunications Carriers",
    "517110": "Wired Telecommunications Carriers",
    "517200": "Wireless Telecommunications Carriers",
    "517210": "Wireless Telecommunications Carriers",
    "517900": "Other Telecommunications",
    "518000": "Data Processing and Hosting",
    "518200": "Data Processing, Hosting, and Related",
    "519000": "Other Information Services",
    
    # Construction (23)
    "236000": "Construction of Buildings",
    "236100": "Residential Building Construction",
    "236200": "Nonresidential Building Construction",
    "237000": "Heavy and Civil Engineering Construction",
    "238000": "Specialty Trade Contractors",
    
    # Real Estate (53)
    "531000": "Real Estate",
    "531100": "Lessors of Real Estate",
    "531200": "Offices of Real Estate Agents",
    "532000": "Rental and Leasing Services",
    "533000": "Lessors of Nonfinancial Intangible Assets",
    
    # Other Services (81)
    "811000": "Repair and Maintenance",
    "812000": "Personal and Laundry Services",
    "813000": "Religious and Civic Organizations",
    "813100": "Religious Organizations",
    "813200": "Grantmaking Foundations",
    "813300": "Social Advocacy Organizations",
    "813400": "Civic and Social Organizations",
    "813900": "Business and Professional Associations",
    "813910": "Business Associations",
    "813920": "Professional Organizations",
    
    # Arts and Entertainment (71)
    "711000": "Performing Arts and Spectator Sports",
    "711100": "Performing Arts Companies",
    "711200": "Spectator Sports",
    "711210": "Spectator Sports",
    "712000": "Museums and Historical Sites",
    "713000": "Amusement, Gambling, and Recreation",
    
    # Accommodation and Food Services (72)
    "721000": "Accommodation",
    "721100": "Traveler Accommodation",
    "721110": "Hotels and Motels",
    "722000": "Food Services and Drinking Places",
    "722100": "Full-Service Restaurants",
    "722500": "Restaurants and Other Eating Places",
    
    # Mining (21)
    "211000": "Oil and Gas Extraction",
    "211100": "Oil and Gas Extraction",
    "211110": "Oil and Gas Extraction",
    "212000": "Mining (except Oil and Gas)",
    "212100": "Coal Mining",
    "212200": "Metal Ore Mining",
    "212300": "Nonmetallic Mineral Mining",
    "213000": "Support Activities for Mining",
}


def get_naics_description(code: Optional[str]) -> str:
    """
    Get the description for a NAICS code.
    
    Args:
        code: 6-digit NAICS code (or partial)
        
    Returns:
        Industry description or 'Unknown' if not found
    """
    if not code or not isinstance(code, str):
        return "Unknown"
    
    code = str(code).strip()
    
    # Try exact match first
    if code in NAICS_CODES:
        return NAICS_CODES[code]
    
    # Try without leading zeros or with padding
    code_clean = code.lstrip("0")
    for naics, desc in NAICS_CODES.items():
        if naics.lstrip("0") == code_clean:
            return desc
    
    # Fall back to sector-level description
    if len(code) >= 2:
        sector = code[:2]
        if sector in NAICS_SECTORS:
            return NAICS_SECTORS[sector]
    
    return "Unknown"


def get_naics_sector(code: Optional[str]) -> str:
    """
    Get the broad sector name for a NAICS code.
    
    Args:
        code: NAICS code (any length)
        
    Returns:
        Sector name or 'Unknown'
    """
    if not code or not isinstance(code, str):
        return "Unknown"
    
    code = str(code).strip()
    if len(code) >= 2:
        sector = code[:2]
        return NAICS_SECTORS.get(sector, "Unknown")
    
    return "Unknown"


def get_naics_info(code: Optional[str]) -> Tuple[str, str]:
    """
    Get both sector and description for a NAICS code.
    
    Args:
        code: NAICS code
        
    Returns:
        Tuple of (sector_name, industry_description)
    """
    return get_naics_sector(code), get_naics_description(code)


def enrich_with_naics(df, code_column: str = "BUSINESS_CODE"):
    """
    Add sector and industry columns to a DataFrame based on NAICS codes.
    
    Args:
        df: pandas DataFrame with NAICS codes
        code_column: Name of the column containing NAICS codes
        
    Returns:
        DataFrame with added INDUSTRY_SECTOR and INDUSTRY_NAME columns
    """
    import pandas as pd
    
    df = df.copy()
    df["INDUSTRY_SECTOR"] = df[code_column].astype(str).apply(get_naics_sector)
    df["INDUSTRY_NAME"] = df[code_column].astype(str).apply(get_naics_description)
    
    return df


if __name__ == "__main__":
    # Test the lookups
    test_codes = ["622000", "541110", "522110", "336411", "221100", "813000", "999999"]
    
    print("NAICS Code Lookup Test:")
    print("-" * 70)
    for code in test_codes:
        sector = get_naics_sector(code)
        desc = get_naics_description(code)
        print(f"{code}: {sector} | {desc}")
