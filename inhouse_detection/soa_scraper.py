import time
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

# ------------------------------------------
# Utility: extract domain from an email
# ------------------------------------------
def extract_domain(email):
    if not email or "@" not in email:
        return None
    return email.split("@")[-1].lower().strip()


# ------------------------------------------
# Main Scraper
# ------------------------------------------
def scrape_soa_directory(company_name, company_domains):
    """
    Scrapes the SOA membership directory for possible in-house actuaries.

    Parameters
    ----------
    company_name : str
        The sponsor/company we are investigating.

    company_domains : list[str]
        List of acceptable email domains (e.g., ["boeing.com"])

    Returns
    -------
    list of dicts
        [
            {
                "name": ...,
                "credentials": ...,
                "email": ...,
                "email_domain": ...,
                "practice_areas": ...,
                "employer_listed": ...,
                "match_level": 0/1/2
            }
        ]
    """

    url = f"https://directory.soa.org/?search={company_name}"

    chrome_opts = Options()
    chrome_opts.add_argument("--headless=new")
    chrome_opts.add_argument("--disable-gpu")

    driver = webdriver.Chrome(options=chrome_opts)
    driver.get(url)
    time.sleep(4)  # Allow page to load dynamically

    results = []

    try:
        cards = driver.find_elements(By.CSS_SELECTOR, ".directory-card-container")

        for card in cards:
            try:
                name = card.find_element(By.CSS_SELECTOR, ".directory-card-name").text.strip()
            except:
                name = None

            try:
                credentials = card.find_element(By.CSS_SELECTOR, ".directory-card-credentials").text.strip()
            except:
                credentials = None

            try:
                email = card.find_element(By.CSS_SELECTOR, "a[href^='mailto:']").get_attribute("href")
                email = email.replace("mailto:", "").strip()
            except:
                email = None

            email_domain = extract_domain(email)

            # Practice area tags
            try:
                areas = [
                    el.text.strip()
                    for el in card.find_elements(By.CSS_SELECTOR, ".directory-card-areas .tag")
                ]
            except:
                areas = []

            # Employer field
            try:
                employer_listed = card.find_element(By.CSS_SELECTOR, ".directory-card-employer").text.strip()
            except:
                employer_listed = None

            # ------------------------------------------
            # IN-HOUSE CLASSIFICATION LOGIC
            # ------------------------------------------
            match_level = 0  # default no relevance

            domain_match = email_domain in company_domains if email_domain else False
            is_retirement_actuary = any(
                kw in " ".join(areas).lower()
                for kw in ["retirement", "pension", "defined benefit"]
            )

            if domain_match and is_retirement_actuary:
                match_level = 2  # DEFINITIVE DB in-house actuary
            elif domain_match:
                match_level = 1  # Email domain suggests in-house, but not DB-specific

            results.append({
                "name": name,
                "credentials": credentials,
                "email": email,
                "email_domain": email_domain,
                "practice_areas": areas,
                "employer_listed": employer_listed,
                "match_level": match_level,
            })

    finally:
        driver.quit()

    return results
