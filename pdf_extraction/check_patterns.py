"""Quick script to find financial statement and attachment patterns in PDFs."""
import pdfplumber

pdf = pdfplumber.open('form5500_pdfs_2024/RTX_060570975_041_2024.pdf')
print(f'Total pages: {len(pdf.pages)}')

# Look for SB attachment patterns specifically
sb_keywords = ['schedule sb attachment', 'sb attachment', 'actuarial exhibit', 'schedule of active', 
               'schedule of retired', 'amortization', 'funding standard', 'participant data',
               'summary of actuarial', 'certification', 'enrolled actuary']

for i in range(30, 60):  # Focus on middle section where SB usually is
    text = (pdf.pages[i].extract_text() or '')[:2000].lower()
    for kw in sb_keywords:
        if kw in text:
            print(f'\n=== Page {i+1}: Found "{kw}" ===')
            print(text[:400])
            break
