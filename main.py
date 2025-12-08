"""
File: main.py
Purpose:
    Entry point for running the Form 5500 data processing pipeline.
    - Loads raw CSV datasets
    - Normalizes participant fields
    - Generates ranked outputs (e.g., top plans by retirees)
    - Saves outputs into the data_output directory
"""


from data_ingestion.combine_years import combine_years
from data_ingestion.normalize_sb_fields import normalize_participant_fields
from data_analysis.rankings import top_plans_by_retirees

def main():
    df = combine_years("data_raw/*.csv")

    participants = normalize_participant_fields(df)
    df = df.join(participants)

    # Example: Top 200 plans by annuitants
    top200 = top_plans_by_retirees(df, n=200)
    top200.to_csv("data_output/top_200_retirees.csv", index=False)

    print("SUCCESS: Output saved to data_output/top_200_retirees.csv")

if __name__ == "__main__":
    main()
