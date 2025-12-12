"""
Report Generation Agent

This module provides a ReportGenerationAgent class for consolidating outputs from peer benchmarking, de-risking, and longevity agents into a structured report.
"""

def extract_metadata(master_row):
    return {
        'sponsor_name': master_row.get('sponsor_dfe_name', ''),
        'ein': master_row.get('ein', ''),
        'plan_numbers': master_row.get('plan_number', ''),
        'industry': master_row.get('industry', '')
    }

def summarize_peer_benchmark(peer_benchmark_output):
    summary = {
        'industry': peer_benchmark_output.get('industry', ''),
        'z_scores': {k: v for k, v in peer_benchmark_output.get('comparison_flags', {}).items() if 'zscore' in k},
        'percentiles': {k: v for k, v in peer_benchmark_output.get('comparison_flags', {}).items() if 'percentile' in k},
        'outlier_flags': {
            'high_annuitant_ratio': peer_benchmark_output.get('comparison_flags', {}).get('annuitant_ratio_zscore', 0) > 2,
            'unusual_liability': abs(peer_benchmark_output.get('comparison_flags', {}).get('liability_per_active_zscore', 0)) > 2,
            'mortality_differs': peer_benchmark_output.get('comparison_flags', {}).get('mortality_differs', False)
        },
        'peer_metrics': peer_benchmark_output.get('peer_metrics', {}),
        'sponsor_metrics': peer_benchmark_output.get('sponsor_metrics', {})
    }
    return summary

def summarize_derisking(derisking_output):
    return {
        'is_freezing': derisking_output.get('is_freezing', False),
        'is_derisking': derisking_output.get('is_derisking', False),
        'active_decline': derisking_output.get('evidence_active_decline', False),
        'asset_shift': derisking_output.get('evidence_asset_shift', False),
        'annuity_purchase': derisking_output.get('evidence_annuity_purchase', False),
        'prt_readiness_score': derisking_output.get('prt_readiness_score', 0)
    }

def summarize_longevity(longevity_output):
    return {
        'mortality_pattern': longevity_output.get('mortality_pattern', {}),
        'risk_position_vs_peers': longevity_output.get('risk_position_vs_peers', ''),
        'annuitant_exposure': longevity_output.get('annuitant_exposure', {}),
        'longevity_risk_flags': longevity_output.get('longevity_risk_flags', {}),
        'recommended_next_steps': longevity_output.get('recommended_next_steps', [])
    }

def generate_talking_points(metadata, peer_summary, derisking_summary, longevity_summary):
    points = []
    if peer_summary['outlier_flags']['high_annuitant_ratio']:
        points.append("Sponsor has a higher-than-peer annuitant ratio.")
    if peer_summary['outlier_flags']['unusual_liability']:
        points.append("Liability per active participant is unusual compared to peers.")
    if peer_summary['outlier_flags']['mortality_differs']:
        points.append("Sponsor uses a different mortality assumption than most peers.")
    if derisking_summary['is_freezing']:
        points.append("Plan shows signs of freezing (declining actives, rising retiree ratio).")
    if derisking_summary['annuity_purchase']:
        points.append("Evidence suggests possible annuity purchase activity.")
    if derisking_summary['prt_readiness_score'] >= 3:
        points.append("Plan is well-positioned for pension risk transfer.")
    if longevity_summary['longevity_risk_flags'].get('high_longevity_risk', False):
        points.append("Sponsor faces elevated longevity risk relative to peers.")
    if not points:
        points.append("No major outlier risks or trends detected versus peer group.")
    return points[:7]

def generate_narrative(metadata, peer_summary, derisking_summary, longevity_summary):
    # Compose a 3-5 paragraph summary
    sponsor = metadata.get('sponsor_name', 'The sponsor')
    industry = metadata.get('industry', 'their industry')
    paragraphs = []
    paragraphs.append(f"{sponsor} operates in the {industry} sector. This report benchmarks the sponsor's defined benefit plan against industry peers and highlights key risk and trend signals.")
    # Peer Benchmark
    ann_z = peer_summary['z_scores'].get('annuitant_ratio_zscore', 0)
    if ann_z > 2:
        paragraphs.append("The sponsor's annuitant ratio is significantly higher than the peer group average, indicating a mature participant base.")
    elif ann_z < -2:
        paragraphs.append("The sponsor's annuitant ratio is well below peer norms, suggesting a relatively younger participant base.")
    # De-risking
    if derisking_summary['is_freezing']:
        paragraphs.append("There are clear signs of plan freezing, with declining active counts and a rising share of retirees.")
    if derisking_summary['annuity_purchase']:
        paragraphs.append("The data suggests the sponsor may have engaged in annuity purchase transactions, as evidenced by simultaneous declines in retiree counts and liabilities.")
    # Longevity
    if longevity_summary['longevity_risk_flags'].get('high_longevity_risk', False):
        paragraphs.append("Relative to peers, the sponsor faces elevated longevity risk, primarily due to a high concentration of retirees.")
    if longevity_summary['mortality_pattern'].get('sb_substitute', False):
        paragraphs.append("The sponsor uses a substitute mortality assumption, which differs from the standard approach used by most peers.")
    if not paragraphs:
        paragraphs.append("No significant risk or trend signals were detected in the sponsor's data compared to peers.")
    return "\n\n".join(paragraphs[:5])

class ReportGenerationAgent:
    """
    Report generation agent integrating new multi-year outputs from all agents.
    - Adds five_year_trends, multi_year_derisking_summary, multi_year_longevity_summary.
    - Robustly handles new agent outputs and preserves backward compatibility.
    """
    def __init__(self):
        pass

    def generate_report(self, peer_benchmark_output, derisking_output, longevity_output, master_row=None):
        metadata = extract_metadata(master_row) if master_row is not None else {}
        peer_summary = summarize_peer_benchmark(peer_benchmark_output)
        derisking_summary = summarize_derisking(derisking_output)
        longevity_summary = summarize_longevity(longevity_output)
        talking_points = generate_talking_points(metadata, peer_summary, derisking_summary, longevity_summary)
        narrative_text = generate_narrative(metadata, peer_summary, derisking_summary, longevity_summary)
        # Multi-year sections
        five_year_trends = peer_benchmark_output.get('five_year_metrics', {})
        multi_year_derisking_summary = derisking_output.get('five_year_slopes', {})
        multi_year_longevity_summary = {
            'mortality_trend': longevity_output.get('mortality_trend', []),
            'longevity_risk_trend': longevity_output.get('longevity_risk_trend', None),
            'multi_year_annuitant_ratio_path': longevity_output.get('multi_year_annuitant_ratio_path', [])
        }
        return {
            'metadata': metadata,
            'peer_summary': peer_summary,
            'derisking_summary': derisking_summary,
            'longevity_summary': longevity_summary,
            'talking_points': talking_points,
            'narrative_text': narrative_text,
            'five_year_trends': five_year_trends,
            'multi_year_derisking_summary': multi_year_derisking_summary,
            'multi_year_longevity_summary': multi_year_longevity_summary
        }
