## ==================================================
## Author(s): Max Weil, Drew DeJarnatt
## Purpose: This script is used to generate the figure for the Shapley decomposition.
## ==================================================

import argparse
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.ticker as ticker
from matplotlib.patches import Patch


def make_shapley_fig(scale_ver):
    shapley_results = pd.read_parquet(f"FILEPATH")
    shapley_results = shapley_results.groupby(["facet", "val"]).mean().reset_index()

    payer_map = {
        "All": "All Payers",
        "mdcr": "Medicare",
        "mdcd": "Medicaid",
        "priv": "Private",
        "oop": "Out-of-Pocket",
    }

    toc_map = {
        "IP": "Inpatient",
        "AM": "Ambulatory",
        "ED": "Emergency\nDepartment",
        "HH": "Home Health",
        "NF": "Nursing\nFacility",
        "RX": "Pharmaceutical\nClaims",
    }

    cause_map = {
        "diabetes_typ2": "Diabetes mellitus type 2",
        "msk_other": "Other musculoskeletal disorders",
        "cvd_ihd": "Ischemic heart disease",
        "urinary": "Urinary diseases and male infertility",
        "skin": "Skin and subcutaneous diseases",
        "neuro_dementia": "Alzheimer's disease and other dementias",
        "lri": "Lower respiratory infections",
        "digest_upper": "Upper digestive system diseases",
        "cvd_stroke": "Stroke",
        "inj_falls": "Falls",
    }

    factors = [
        "pop_frac_ln_adj",
        "prev_per_pop_ln_adj",
        "enc_per_prev_ln_adj",
        "spend_per_enc_ln_adj",
    ]

    factor_map = {
        "pop_frac_ln_adj": "Pop Age Fractions",
        "prev_per_pop_ln_adj": "Prevalence Rate",
        "enc_per_prev_ln_adj": "Service Utilization",
        "spend_per_enc_ln_adj": "Service Price and Intensity",
    }

    colors = [i["color"] for i in plt.cycler(color=plt.cm.Set2.colors)]
    color_dict = {factor: colors[i] for i, factor in enumerate(factors)}

    fig, ax = plt.subplots(3, 1, figsize=[8.5, 11], squeeze=False)
    fig.supylabel(
        "Factors that explain cross-county variation in spending per capita in 2019",
        fontsize=16,
    )

    ax[0, 0].set_title(
        "Factors explaining cross-county payer-specific spending variation"
    )
    for p in payer_map.keys():
        bar_start = 0
        # Iterate over each factor
        for f in reversed(factors):
            factor_value = shapley_results.loc[shapley_results["val"] == p, f].values[0]
            ax[0, 0].bar(
                payer_map[p], factor_value, bottom=bar_start, color=color_dict[f]
            )
            bar_start += factor_value

    ax[0, 0].yaxis.set_major_formatter(ticker.PercentFormatter(1.0, None, "%"))

    ax[1, 0].set_title(
        "Factors explaining cross-county type of care-specific spending variation"
    )
    for t in toc_map.keys():
        bar_start = 0
        # Iterate over each factor
        for f in reversed(factors):
            factor_value = shapley_results.loc[shapley_results["val"] == t, f].values[0]
            ax[1, 0].bar(
                toc_map[t], factor_value, bottom=bar_start, color=color_dict[f]
            )
            bar_start += factor_value
    ax[1, 0].yaxis.set_major_formatter(ticker.PercentFormatter(1.0, None, "%"))

    ax[2, 0].set_title(
        "Factors explaining cross-country health condition-specific spending variation"
    )
    for c in cause_map.keys():
        bar_start = 0
        # Iterate over each factor
        for f in reversed(factors):
            factor_value = shapley_results.loc[shapley_results["val"] == c, f].values[0]
            ax[2, 0].bar(
                cause_map[c], factor_value, bottom=bar_start, color=color_dict[f]
            )
            bar_start += factor_value
    ax[2, 0].yaxis.set_major_formatter(ticker.PercentFormatter(1.0, None, "%"))
    ax[2, 0].set_xticklabels(ax[2, 0].get_xticklabels(), rotation=45, ha="right")

    # Order legend
    legend_handles = [Patch(color=color_dict[f], label=factor_map[f]) for f in factors]
    ax[2, 0].legend(handles=legend_handles)

    fig.tight_layout()

    # Saving figures to pdf
    with PdfPages(f"FILEPATH") as pdf:
        pdf.savefig(fig, bbox_inches="tight")
        plt.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--scale_ver",
        type=str,
        required=True,
        help="Scaled version of data to prepare for decomposition.",
    )
    args = parser.parse_args()

    make_shapley_fig(args.scale_ver)
