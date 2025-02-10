## ==================================================
## Author(s): Max Weil, Drew DeJarnatt
## Purpose: This script is used to generate the figure for the Das Gupta decomposition.
## ==================================================

import argparse
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.lines as lines
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.ticker as ticker
import numpy as np

# Defining state map
state_map = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DC": "District of Columbia",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
}


def make_das_gupta_fig(scale_ver):

    # Reading in data
    df = pd.read_parquet("FILEPATH")

    # Subsetting to only include states of interest
    df = df[
        (
            df["state"].isin(
                ["NY", "WV", "MA", "DE", "CT"] + ["CO", "NM", "NV", "ID", "UT"]
            )
        )
    ]

    # Aggregating dataframe to get results by toc/state
    result = (
        df.groupby(["draw", "toc", "state"], as_index=False)
        .sum()
        .groupby(["toc", "state"])
        .mean()
    )

    # Mapping factors to display names
    factor_map = {
        "spend_per_enc_effect": "Service Price and Intensity",
        "enc_per_prev_effect": "Service Utilization",
        "prev_per_pop_effect": "Prevalence Rate",
        "pop_frac_effect": "Pop Age Fractions",
    }
    result.rename(columns=factor_map, inplace=True)

    # Changing factor columns to reflect percent of base rate
    for col in factor_map.values():
        result[f"{col}"] = result[col] / result["base_rate"]

    # Reseting index to make state/toc back into columns
    result.reset_index(inplace=True)

    # Mapping state abbreviations to full names
    result["state"] = result["state"].map(state_map)

    # Changing toc to categorical to ensure proper ordering
    result["toc"] = pd.Categorical(
        result["toc"], ["ED", "NF", "HH", "RX", "DV", "IP", "AM"]
    )
    result = result.sort_values("toc")

    # Getting national spend per capita
    nat_spc = (
        result.groupby("toc", as_index=False)["base_rate"].mean()["base_rate"].sum()
    )

    # Setting up the number of rows/columns per figure/page as well as dimensions and number of figures/pages
    MAX_ROWS = 5
    MAX_COLS = 2
    PER_PAGE = MAX_ROWS * MAX_COLS
    N_PAGES = int(np.ceil(result["state"].nunique() / PER_PAGE))
    MAX_FIG_HEIGHT = 11
    MAX_FIG_WIDTH = 8.5

    # Setting colors
    plt.rcParams["axes.prop_cycle"] = plt.cycler(color=plt.cm.Set2.colors[0:4][::-1])

    # Creating a list of figures
    fig_list = []

    # Iterative over the number of pages
    for page in range(N_PAGES):

        # Getting state that will be plotted on current page and calculating the number of needed rows
        page_states = (
            [
                "Utah",
                "New York",
                "Idaho",
                "West Virginia",
                "Nevada",
                "Massachusetts",
                "New Mexico",
                "Delaware",
                "Colorado",
                "Connecticut",
            ]
        )[(PER_PAGE * page) : ((PER_PAGE * page) + PER_PAGE)]
        PAGE_ROWS = int(np.ceil(len(page_states) / MAX_COLS))

        # Creating figure and subplots to plot on the current page
        fig, ax = plt.subplots(
            PAGE_ROWS,
            MAX_COLS,
            figsize=[MAX_FIG_WIDTH, MAX_FIG_HEIGHT / MAX_ROWS * PAGE_ROWS],
            squeeze=False,
        )
        fig.supxlabel(
            "State-specific spending per capita relative to the national spending per capita",
            fontsize=16,
        )
        fig.supylabel("Type-of-Care", fontsize=16)

        # Initializing row/col counters for subplot axes
        row = 0
        col = 0

        for idx, state in enumerate(page_states):

            # Reset bars for new plot
            bar_start = 0
            bar_end = 0

            # Get values to plot for this state
            state_result = result[result["state"] == state]

            # Plot each factor for all TOCs
            for factor in factor_map.values():

                # Plot bar for factor, using offset from last bar
                ax[row, col].barh(
                    state_result["toc"],
                    state_result[factor],
                    label=factor,
                    left=(
                        np.where(state_result[factor].values < 0, bar_start, 0)
                        + np.where(state_result[factor].values > 0, bar_end, 0)
                    ),
                )

                # Change offset for next bar to be plotted
                bar_end = bar_end + np.maximum(state_result[factor].values, 0)
                bar_start = bar_start + np.minimum(state_result[factor].values, 0)

            # Add a point for the overall change and a line at x=0
            ax[row, col].scatter(
                state_result[factor_map.values()].sum(axis=1),
                state_result["toc"],
                c="black",
                s=10,
                label="Total Change",
            )
            ax[row, col].axvline(x=0, color="black", linestyle="dashed", alpha=0.3)

            # Setting new limits to make room for labels
            max_val = max(
                state_result[state_result[factor_map.values()] > 0].sum(axis=1)
            )
            min_val = min(
                state_result[state_result[factor_map.values()] < 0].sum(axis=1)
            )
            ax[row, col].set_xlim(min_val - 0.3, max_val + 0.3)

            # Creating dataframe with values of most positive and negative positions of bars
            bar_ends_df = pd.concat(
                [
                    state_result[state_result[factor_map.values()] < 0].sum(axis=1),
                    state_result[state_result[factor_map.values()] > 0].sum(axis=1),
                ],
                axis=1,
            )
            # Choosing end of bar to place label
            # If overall change is negative place on left, if overall change is positive, place on right
            bar_ends_df["label_loc"] = np.where(
                bar_ends_df.sum(1) > 0, bar_ends_df[1], bar_ends_df[0]
            )

            # Getting the total % change for each TOC
            tot_bar_change = [
                (f"{v}%" if v < 0 else f"+{v}%")
                for v in round(100 * state_result[factor_map.values()].sum(1), 1)
            ]

            # Adding bar labels, using bar_ends_df for x position, toc for y position, and total bar change as labels
            for x, y, label in zip(
                bar_ends_df["label_loc"], state_result["toc"].to_numpy(), tot_bar_change
            ):

                # Adding labels
                ax[row, col].text(
                    (
                        x - 0.02 if x < 0 else x + 0.02
                    ),  # Plotting to the left or right of bar, depending on x position
                    y,
                    label,
                    va="center",
                    ha=("left" if x > 0 else "right"),  # Justifying based on x position
                    fontsize=6,
                )

            # Adding title, including the overall change from the national spend per capita rate
            change = round(
                100 * (state_result["comp_rate"].sum() - nat_spc) / nat_spc, 1
            )
            state = state.replace(" ", "\ ")
            ax[row, col].set_title(
                f"$\\bf{{{state}}}$ \n {'+' if change>0 else ''}{change}% change from US avg.",
                fontsize=10,
            )

            # Formatting tick labels as percentages
            ax[row, col].tick_params(
                axis="both", which="major", labelsize=8, rotation=45
            )
            ax[row, col].xaxis.set_major_formatter(ticker.PercentFormatter(1.0, 0, "%"))

            # For every N state, move to next row and reset to first column
            # Otherwise, move to next column for plotting
            if (idx + 1) % MAX_COLS == 0:
                row += 1
                col = 0
            else:
                col += 1

        plt.figtext(
            0.30,
            1,
            "Lowest spending states per capita",
            va="center",
            ha="center",
            size=14,
        )
        plt.figtext(
            0.77,
            1,
            "Highest spending states per capita",
            va="center",
            ha="center",
            size=14,
        )

        # Tightening layout before legend, leaving space for legend
        fig.tight_layout(rect=[0, 0.003, 1, 1])

        # Plotting labels in sensible order
        handles, labels = ax[0, MAX_COLS - 1].get_legend_handles_labels()
        handles = handles[::-1]
        labels = labels[::-1]
        fig.legend(
            handles, labels, loc="upper center", bbox_to_anchor=(0.5, -0.005), ncol=5
        )

        fig.add_artist(
            lines.Line2D([0.52, 0.52], [1, 0.05], color="black", linewidth=1)
        )

        # Deleting any unused axes from figure
        delete = False
        for i, j in np.ndindex((PAGE_ROWS, MAX_COLS)):
            if (i, j) == (row, col):
                delete = True
            if delete == True:
                fig.delaxes(ax[i, j])
                print((i, j))

        # Adding figure to list
        fig_list.append(fig)

    # Saving figures to pdf
    with PdfPages("FILEPATH") as pdf:
        for fig in fig_list:
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

    make_das_gupta_fig(args.scale_ver)
