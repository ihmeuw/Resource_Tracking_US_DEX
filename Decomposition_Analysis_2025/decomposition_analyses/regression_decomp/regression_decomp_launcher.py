## ==================================================
## Author(s): Max Weil
## Purpose: This script is used to submit regression decomposition tasks.
## ==================================================

import argparse
from pathlib import Path
from jobmon.client.api import Tool
from datetime import datetime
import getpass
import itertools

# Defining constants
# Lists of all DEX TOCs
TOCS = ["IP", "AM", "HH", "ED", "NF", "RX"]

# List of covariates
COVS = [
    "ahrf_mds_pc_adj",
    "urban_adj",
    "income_median_adj",
    "insured_pct_adj",
    "edu_ba_adj",
    "percent_insured_w_priv_aa_adj",
    "primarycare_phys_nonfed_ratio_to_mds_adj",
    "prop_medicare_mc_adj",
]

# List of draws and means
DRAWS = range(0, 50)
MEANS = [-1]


def run_decomp_submitter(scale_ver, draw_lvl, weighted):

    # Getting draws or means based on draw level flag
    draws = DRAWS if draw_lvl else MEANS

    # Making toc dictionary to run decomp by TOC
    tocs = {toc: "toc" for toc in TOCS}

    # Setting up Jobmon workflow/tool info
    job_tool = Tool(name="regression_decomp")
    wf_name = f"regression_decomp_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}"
    wf = job_tool.create_workflow(workflow_args=wf_name, name=wf_name)

    # Creating task templates for prepping and adjusting decomposition data
    decomp_task_temp = create_task_temp(
        job_tool,
        task_temp_name="regression_decomp",
        script_path=Path(__file__).resolve().parent
        / "helpers"
        / "regression_decomp_bivar.R",
        script_args=[
            "scale_ver",
            "draw",
            "weighted",
            "covariate",
            "filter_col",
            "filter_val",
        ],
    )

    # Creating outpath for results
    outpath = Path("FILEPATH")
    outpath.mkdir(parents=True, exist_ok=True)

    # Creating job specifications, all possible combinations of draws, filter_cols, and filter_vals
    decomp_task_specs = list(itertools.product(draws, COVS, list(tocs.items())))

    # Creating data prep task for each year and state
    decomp_task_list = []
    for draw, cov, (filter_val, filter_col) in decomp_task_specs:

        # Creating logpath for each task
        logpath = Path(f"FILEPATH/{getpass.getuser()}") / wf_name / f"draw_{draw}"
        logpath.mkdir(parents=True, exist_ok=True)

        # Creating task
        decomp_prep_task = decomp_task_temp.create_task(
            name=f"{filter_col}_{filter_val}_{draw}",
            cluster_name="slurm",
            compute_resources={
                "queue": "all.q",
                "cores": 1,
                "memory": "25G",
                "runtime": "600s",
                "stdout": logpath.as_posix(),
                "stderr": logpath.as_posix(),
                "project": "proj_dex",
                "constraints": "archive",
            },
            **{
                "scale_ver": scale_ver,
                "draw": draw,
                "weighted": weighted,
                "covariate": cov,
                "filter_col": filter_col,
                "filter_val": filter_val,
            },
        )
        decomp_task_list.append(decomp_prep_task)

    # Running the workflow
    print(f"Adding tasks to workflow.")
    wf.add_tasks(decomp_task_list)
    print(f"Running regression decomposition workflow.")
    wf_run = wf.run(seconds_until_timeout=86400, fail_fast=True)

    if wf_run == "D":
        print(f"Workflow completed successfully!")
    else:
        print(f"The workflow did not complete successfully. Please check error logs.")


def create_task_temp(tool, task_temp_name, script_path, script_args):

    # Formatting command template from desired arguments
    command_template = (
        f"FILEPATH/execRscript.sh  -i FILEPATH/r.img -s {script_path} "
        + "".join([f"--{arg} {{{arg}}} " for arg in script_args])[:-1]
    )

    # Creating task template
    task_temp = tool.get_task_template(
        template_name=task_temp_name,
        command_template=command_template,
        node_args=script_args,
    )

    return task_temp


if __name__ == "__main__":

    # Adding command line script arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--scale_ver",
        "-v",
        type=str,
        required=True,
        help="Scaled version of data to decompose.",
    )
    parser.add_argument(
        "--draw_lvl",
        "-d",
        type=int,
        required=True,
        choices=[1, 0],
        help="Flag of whether decomposition is at draw level or not.",
    )
    parser.add_argument(
        "--weighted",
        "-w",
        type=int,
        required=True,
        choices=[1, 0],
        help="Flag of whether to use county spending weights or not.",
    )
    args = vars(parser.parse_args())

    scale_ver = args["scale_ver"]
    draw_lvl = args["draw_lvl"]
    weighted = args["weighted"]

    run_decomp_submitter(scale_ver, draw_lvl, weighted)
