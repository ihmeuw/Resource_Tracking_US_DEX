## ==================================================
## Author(s): Max Weil
## Purpose: This script is used to submit Das Gupta decomposition tasks.
## ==================================================

import argparse
from pathlib import Path
from jobmon.client.api import Tool
from datetime import datetime
import sys
import getpass
import itertools

# Defining constants
# Lists of all DEX TOCs and US states
TOCS = ["IP", "AM", "HH", "ED", "NF", "RX", "DV"]
STATES = [
    "AK",
    "AL",
    "AR",
    "AZ",
    "CA",
    "CO",
    "CT",
    "DC",
    "DE",
    "FL",
    "GA",
    "HI",
    "IA",
    "ID",
    "IL",
    "IN",
    "KS",
    "KY",
    "LA",
    "MA",
    "MD",
    "ME",
    "MI",
    "MN",
    "MO",
    "MS",
    "MT",
    "NC",
    "ND",
    "NE",
    "NH",
    "NJ",
    "NM",
    "NV",
    "NY",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VA",
    "VT",
    "WA",
    "WI",
    "WV",
    "WY",
]

# List of draws and means
# Using 50 draws if draw level is specified
# OPtherwise using an artificial draw of -1 for means
DRAWS = range(0, 50)
MEANS = [-1]


def run_decomp_submitter(
    scale_ver,
    draw_lvl,
    decomp_type,
    base_lvl,
    comp_lvl,
    spatial_year,
    decomp_cols,
    deflate_spend,
    use_prev,
    states,
):

    # Setting up Jobmon workflow/tool info
    job_tool = Tool(name="das_gupta_decomp")
    wf_name = f"das_gupta_decomp_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}"
    wf = job_tool.create_workflow(workflow_args=wf_name, name=wf_name)

    # Creating task template for decomposition jobs
    decomp_task_temp = create_task_temp(
        job_tool,
        task_temp_name="decomp_task",
        script_path=Path(__file__).resolve().parent / "helpers" / "das_gupta_decomp.py",
        script_args=[
            "scale_ver",
            "draw",
            "decomp_type",
            "base_lvl",
            "comp_lvl",
            "spatial_year",
            "decomp_cols",
            "deflate_spend",
            "use_prev",
            "state",
            "toc",
            "outpath",
        ],
    )

    # Creating outpath for results
    outpath = Path("FILEPATH")
    outpath.mkdir(parents=True, exist_ok=True)

    # Creating job specifications, all possible combinations of tocs, states, and draws
    # If draw level, then use 50 draws, otherwise use -1 for means
    job_specs = list(itertools.product(TOCS, states, DRAWS if draw_lvl else MEANS))

    # Creating tasks for each type-of-care, state, and draw
    decomp_task_list = []
    for toc, state, draw in job_specs:

        # Creating logpath for each task
        logpath = Path(f"FILEPATH/{getpass.getuser()}") / wf_name / toc / state
        logpath.mkdir(parents=True, exist_ok=True)

        # Creating task
        decomp_task = decomp_task_temp.create_task(
            name=f"{decomp_type}_{state}_{toc}_{draw}",
            cluster_name="slurm",
            compute_resources={
                "queue": "all.q",
                "cores": 1,
                "memory": "150G",
                "runtime": "3600s",
                "stdout": logpath.as_posix(),
                "stderr": logpath.as_posix(),
                "project": "proj_dex",
                "constraints": "archive",
            },
            **{
                "scale_ver": scale_ver,
                "draw": draw,
                "decomp_type": decomp_type,
                "base_lvl": base_lvl,
                "comp_lvl": comp_lvl,
                "spatial_year": spatial_year,
                "decomp_cols": " ".join(decomp_cols),
                "deflate_spend": deflate_spend,
                "use_prev": use_prev,
                "state": state,
                "toc": toc,
                "outpath": outpath,
            },
        )
        decomp_task_list.append(decomp_task)

    # Running the workflow
    print(f"Adding tasks to workflow.")
    wf.add_tasks(decomp_task_list)
    print(f"Running Das Gupta decomposition workflow.")
    wf_run = wf.run(seconds_until_timeout=86400)

    if wf_run == "D":
        print(f"Workflow completed successfully!")
    else:
        print(f"The workflow did not complete successfully. Please check error logs.")


def create_task_temp(tool, task_temp_name, script_path, script_args):

    # Formatting command template from desired arguments
    command_template = (
        f"{sys.executable} {script_path} "
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
        help="Scaled version of data to use for decomposition.",
    )
    parser.add_argument(
        "--draw_lvl",
        "-d",
        type=int,
        required=True,
        choices=[1, 0],
        help="Flag of whether to calculate metrics at the draw level or for means. 1 for draw level, 0 for means.",
    )
    parser.add_argument(
        "--decomp_type",
        "-t",
        type=str,
        required=True,
        choices=["temporal", "spatial"],
        help="Type of Das Gupta Decomposition to run.",
    )
    parser.add_argument(
        "--base_lvl",
        "-b",
        type=str,
        required=True,
        help="Base year or geographic level to decompose against",
    )
    parser.add_argument(
        "--comp_lvl",
        "-c",
        type=str,
        required=True,
        help="Year or geographic level to decompose against base.",
    )
    parser.add_argument(
        "--spatial_year",
        "-y",
        type=int,
        required=False,
        default=2019,
        help="Year to use for spatial decomposition, required if decomp_type is spatial. Defaults to 2019.",
    )
    parser.add_argument(
        "--decomp_cols",
        "-l",
        type=str,
        nargs="+",
        required=False,
        choices=["acause", "toc", "state", "location"],
        default=["acause", "toc", "state", "location"],
        help="Columns to decompose by, defaults to type-of-care and location. These columns will be included in final decomposition output.",
    )
    parser.add_argument(
        "--deflate_spend",
        "-x",
        type=int,
        required=False,
        default=0,
        help="Whether to deflate spending to original year dollars or not. 1 to deflate, 0 to not deflate.",
    )
    parser.add_argument(
        "--use_prev",
        "-u",
        type=int,
        required=False,
        default=1,
        help="Whether to use prevalence estimates or not. 1 to use, 0 to not use.",
    )
    parser.add_argument(
        "--states",
        "-s",
        type=str,
        nargs="+",
        required=False,
        choices=STATES,
        default=STATES,
        help="Specific states to run decomposition on. Defaults to all states.",
    )
    args = vars(parser.parse_args())

    # Parsing arguments
    scale_ver = args["scale_ver"]
    draw_lvl = args["draw_lvl"]
    decomp_type = args["decomp_type"]
    base_lvl = args["base_lvl"]
    comp_lvl = args["comp_lvl"]
    spatial_year = args["spatial_year"]
    decomp_cols = (
        args["decomp_cols"] if draw_lvl == 0 else ["draw"] + args["decomp_cols"]
    )
    deflate_spend = args["deflate_spend"]
    use_prev = args["use_prev"]
    states = args["states"]

    # Checking that base and comparision levels are valid, given decomp_type
    if decomp_type == "temporal":
        temporal_lvls = [
            "2010",
            "2011",
            "2012",
            "2013",
            "2014",
            "2015",
            "2016",
            "2017",
            "2018",
            "2019",
            "2020",
            "2021",
            "2022",
        ]
        if not (base_lvl in temporal_lvls and comp_lvl in temporal_lvls):
            raise ValueError(
                "Base and comparison levels must be years for temporal decomposition. Valid years are 2010-2022."
            )
        elif int(base_lvl) >= int(comp_lvl):
            raise ValueError(
                "Base year must be prior to comparison year for temporal decomposition."
            )
    elif decomp_type == "spatial":
        spatial_lvls = ["national", "state", "county"]
        if not (base_lvl in spatial_lvls and comp_lvl in spatial_lvls):
            raise ValueError(
                "Base and comparison levels must be geographic levels for spatial decomposition. Valid levels are 'national', 'state', 'county'."
            )
        elif spatial_lvls.index(base_lvl) >= spatial_lvls.index(comp_lvl):
            raise ValueError(
                "Base geographical level must be larger than comparison geographical level for spatial decomposition."
            )
        elif not spatial_year:
            raise ValueError(
                "Year of decomp must be specified for spatial decomposition."
            )

    # Running decomposition submitter
    run_decomp_submitter(
        scale_ver,
        draw_lvl,
        decomp_type,
        base_lvl,
        comp_lvl,
        spatial_year,
        decomp_cols,
        deflate_spend,
        use_prev,
        states,
    )
