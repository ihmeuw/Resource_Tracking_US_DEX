## ==================================================
## Author(s): Max Weil
## Purpose: This script is used to submit Shapley decomposition data prep tasks.
## ==================================================

import argparse
from pathlib import Path
from jobmon.client.api import Tool
from datetime import datetime
import sys
import getpass
import itertools

# Defining constants
# Lists of all DEX TOCs and years to run prep on
YEARS = ["2019"]
TOCS = ["IP", "AM", "ED", "HH", "NF", "RX", "DV"]

# List of draws and means
# Using 50 draws if draw level is specified
# OPtherwise using an artificial draw of -1 for means
DRAWS = range(0, 50)
MEANS = [-1]


def run_decomp_prep_submitter(scale_ver, draw_lvl):

    # Setting up Jobmon workflow/tool info
    job_tool = Tool(name="regression_decomp_prep")
    wf_name = f"regression_decomp_prep_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}"
    wf = job_tool.create_workflow(workflow_args=wf_name, name=wf_name)

    # Creating task templates for prepping decomposition data
    decomp_prep_task_temp = create_task_temp(
        job_tool,
        task_temp_name="decomp_prep",
        script_path=Path(__file__).resolve().parent
        / "helpers"
        / "regression_decomp_prep.py",
        script_args=["scale_ver", "draw", "year", "toc", "outpath"],
    )

    # Creating outpath for results
    outpath = Path("FILEPATH")
    outpath.mkdir(parents=True, exist_ok=True)

    # Creating data prep task specifications, all possible combinations of years, states, and draws
    # If draw level, then use 50 draws, otherwise use -1 for means
    prep_task_specs = list(itertools.product(YEARS, TOCS, DRAWS if draw_lvl else MEANS))

    # Creating data prep task for each year, toc, and draw
    decomp_prep_task_list = []
    for year, toc, draw in prep_task_specs:

        # Creating logpath for each task
        logpath = Path(f"FILEPATH/{getpass.getuser()}") / wf_name / year
        logpath.mkdir(parents=True, exist_ok=True)

        # Creating task
        decomp_prep_task = decomp_prep_task_temp.create_task(
            name=f"prep_{year}_{toc}_{draw}",
            cluster_name="slurm",
            compute_resources={
                "queue": "all.q",
                "cores": 1,
                "memory": "250G",
                "runtime": "7200s",
                "stdout": logpath.as_posix(),
                "stderr": logpath.as_posix(),
                "project": "proj_dex",
                "constraints": "archive",
            },
            **{
                "scale_ver": scale_ver,
                "draw": draw,
                "year": year,
                "toc": toc,
                "outpath": outpath,
            },
        )
        decomp_prep_task_list.append(decomp_prep_task)

    # Running the workflow
    print(f"Adding tasks to workflow.")
    wf.add_tasks(decomp_prep_task_list)
    print(f"Running regression decomposition data prep workflow.")
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
        help="Scaled version of data to prepare for decomposition.",
    )
    parser.add_argument(
        "--draw_lvl",
        "-d",
        type=int,
        required=True,
        choices=[1, 0],
        help="Flag of whether to calculate metrics at the draw level or for means. 1 for draw level, 0 for means.",
    )
    args = vars(parser.parse_args())

    scale_ver = args["scale_ver"]
    draw_lvl = args["draw_lvl"]

    run_decomp_prep_submitter(scale_ver, draw_lvl)
