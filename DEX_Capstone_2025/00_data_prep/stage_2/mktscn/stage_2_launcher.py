## ==================================================
## Author(s): Max Weil
## Purpose: This script is used to submit stage 2 processing tasks for MarketScan data.
## ==================================================

import argparse
import itertools
from datetime import datetime
from pathlib import Path
from jobmon.client.api import Tool
import sys
import getpass
import dexdbload.pipeline_runs as dex_pr


def run_stage_2_submitter(datasets, tables, years):

    # Getting config filepaths
    config = dex_pr.get_config()

    # Finding all filepaths in stage 1 directory
    stage_1_path = Path(
        dex_pr.parsed_config(config, key="FORMAT")["stage_1_dir"]["MSCAN"]
    )
    filepaths = [i for i in stage_1_path.iterdir()]

    # Finding all filepaths that match the specified datasets and tables to run
    names = list(map("_".join, itertools.product(datasets, tables, years)))
    filepaths_to_proc = [i for i in filepaths if any(j in i.stem for j in names)]

    # Raising error if no files are found
    if not filepaths_to_proc:
        raise RuntimeError(
            f"Something went wrong. No files with {datasets} dataset name(s) and {tables} table name(s) were found in {str(stage_1_path)}."
        )

    # Creating a Jobmon workflow for all years of data.
    # Tasks are created for each row group batch of each file.

    # Setting up Jobmon workflow/tool info
    job_tool = Tool(name="MKTSCN_stage_2")
    wf_name = f"dex_MKTSCN_format_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}"
    wf = job_tool.create_workflow(workflow_args=wf_name, name=wf_name)

    # Creating task template for batch processing
    batch_task_temp = create_task_temp(
        job_tool,
        task_temp_name="batch_format",
        script_path=Path(__file__).resolve().parent / "helpers" / "mktscn_formatter.py",
        script_args=["batch_path", "outpath"],
    )

    # Getting stage 2 path
    stage_2_path = Path(
        dex_pr.parsed_config(config, key="FORMAT")["stage_2_dir"]["MSCAN"]
    )

    # For each row group batch, create a processing task
    print(f"Creating batch processing tasks.")
    batch_task_list = []
    for filepath in filepaths_to_proc:

        # Finding all batch files of interest in current filepath
        batch_paths = [i for i in filepath.iterdir() if i.is_file()]

        # Creating outpath and logpath for each file
        outpath = stage_2_path / filepath.name
        outpath.mkdir(parents=True, exist_ok=True)
        logpath = Path(f"FILEPATH/{getpass.getuser()}") / wf_name / outpath.stem
        logpath.mkdir(parents=True, exist_ok=True)

        # For each row group batch, create a Jobmon task
        for batch_path in batch_paths:

            # Creating batch task and adding to workflow
            batch_task = batch_task_temp.create_task(
                name=batch_path.stem,
                cluster_name="slurm",
                compute_resources={
                    "queue": "long.q",
                    "cores": 1,
                    "memory": "10G",
                    "runtime": "10800s",
                    "stdout": logpath.as_posix(),
                    "stderr": logpath.as_posix(),
                    "project": "proj_dex",
                    "constraints": "archive",
                },
                **{"batch_path": batch_path, "outpath": outpath},
            )
            batch_task_list.append(batch_task)

    # Running the workflow
    print(f"Adding tasks to workflow and running.")
    wf.add_tasks(batch_task_list)
    wf_run = wf.run(seconds_until_timeout=86400)

    if wf_run == "D":
        print(f"Workflow completed successfully!")
    else:
        print(f"The workflow did not complete successfully. Please check error logs.")

    print("All workflows completed.")


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
    parser = argparse.ArgumentParser()

    # Must specify marketscan dataset to process
    parser.add_argument(
        "-d",
        "--datasets",
        nargs="+",
        required=True,
        choices=["ccae", "mdcr"],
        help="MarketScan datasets to process.",
    )

    # Must specify tocs to process
    parser.add_argument(
        "-t",
        "--tables",
        nargs="+",
        required=True,
        choices=["i", "s", "o", "d"],
        help="Types-of-care to process. Inpatient (i), inpatient services (s), outpatient (o), or pharmaceutical (d).",
    )
    # Must specify years of marketscan data to process
    parser.add_argument(
        "-y",
        "--years",
        nargs="+",
        required=False,
        default=[
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
        ],
        choices=[
            "2000",
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
        ],
        help="Years of MarketScan stage 1 data to process.",
    )

    # Must specify years of marketscan data to process
    parser.add_argument(
        "-y",
        "--years",
        nargs="+",
        required=False,
        default=[
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
        ],
        choices=[
            "2000",
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
        ],
        help="Years of MarketScan stage 1 data to process.",
    )

    # Getting args from parser and running stage 2 workflow
    args = vars(parser.parse_args())
    datasets = args["datasets"]
    tables = args["tables"]
    years = args["years"]
    run_stage_2_submitter(datasets, tables, years)
