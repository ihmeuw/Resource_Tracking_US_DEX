## ==================================================
## Author(s): Max Weil
## Purpose: This script is used to submit stage 3 processing tasks for HCUP data.
## ==================================================

import argparse
import re
from datetime import datetime
from pathlib import Path
from jobmon.client.api import Tool
import sys
import itertools
import pyarrow.dataset as ds
import getpass
import math
import dexdbload.pipeline_runs as dex_pr


def run_stage_3_submitter(datasets, yrs):

    # Getting config
    config = dex_pr.get_config()

    # Getting all HCUP stage 2 paths
    all_paths = [
        fp
        for fp in itertools.chain(
            Path(dex_pr.parsed_config(config, key="FORMAT")["stage_2_dir"]["SIDS"]),
            Path(dex_pr.parsed_config(config, key="FORMAT")["stage_2_dir"]["SEDD"]),
            Path(dex_pr.parsed_config(config, key="FORMAT")["stage_2_dir"]["NIS"]),
            Path(dex_pr.parsed_config(config, key="FORMAT")["stage_2_dir"]["NEDS"]),
        )
    ]

    # Setting up Jobmon workflow/tool info
    job_tool = Tool(name="HCUP_stage_3")
    wf_name = f'dex_HCUP_transform_{datetime.now().strftime("%Y-%m-%d_%H%M%S")}'
    wf = job_tool.create_workflow(workflow_args=wf_name, name=wf_name)

    # Creating names to search for in filepath, depends on if year was provided
    if not yrs:
        names = ["HCUP_" + i for i in datasets]
    else:
        names = list(
            map("_".join, itertools.product(["HCUP_" + i for i in datasets], yrs))
        )

    # Getting files to process and their respective output path. Output paths are based on year
    # Of data, so all SIDS/SEDD data for a given year will be combined to a single output path.
    files_to_proc = []
    for filepath in all_paths:

        # If file contains a match to name...
        if any(name in filepath.stem for name in names):

            # Find the parent HCUP_XXXX directory. Filepath should only have a single directory starting
            # With "HCUP_", otherwise we raise an error.
            hcup_idx = [
                idx
                for idx, item in enumerate(filepath.parts)
                if item.startswith("HCUP_")
            ]
            if hcup_idx[0] == hcup_idx[-1]:

                # Creating an output path and logpath corresponding to the given filepath's year and dataset
                year = re.search(r"\d{4}", filepath.stem).group()
                outpath = Path(
                    "/".join(filepath.parts[: hcup_idx[0] + 1])[1:]
                ).joinpath(
                    "dex",
                    "00_data_prep",
                    "stage_3",
                    wf_name,
                    f"transformed_{filepath.parts[hcup_idx[0]]}_{year}.parquet",
                )

            else:
                raise RuntimeError(
                    f"Filepath: {filepath} was found to contain multiple items starting with HCUP_. Expected just HCUP_XXXX directory."
                )
            files_to_proc.append((filepath, outpath))

    # Creating a Jobmon workflow for all years of data.
    # Tasks are created for each row group batch of each file.
    # Stage 2 sets row group size at 1,000,000 rows. Processing script
    # Is passed a file, a row group batch index, and an output path

    # Creating task templates for batch processing and defragmenting
    batch_task_temp = create_task_temp(
        job_tool,
        task_temp_name="batch_transform",
        script_path=Path(__file__).resolve().parent / "helpers" / "hcup_transformer.py",
        script_args=["filepath", "outpath", "index"],
    )
    defrag_task_temp = create_task_temp(
        job_tool,
        task_temp_name="defrag",
        script_path=Path(__file__).resolve().parent
        / "helpers"
        / "hcup_defragmenter.py",
        script_args=["filepath"],
    )

    # For each row group batch, create a processing task
    print(f"Creating batch processing tasks.")
    batch_task_list = []

    for filepath, outpath in files_to_proc:

        # Creating batches of 1,000,000 rows for each file
        data_batches = math.ceil(ds.dataset(filepath).count_rows() / 1000000)

        # Creating outpath and logpath for each file
        logpath = Path(f"FILEPATH/{getpass.getuser()}") / wf_name / outpath.stem
        logpath.mkdir(parents=True, exist_ok=True)
        outpath.mkdir(parents=True, exist_ok=True)

        # For each row group batch, create a Jobmon task
        for idx in range(data_batches):

            # Creating batch task and adding to workflow
            batch_task = batch_task_temp.create_task(
                name=filepath.stem + f"-{idx}",
                cluster_name="slurm",
                compute_resources={
                    "queue": "all.q",
                    "cores": 4,
                    "memory": "75G",
                    "runtime": "5400s",
                    "stdout": logpath.as_posix(),
                    "stderr": logpath.as_posix(),
                    "project": "proj_dex",
                    "constraints": "archive",
                },
                **{
                    "filepath": filepath,
                    "index": idx,
                    "outpath": outpath,
                },
            )
            batch_task_list.append(batch_task)

    # For each output file, create a defragmenting task
    # Using a listto avoid duplicate tasks
    print(f"Creating defragmenting tasks.")
    defrag_task_list = []
    added_tasks = []
    for _, outpath in files_to_proc:

        if outpath not in added_tasks:

            # Establishing logpath for each task
            # Should already exist from processing
            logpath = Path(f"FILEPATH/{getpass.getuser()}") / wf_name / outpath.stem

            # Creating defrag task and adding to workflow
            defrag_task = defrag_task_temp.create_task(
                name="defrag_" + outpath.stem,
                cluster_name="slurm",
                upstream_tasks=batch_task_list,
                compute_resources={
                    "queue": "all.q",
                    "cores": 10,
                    "memory": "50G",
                    "runtime": "5400s",
                    "stdout": logpath.as_posix(),
                    "stderr": logpath.as_posix(),
                    "project": "proj_dex",
                },
                **{"filepath": outpath},
            )
            defrag_task_list.append(defrag_task)
            added_tasks.append(outpath)
        else:
            pass

    # Running the workflow
    print(f"Adding tasks to workflow and running.")
    wf.add_tasks(batch_task_list + defrag_task_list)
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
    # Argument parser, must specify dataset to run
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        "--datasets",
        nargs="+",
        required=True,
        choices=["SID", "SEDD", "NIS", "NEDS"],
        help="HCUP dataset to run through stage 3 (SID, SEDD, NIS, or NEDS).",
    )
    parser.add_argument(
        "-y",
        "--years",
        nargs="*",
        required=False,
        default=[],
        help="Choose year(s) of data to run. Default is all years.",
    )
    args = vars(parser.parse_args())

    # Parameters for converting task
    datasets = args["datasets"]
    yrs = args["years"]
    run_stage_3_submitter(datasets, yrs)
