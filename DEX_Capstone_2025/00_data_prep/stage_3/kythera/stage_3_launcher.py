## ==================================================
## Author(s): Max Weil
## Purpose: This script is used to submit stage 3 processing tasks for Kythera data.
## ==================================================

import argparse
from datetime import datetime
from pathlib import Path
from jobmon.client.api import Tool
import sys
import getpass
import os
import glob
import dexdbload.pipeline_runs as dex_pr


def run_stage_3_submitter(tables):

    # Getting config filepaths
    config = dex_pr.get_config()

    # Getting stage 2 path
    stage_2_path = Path(
        dex_pr.parsed_config(config, key="FORMAT")["stage_2_dir"]["KYTHERA"]
    )

    # Setting up Jobmon workflows/tool info
    job_tool = Tool(name="Kythera")
    transform_wf_name = (
        f'dex_KYTHERA_transform_{datetime.now().strftime("%Y-%m-%d_%H%M%S")}'
    )
    transform_wf = job_tool.create_workflow(
        workflow_args=transform_wf_name, name=transform_wf_name
    )
    defrag_wf_name = f'dex_KYTHERA_defrag_{datetime.now().strftime("%Y-%m-%d_%H%M%S")}'
    defrag_wf = job_tool.create_workflow(
        workflow_args=defrag_wf_name, name=defrag_wf_name
    )

    # Creating task templates for processing and defragmenting
    transform_task_temp = create_task_temp(
        job_tool,
        task_temp_name="table_transform",
        script_path=Path(__file__).resolve().parent
        / "helpers"
        / "kythera_transformer.py",
        script_args=["batch_path", "outpath", "table"],
    )
    defrag_task_temp = create_task_temp(
        job_tool,
        task_temp_name="defrag",
        script_path=Path(__file__).resolve().parent
        / "helpers"
        / "kythera_defragmenter.py",
        script_args=["filepath", "table"],
    )

    # Getting stage 3 path
    stage_3_path = Path(
        dex_pr.parsed_config(config, key="FORMAT")["stage_3_dir"]["KYTHERA"]
    )

    # For each table, create a transformation task
    print(f"Creating table transformation tasks.")
    transform_task_list = []
    for table in tables:

        # Getting files to process
        table_path = stage_2_path / f"{table}.parquet"
        batch_paths = [i for i in table_path.iterdir() if i.is_dir()]
        outpath = stage_3_path / f"{table}.parquet"
        outpath.mkdir(parents=True, exist_ok=True)

        # Setting logpath
        logpath = Path(f"FILEPATH/{getpass.getuser()}") / transform_wf_name / table
        logpath.mkdir(parents=True, exist_ok=True)

        for batch_path in batch_paths:

            # Creating processing task and adding to workflow
            format_task = transform_task_temp.create_task(
                name=table,
                cluster_name="slurm",
                compute_resources={
                    "queue": "all.q",
                    "cores": 1,
                    "memory": "150G",
                    "runtime": "7200s",
                    "stdout": logpath.as_posix(),
                    "stderr": logpath.as_posix(),
                    "project": "proj_dex",
                    "constraints": "archive",
                },
                **{"batch_path": batch_path, "outpath": outpath, "table": table},
            )
            transform_task_list.append(format_task)

    # Running the first workflow
    print(f"Adding tasks to transformation workflow.")
    transform_wf.add_tasks(transform_task_list)
    print(f"Running transformation workflow.")
    transform_wf_run = transform_wf.run(seconds_until_timeout=86400)

    # Checking if first workflow completed successfully
    if transform_wf_run == "D":
        print(f"Workflow completed successfully!")
    else:
        print(f"The workflow did not complete successfully. Please check error logs.")

    # Creating defragmentation tasks
    print(f"Creating defragmenting tasks.")
    defrag_task_list = []
    for table in tables:

        # Walk parquet directory until some files are found
        base_path = stage_3_path / f"{table}.parquet"
        for root, _, files in os.walk(base_path):
            if files:
                break

        # Using the directory found above, find the total number of partitions used
        # And create a list of all sub-directories in the parquet directory
        n_parts = len(str(Path(root).relative_to(base_path)).split("/"))
        partition_dirs = list(
            filter(lambda f: os.path.isdir(f), glob.glob(f"{base_path}{'/*'*n_parts}"))
        )

        # Setting logpath
        logpath = Path(f"FILEPATH/{getpass.getuser()}") / defrag_wf_name / table
        logpath.mkdir(parents=True, exist_ok=True)

        for filepath in partition_dirs:
            # Creating defrag task and adding to workflow
            defrag_task = defrag_task_temp.create_task(
                name="defrag_"
                + "__".join(filepath.split("/")[-n_parts:]).replace("=", "-"),
                cluster_name="slurm",
                compute_resources={
                    "queue": "long.q",
                    "cores": 20,
                    "memory": "250G",
                    "runtime": "36000",
                    "stdout": logpath.as_posix(),
                    "stderr": logpath.as_posix(),
                    "project": "proj_dex",
                },
                **{"filepath": filepath, "table": table},
            )
            defrag_task_list.append(defrag_task)

    # Running the second workflow
    print(f"Adding tasks to defragmentation workflow.")
    defrag_wf.add_tasks(defrag_task_list)
    print(f"Running defragmentation workflow.")
    defrag_wf_run = defrag_wf.run(seconds_until_timeout=86400)

    # Checking if second workflow completed successfully
    if defrag_wf_run == "D":
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
    # Argument parser, must supply certain arguments to this python script
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-t",
        "--tables",
        nargs="+",
        required=True,
        choices=["rx_claims", "mx_claimline"],
        help="Kythera tables to transform.",
    )

    # Getting input arguments
    args = vars(parser.parse_args())
    tables = args["tables"]

    # Running validation submitter
    run_stage_3_submitter(tables)
