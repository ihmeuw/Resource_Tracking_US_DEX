## ==================================================
## Author(s): Max Weil
## Purpose: This script is used to submit denominator processing tasks for MarketScan data.
## ==================================================

import argparse
from datetime import datetime
from pathlib import Path
from jobmon.client.api import Tool
import sys
import getpass


def run_denom_submitter(datasets):

    # Finding all filepaths in base marketscan folder
    basepath = Path("FILEPATH")
    filepaths = [i for i in basepath.iterdir() if "_t_" in i.stem]

    # Finding all filepaths that match the specified datasets
    filepaths_to_proc = [i for i in filepaths if any(j in i.stem for j in datasets)]

    # Raising error if no files are found
    if not filepaths_to_proc:
        raise RuntimeError(
            f"Something went wrong. No files with {datasets} dataset name(s) were found in {str(basepath)}."
        )

    # Setting up Jobmon workflow/tool info
    job_tool = Tool(name="MKTSCN_denoms")
    wf_name = f"dex_MKTSCN_denoms_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}"
    wf = job_tool.create_workflow(workflow_args=wf_name, name=wf_name)

    # Creating task template
    denom_task_temp = create_task_temp(
        job_tool,
        task_temp_name="get_denoms",
        script_path=Path(__file__).resolve().parent / "helpers" / "get_denoms.py",
        script_args=["filepath"],
    )

    # For each filepath, create a task
    print(f"Creating get denom tasks.")
    denom_task_list = []
    for filepath in filepaths_to_proc:

        # Creating logpath for each file
        logpath = Path(f"FILEPATH/{getpass.getuser()}") / wf_name / filepath.stem
        logpath.mkdir(parents=True, exist_ok=True)

        # Creating batch task and adding to workflow
        denom_task = denom_task_temp.create_task(
            name=filepath.stem,
            cluster_name="slurm",
            compute_resources={
                "queue": "all.q",
                "cores": 1,
                "memory": "100G",
                "runtime": "10800s",
                "stdout": logpath.as_posix(),
                "stderr": logpath.as_posix(),
                "project": "proj_dex",
                "constraints": "archive",
            },
            **{"filepath": filepath},
        )
        denom_task_list.append(denom_task)

    # Running the workflow
    print(f"Adding tasks to workflow and running.")
    wf.add_tasks(denom_task_list)
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
        help="MarketScan datasets to get denoms for.",
    )

    # Getting args from parser
    args = vars(parser.parse_args())
    datasets = args["datasets"]

    # Running denom workflow submission
    run_denom_submitter(datasets)
