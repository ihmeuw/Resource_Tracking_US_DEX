## ==================================================
## Author(s): Max Weil
## Purpose: This script is used to submit stage 3 processing tasks for MarketScan data.
## ==================================================

import argparse
from datetime import datetime
from pathlib import Path
from jobmon.client.api import Tool
import sys
import getpass
import dexdbload.pipeline_runs as dex_pr


def run_stage_3_submitter(yrs, rx_proc):

    # Getting config filepaths
    config = dex_pr.get_config()

    # Getting stage 2 path
    stage_2_path = Path(
        dex_pr.parsed_config(config, key="FORMAT")["stage_2_dir"]["MSCAN"]
    )

    # Finding all filepaths in stage 2 directory that match the specified years and processing type
    filepaths_to_proc = {}
    if rx_proc:
        for yr in yrs:
            filepaths_to_proc[yr] = [
                str(fp)
                for fp in stage_2_path.iterdir()
                if yr in fp.stem and "_d_" in fp.stem
            ]
    else:
        for yr in yrs:
            filepaths_to_proc[yr] = [
                str(fp)
                for fp in stage_2_path.iterdir()
                if yr in fp.stem and "_d_" not in fp.stem
            ]

    # Raising error if no files are found
    if not filepaths_to_proc:
        raise RuntimeError(
            f"Something went wrong. No files for the years(s) of {yrs} were found in {str(stage_2_path)}."
        )

    # Creating a Jobmon workflow for all years of data.
    # Tasks are created for each row group batch of each file.

    # Setting up Jobmon workflow/tool info
    job_tool = Tool(name="MKTSCN_stage_3")
    wf_name = f"dex_MKTSCN_transform_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}"
    wf = job_tool.create_workflow(workflow_args=wf_name, name=wf_name)

    # Creating task templates for processing and defragmenting
    process_task_temp = create_task_temp(
        job_tool,
        task_temp_name="batch_transform",
        script_path=Path(__file__).resolve().parent
        / "helpers"
        / "mktscn_transformer.py",
        script_args=["filepaths", "year", "outpath", "rx_proc"],
    )
    defrag_task_temp = create_task_temp(
        job_tool,
        task_temp_name="defrag",
        script_path=Path(__file__).resolve().parent
        / "helpers"
        / "mktscn_defragmenter.py",
        script_args=["filepath"],
    )

    # Getting stage 3 path
    stage_3_path = Path(
        dex_pr.parsed_config(config, key="FORMAT")["stage_3_dir"]["MSCAN"]
    )

    # For each year of data, create a processing task
    print(f"Creating processing tasks.")
    processing_task_list = []
    outpaths = []
    for yr, filepaths in filepaths_to_proc.items():

        # Creating outpath and logpath for each file
        outpath = stage_3_path / wf_name / f"transformed_MKTSCN_{yr}.parquet"
        outpath.mkdir(parents=True, exist_ok=True)
        logpath = Path(f"FILEPATH/{getpass.getuser()}") / wf_name / outpath.stem
        logpath.mkdir(parents=True, exist_ok=True)

        # Creating process task and adding to workflow
        processing_task = process_task_temp.create_task(
            name=outpath.stem,
            cluster_name="slurm",
            compute_resources={
                "queue": "all.q",
                "cores": 1,
                "memory": "2G",
                "runtime": "36000s",
                "stdout": logpath.as_posix(),
                "stderr": logpath.as_posix(),
                "project": "proj_dex",
                "constraints": "archive",
            },
            **{
                "filepaths": " ".join(filepaths),
                "year": yr,
                "outpath": outpath,
                "rx_proc": rx_proc,
            },
        )
        processing_task_list.append(processing_task)

        # Adding outpath for defragmenting step
        outpaths.append(outpath)

    # For each output file, create a defragmenting task
    print(f"Creating defragmenting tasks.")
    defrag_task_list = []
    for filepath in outpaths:

        # Establishing logpath for each task
        # Should already exist from processing
        logpath = Path(f"FILEPATH/{getpass.getuser()}") / wf_name / outpath.stem

        # Creating defrag task and adding to workflow
        defrag_task = defrag_task_temp.create_task(
            name="defrag_" + outpath.stem,
            cluster_name="slurm",
            upstream_tasks=processing_task_list,
            compute_resources={
                "queue": "long.q",
                "cores": 20,
                "memory": "200G",
                "runtime": "5400s",
                "stdout": logpath.as_posix(),
                "stderr": logpath.as_posix(),
                "project": "proj_dex",
            },
            **{"filepath": filepath},
        )
        defrag_task_list.append(defrag_task)

    # Running the workflow
    print(f"Adding tasks to workflow and running.")
    wf.add_tasks(processing_task_list + defrag_task_list)
    wf_run = wf.run()

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
    parser = argparse.ArgumentParser()

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
        help="Years of MarketScan stage 2 data to process.",
    )

    # Specifying if processing RX or IP/OP
    parser.add_argument(
        "-r",
        "--rx_proc",
        required=True,
        choices=[1, 0],
        type=int,
        help="Flag of whether to process RX or not (processed separately from IP/OP).",
    )

    # Getting args from parser and running stage 3 workflow
    args = vars(parser.parse_args())
    yrs = args["years"]
    rx_proc = args["rx_proc"]
    run_stage_3_submitter(yrs, rx_proc)
