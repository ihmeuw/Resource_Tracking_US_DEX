## ==================================================
## Author(s): Sawyer Crosby
## Date: Jan 31, 2025
## Purpose: Launches PRIV_UTIL step in parallel on a SLURM cluster
## ==================================================

import argparse
from datetime import datetime
from pathlib import Path
from jobmon.client.api import Tool
import sys
import getpass
import os
import glob
import pandas as pd
import shutil
from dexdbload.pipeline_runs import get_config, parsed_config

def find_dir_depth(root_dir, target_str):
    for root, dirs, files in os.walk(root_dir):
        if target_str in root:  # Check if the directory path contains the target string
            return root.replace(root_dir, '').count(os.sep)
    return 0  # Target directory not found

def create_task_temp(tool, task_temp_name, script_path, script_args):
    
    # Formatting command template from desired arguments
    command_template = (
        f"{sys.executable} {script_path} " + "".join([f"--{arg} {{{arg}}} " for arg in script_args])[:-1]
    )

    # Creating task template
    task_temp = tool.get_task_template(
        template_name=task_temp_name,
        command_template=command_template,
        node_args=script_args
    )

    return task_temp

def run_priv_util(pri, sources):

    # Loading in config information for input/output directories
    raw_config = get_config()
    config = parsed_config(raw_config, key = "PRIV_UTIL", run_id = pri)

    # Setting up Jobmon workflow/tool info
    job_tool = Tool(name="PRIV_UTIL")
    util_wf_name = f'dex_PRIV_UTIL_{datetime.now().strftime("%Y-%m-%d_%H%M%S")}'
    util_wf = job_tool.create_workflow(workflow_args=util_wf_name,
                                  name=util_wf_name)

    # Creating task template for privtate utilization calculation
    util_task_temp = create_task_temp(job_tool,
                                    task_temp_name="priv_util_counts",
                                    script_path=Path(__file__).resolve().parent / "priv_util_counts.py",
                                    script_args=["source", "filepath", "outpath"])

    # Based on the sources provided, creating jobs at a certain partition level
    job_paths = {}

    # Looping through sources to find directories to process
    print('\nIdentifying filepaths.')
    for source in sources:

        print("   > " + source)
        
        ## identify directories
        main_path = config['data_input_dir'][source] + "data"
        main_depth = find_dir_depth(main_path, "year_id")
        main_part_dirs = glob.glob(f"{main_path}{'/*'*main_depth}")

        ## and for RX
        if source in config['data_input_dir_RX']:
            rx_path = config['data_input_dir_RX'][source] + "data/toc=RX"
            rx_depth = find_dir_depth(rx_path, "year_id")
            rx_part_dirs = glob.glob(f"{rx_path}{'/*'*rx_depth}")
        else:
            rx_part_dirs = []

        ## and for DV
        if source in config['data_input_dir_DV']:
            dv_path = config['data_input_dir_RX'][source] + "data/toc=DV"
            dv_depth = find_dir_depth(dv_path, "year_id")
            dv_part_dirs = glob.glob(f"{dv_path}{'/*'*dv_depth}")
        else:
            dv_part_dirs = []

        # Adding the source and list of directories to the job_paths dictionary
        job_paths[source] = main_part_dirs + rx_part_dirs + dv_part_dirs
        
    # Creating tasks
    print("\nPreparing directories.")
    util_task_list = []
    for source, filepaths in job_paths.items():
        print("   > " + source)
        # Setting logpath
        logpath = Path("FILEPATH") / source
        print("      - wiping logs")
        shutil.rmtree(logpath, ignore_errors=True)
        logpath.mkdir(parents=True, exist_ok=True)
        
        # Setting outpath and clearing if data is already present
        outpath = config['data_output_dir'][source] + "data"
        print("      - wiping data")
        shutil.rmtree(outpath, ignore_errors=True)
        Path(outpath).mkdir(parents=True, exist_ok=True)
        
        for filepath in filepaths:
            # Creating defrag task and adding to workflow
            util_task = util_task_temp.create_task(
                name=f"priv_util_{source}",
                cluster_name="slurm",
                compute_resources={
                    "queue": "all.q",
                    "cores": 1,
                    "memory": "50G",
                    "runtime": "7200", ## 2hr
                    "stdout": logpath.as_posix(),
                    "stderr": logpath.as_posix(),
                    "project": "proj_dex",
                },
                **{"source" : source,
                "filepath": filepath,
                "outpath": outpath})
            util_task_list.append(util_task)    
    
    print("Adding tasks to workflow")
    util_wf.add_tasks(util_task_list)

    print('Binding to database')
    util_wf.bind()
    WFID = str(util_wf.workflow_id)
    print("URL")

    print("Running workflow")
    wf_run = util_wf.run(seconds_until_timeout = 259200)
    if wf_run == "D":
        print(f"Workflow completed successfully!")
    else:
        print(f"The workflow did not complete successfully. Please check error logs.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--phase_run_id",
                        type=int,
                        required=True,
                        help="Phase run ID of pipeline run.")
    parser.add_argument("-s", "--sources",
                        type=str,
                        nargs="+",
                        choices=["SIDS", "SEDD", "NIS", "NEDS", "MSCAN", "KYTHERA"],
                        default=["SIDS", "SEDD", "NIS", "NEDS", "MSCAN", "KYTHERA"],
                        help="Data sources to run count private utiliation for.")
    args = vars(parser.parse_args())
    pri = args["phase_run_id"]
    sources = args["sources"]

    # !!! CONFIRM
    check = "".join([
        "---------------------------------------------",
        "\nConfirm correct args:",
        "\n    > run_id:  %s ?" % (str(pri)), 
        "\n    > sources:  %s" % (", ".join(sources)), 
        "\n[yes/no]: "
    ])
    user_input = input(check)
    print("---------------------------------------------")
    if user_input != "yes":
        raise ValueError('Must confirm with "yes" to run')

    run_priv_util(pri, sources)
