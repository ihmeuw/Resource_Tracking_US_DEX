## ==================================================
## Author(s): Sawyer Crosby
## Date: Jan 31, 2025
## Purpose: Launches DETRUNC application in parallel on a SLURM cluster
## ==================================================

## importing modules
from cmath import log
from multiprocessing.util import log_to_stderr
import sys
import os
from attr import attr
import glob
import shutil
import argparse
import uuid
import getpass
from pathlib import Path
from datetime import datetime
from jobmon.client.api import Tool
from atexit import register
from cProfile import run
from dexdbload.maps import (
    get_map_metadata,
    update_phase_run
)
from dexdbload.pipeline_runs import (
    get_phase_run_id, 
    get_config, 
    parsed_config
)

## ----------------------------------------
## Get run_id and map_version_ids
PRIs = get_phase_run_id() ## get phase run ids
PRI = int(PRIs.loc[(PRIs["status"].isin(["Best", "Active"])), "phase_run_id"].max()) ## defaults to latest, set manually as needed

MVI = get_map_metadata(maps = ["DETRUNC"], status = ["Best"]).map_version_id.values[0]
CAUSEMAP_MVI = int(PRIs.loc[PRIs["phase_run_id"] == PRI, "CAUSEMAP"].values[0]) ## defaults to the map_version applied in this run_id, set manually as needed

## !!! CONFIRM IDS ARE CORRECT
check = "".join([
    "---------------------------------------------",
    "\nConfirm launch of DETRUNC application for:",
    "\n    > run_id: %s ?\n    > DETRUNC map_version_id: %s ?\n    > CAUSEMAP map_version_id: %s ?" % (str(PRI), str(MVI), str(CAUSEMAP_MVI)), 
    "\n[yes/no]: "
])
user_input = input(check)
print("---------------------------------------------")
if user_input != "yes":
    raise ValueError('Must confirm with "yes" to run')
## ----------------------------------------

print("Setting up config")
config = parsed_config(config = get_config(), key = "DETRUNC", run_id = PRI, map_version_id = MVI) 

print("Location scripts and directories")
detrunc_apply_script = os.path.dirname(os.path.abspath(__file__)) + "/apply_map/apply_detrunc.R"
user = getpass.getuser()
log_dir_root = "FILEPATH"

if __name__ == "__main__":

    print("Preparing directories")
    print(" -> Clearing logs")
    shutil.rmtree(log_dir_root, ignore_errors=True)
    Path(log_dir_root).mkdir(exist_ok=True,parents=True)
    ## data
    out_dir = config["data_output_dir"]["MEPS"]
    print(" -> Wiping data")
    shutil.rmtree(out_dir, ignore_errors=True)
    Path(out_dir).mkdir(exist_ok=True,parents=True)

    print("Defining workflow and executor")
    tool = Tool(name="DETRUNC_apply")
    wf_uuid = uuid.uuid4()
    workflow = tool.create_workflow(name=f"detrunc_apply{wf_uuid}")

    print("Defining templates")
    detrunc_template = tool.get_task_template(
        default_compute_resources={
            "queue": "all.q",
            "runtime": "2h",
            "project": "proj_dex",
        },
        template_name="detrunc_apply",
        default_cluster_name="slurm",
        command_template="{r_shell} {script_path} --sub_dataset {sub_dataset} --age {age} --sex {sex} --PRI {PRI} --MVI {MVI} --CAUSEMAP_MVI {CAUSEMAP_MVI}",
        node_args=["sub_dataset", "age", "sex", "PRI", "MVI", "CAUSEMAP_MVI"],
        op_args=["r_shell", "script_path"],
    )

    print("Creating causemap tasks")
    detrunc_apply_tasks = [] ## keep list of task IDs
    sub_datasets = config["sub_dataset"]
    for sub in sub_datasets:
        print(sub)

        CORES = 1
        MEM = "5G"

        log_dir_source = log_dir_root
        sub_datasets = config["sub_dataset"]
        
        ## loop through ages and sexes
        ages = [0,1] + list(range(5,100,5))
        sexes = [1,2]
        for age in ages:
            for sex in sexes:
                task_sub = detrunc_template.create_task( 
                    name = "_".join(["detrunc_apply_", sub]),
                    r_shell = "FILEPATH -s",
                    script_path = detrunc_apply_script,
                    sub_dataset = sub, 
                    age = age, 
                    sex = sex,
                    PRI = PRI, 
                    MVI = MVI,
                    CAUSEMAP_MVI = CAUSEMAP_MVI, 
                    compute_resources={
                        "cores": CORES, 
                        "memory": MEM,
                        "stdout": log_dir_source,
                        "stderr": log_dir_source
                    }                            
                )
                detrunc_apply_tasks.append(task_sub)            
        
    print("Adding tasks to workflow")
    workflow.add_tasks(detrunc_apply_tasks)

    print('Binding to database')
    workflow.bind()
    WFID = str(workflow.workflow_id)
    print("URL")

    print("Running workflow")
    wf_run = workflow.run(seconds_until_timeout = 259200)
    if wf_run == "D":
        try:
            update_phase_run(phase_run_id = int(PRI), map_version_id = int(MVI)) ## if workflow succeeds, update database to show map was applied
        except Exception as e: 
            print(e)
        print("----------------")
        print("DETRUNC applied!")
        print("----------------")
    else:
        raise RuntimeError("The DETRUNC submitter workflow did not complete successfully.")
