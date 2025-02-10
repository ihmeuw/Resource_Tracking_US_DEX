## ==================================================
## Author(s): Sawyer Crosby
## Date: Jan 31, 2025
## Purpose: Launches PRIMARY_CAUSE step in parallel on a SLURM cluster
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
from dexdbload.pipeline_runs import (
    get_phase_run_id, 
    get_config, 
    parsed_config
)
from dexdbload.maps import (
    get_map_metadata
)

## ----------------------------------------
## Get run_id and causemap map_version_id
PRIs = get_phase_run_id() ## get phase run ids
PRI = int(PRIs.loc[(PRIs["status"].isin(["Best", "Active"])), "phase_run_id"].max()) ## defaults to latest, set manually as needed
CAUSEMAP_MVI = int(PRIs.loc[PRIs["phase_run_id"] == PRI, "CAUSEMAP"].values[0]) ## defaults to the map_version applied in this run_id, set manually as needed

## !!! CONFIRM IDS ARE CORRECT
check = "".join([
    "--------------------------------------------------",
    "\nConfirm launch of PRIMARY_CAUSE application for:",
    "\n    > run_id: %s ?\n    > CAUSEMAP map_version_id: %s ?" % (str(PRI), str(CAUSEMAP_MVI)), 
    "\n[yes/no]: "
])
user_input = input(check)
print("--------------------------------------------------")
if user_input != "yes":
    raise ValueError('Must confirm with "yes" to run')
## ----------------------------------------

## determing memory allocation -- (roughly based on 2*MaxRSS of largest job)
## (Some of these are parallelized by state -- see below)
MEMORY = {
    "SIDS": "20G",
    "SEDD": "20G",
    "NIS": "20G",
    "NEDS": "20G",
    "MSCAN": "20G",
    "KYTHERA": "60G",
    "MDCD": "60G",
    "MDCR": "70G",
    "CHIA_MDCR": "20G"
}

print("Setting up config")
raw_config = get_config()
primary_cause_config = parsed_config(config = raw_config, key = "PRIMARY_CAUSE", run_id = PRI)
causemap_config = parsed_config(raw_config, key = "CAUSEMAP", run_id = PRI, map_version_id = CAUSEMAP_MVI)
metadata_config = raw_config["METADATA"]

print("Location scripts and directories")
primary_cause_script = os.path.dirname(os.path.abspath(__file__)) + "/primary_cause.py"
user = getpass.getuser()
log_dir_root = "FILEPATH"

## get list of states for MDCR parallelization
states = [
    "-1", "AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DC", "DE",
    "FL", "GA", "HI", "IA", "ID", "IL", "IN", "KS", "KY", "LA", "MA",
    "MD", "ME", "MI", "MN", "MO", "MS", "MT", "NC", "ND", "NE", "NH",
    "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA", "RI", "SC", "SD",
    "TN", "TX", "UT", "VA", "VT", "WA", "WI", "WV", "WY"
]

if __name__ == "__main__":

    print("Parsing arguments")
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--sources", nargs="+", type=str, required=True, choices=["ALL", "SIDS", "SEDD", "NIS", "NEDS", "KYTHERA", "MSCAN", "MDCD", "MDCR", "CHIA_MDCR"],help="source")
    args = vars(parser.parse_args())
    print(args)

    sources = args["sources"]
    if sources == ["ALL"]:
        sources = ["SIDS", "SEDD", "NIS", "NEDS", "KYTHERA", "MSCAN", "MDCD", "MDCR", "CHIA_MDCR"]
    
    ## !!! CONFIRM SOURCES ARE CORRECT
    check2 = "".join([
        "--------------------------------------------------",
        "\nConfirm launch of PRIMARY_CAUSE application for:",
        "\n    > sources: %s ?" % (", ".join(sources)), 
        "\n[yes/no]: "
    ])
    user_input2 = input(check2)
    print("--------------------------------------------------")
    if user_input2 != "yes":
        raise ValueError('Must confirm with "yes" to run')
    ## ----------------------------------------

    print("Preparing directories")
    for source in sources:
        print(source)
        ## logs
        print(" -> Wiping logs")
        log_dir_source = log_dir_root + source
        shutil.rmtree(log_dir_source, ignore_errors=True)
        Path(log_dir_source).mkdir(exist_ok=True,parents=True)
        # data
        print(" -> Wiping data")
        out_dir = primary_cause_config["data_output_dir"][source]
        shutil.rmtree(out_dir, ignore_errors=True)
        Path(out_dir).mkdir(exist_ok=True,parents=True)

    print("Defining workflow and executor")
    tool = Tool(name="PRIMARY_CAUSE")
    wf_uuid = uuid.uuid4()
    workflow = tool.create_workflow(name=f"primary_cause{wf_uuid}")

    print("Defining templates")
    primary_cause_template = tool.get_task_template( 
        default_compute_resources={
            "queue": "all.q",
            "runtime": "4h",
            "project": "proj_dex"
        },
        template_name="primarycause",
        default_cluster_name="slurm",
        command_template="{python} {script_path} --source {source} --toc {toc} --year {year} --state {state} --PRI {PRI} --CAUSEMAP_MVI {CAUSEMAP_MVI}",
        node_args=["source", "toc", "year", "state", "PRI", "CAUSEMAP_MVI"],
        op_args=["python", "script_path"],
    )

    print("Creating primary_cause tasks")
    primary_cause_tasks = [] ## keep list of task IDs
    for source in sources:
        print(source)

        CORES = 4
        MEM = MEMORY[source]

        log_dir_source = log_dir_root + source

        ## Drop DV and RX
        tocs = [x for x in metadata_config["tocs"][source] if x not in ["DV", "RX"]]
      
        for toc in tocs:
            print(toc)

            ## find data directory
            if toc == "RX":
                datadir = causemap_config['data_output_dir'][source] + "data" + "/toc=" + toc    
            else:
                datadir = primary_cause_config['data_input_dir'][source] + "data" + "/toc=" + toc

            ## find which years for each source
            files = os.listdir(datadir)
            years = list(map(lambda sub:int("".join([ele for ele in sub if ele.isnumeric()])), files))
            years.sort()

            ## for the big bois, parallelize on toc/year/state
            more_mem = [
                "MDCR_AM",
                "MDCR_ED",
                "MDCR_HH",
                "MDCR_IP",
                "MDCD_AM",
                "KYTHERA_AM", 
                "KYTHERA_RX", 
                "MSCAN_AM",
                "MSCAN_RX"
            ]
            if "_".join([source, toc]) in more_mem:
                for yr in years:
                    print("   > " + str(yr) + " (creating state-specific jobs)")
                    for st in states:
                        ## add task to workflow
                        big_task = primary_cause_template.create_task(
                            name = "_".join(["primary_cause", source, toc, str(yr), st]),
                            python = sys.executable,
                            script_path = primary_cause_script,
                            source = source, 
                            toc = toc, 
                            year = yr,
                            state = st,
                            PRI = PRI,
                            CAUSEMAP_MVI = CAUSEMAP_MVI,
                            compute_resources={
                                "cores": CORES, 
                                "memory": MEM,
                                "stdout": log_dir_source,
                                "stderr": log_dir_source
                            }
                        )
                        primary_cause_tasks.append(big_task)
            
            ## otherwise, just parallelize on toc/year
            else: 
                for yr in years:
                    print("   > " + str(yr))
                    ## add task to workflow
                    other_task = primary_cause_template.create_task(
                        name = "_".join(["primary_cause", source, toc, str(yr)]),
                        python = sys.executable,
                        script_path = primary_cause_script,
                        source = source, 
                        toc = toc, 
                        year = yr,
                        state = "all",
                        PRI = PRI,
                        CAUSEMAP_MVI = CAUSEMAP_MVI,
                        compute_resources={
                                "cores": CORES, 
                                "memory": MEM,
                                "stdout": log_dir_source,
                                "stderr": log_dir_source
                            }
                    )
                    primary_cause_tasks.append(other_task)

    print("Adding tasks to workflow")
    workflow.add_tasks(primary_cause_tasks)

    print('Binding to database')
    workflow.bind()
    WFID = str(workflow.workflow_id)
    print("URL")

    print("Running workflow")
    wf_run = workflow.run(seconds_until_timeout = 259200)
    if wf_run == "D":
        print("----------------------")
        print("PRIMARY_CAUSE applied!")
        print("----------------------")
    else:
        raise RuntimeError("The primary_cause workflow did not complete successfully.")
