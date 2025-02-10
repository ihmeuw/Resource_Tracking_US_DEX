## ==================================================
## Author(s): Sawyer Crosby
## Date: Jan 31, 2025
## Purpose: Launches DETRUNC map creation in parallel on a SLURM cluster
## ==================================================

## importing modules
from cmath import log
from multiprocessing.util import log_to_stderr
import sys
import os
from attr import attr
import shutil
import uuid
import getpass
from pathlib import Path
from jobmon.client.api import Tool
from atexit import register
from cProfile import run
from dexdbload.pipeline_runs import (
    get_phase_run_id, 
    get_config, 
    parsed_config
)
from dexdbload.maps import (
    register_map_version
)
from dexdbload.load_data import (
    mark_best
)

## ----------------------------------------
## Get run_id
PRIs = get_phase_run_id() ## get phase run ids
PRI = int(PRIs.loc[(PRIs["status"].isin(["Best", "Active"])), "phase_run_id"].max()) ## defaults to latest, set manually as needed

## !!! CONFIRM IDS ARE CORRECT
check = "".join([
    "--------------------------------------------------",
    "\nConfirm launch of DETRUNC map creation for:",
    "\n    > run_id: %s ?" % (str(PRI)), 
    "\n[yes/no]: "
])
user_input = input(check)
print("--------------------------------------------------")
if user_input != "yes":
    raise ValueError('Must confirm with "yes" to run')

## register map after checking run_id
MVI = register_map_version(related_map = "DETRUNC", phase_run_id = PRI)
print("Map version id: " + str(MVI))
## ----------------------------------------

## determing memory allocation -- (roughly based on 2*MaxRSS of largest job)
## (Some of these are parallelized by state -- see below)
MEMORY = {
    "MSCAN": "30G",
    "MDCD": "60G",
    "MDCR": "70G", 
}

print("Setting up config")
raw_config = get_config()
detrunc_config = parsed_config(config = raw_config, key = "DETRUNC", run_id = PRI, map_version_id =  MVI)
metadata_config = raw_config["METADATA"]

print("Location scripts and directories")
detrunc_create_script = os.path.dirname(os.path.abspath(__file__)) + "/create_map/create_detrunc.py"
detrunc_agg_script = os.path.dirname(os.path.abspath(__file__)) + "/create_map/aggregate_detrunc.py"
user = getpass.getuser()
log_dir_root = "FILEPATH"

## get list of states for parallelization
states = [
    "unknown", "AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DC", "DE",
    "FL", "GA", "HI", "IA", "ID", "IL", "IN", "KS", "KY", "LA", "MA",
    "MD", "ME", "MI", "MN", "MO", "MS", "MT", "NC", "ND", "NE", "NH",
    "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA", "RI", "SC", "SD",
    "TN", "TX", "UT", "VA", "VT", "WA", "WI", "WV", "WY"
]

## always run for all sources
sources = ["MSCAN", "MDCD", "MDCR"]

if __name__ == "__main__":
    
    print("Preparing directories")
    for source in sources:
        print(source)
        ## logs
        print(" -> Wiping logs")
        log_dir_source = log_dir_root + source
        shutil.rmtree(log_dir_source, ignore_errors=True)
        Path(log_dir_source).mkdir(exist_ok=True,parents=True)
    
    ## data
    print("Wiping data (if any)")
    out_dir = detrunc_config["map_output_dir"]
    shutil.rmtree(out_dir, ignore_errors=True)
    Path(out_dir).mkdir(exist_ok=True,parents=True)

    print("Defining workflow and executor")
    tool = Tool(name="DETRUNC_create")
    wf_uuid = uuid.uuid4()
    workflow = tool.create_workflow(name=f"detrunc_create{wf_uuid}")

    print("Defining templates")
    detrunc_template = tool.get_task_template( 
        default_compute_resources={
            "queue": "all.q",
            "runtime": "2h",
            "project": "proj_dex"
        },
        template_name="detrunc_create",
        default_cluster_name="slurm",
        command_template="{python} {script_path} --source {source} --toc {toc} --year {year} --state {state} --PRI {PRI} --MVI {MVI}",
        node_args=["source", "toc", "year", "state", "PRI", "MVI"],
        op_args=["python", "script_path"],
    )

    agg_template = tool.get_task_template( 
        default_compute_resources={
            "queue": "all.q",
            "runtime": "2h",
            "project": "proj_dex"
        },
        template_name="detrunc_agg",
        default_cluster_name="slurm",
        command_template="{python} {script_path} --MVI {MVI}",
        node_args=["MVI"],
        op_args=["python", "script_path"],
    )

    print("Creating tasks")
    detrunc_create_tasks = [] ## keep list of task IDs
    for source in sources:
        print(source)

        CORES = 4
        MEM = MEMORY[source]

        log_dir_source = log_dir_root + source

        ## drop DV/RX
        tocs = metadata_config["tocs"][source]
        tocs = [x for x in tocs if x not in ["DV", "RX"]]

        for toc in tocs:
            print("  " + toc)

            ## find data directory
            datadir = detrunc_config['data_input_dir'][source] + "data" + "/toc=" + toc

            ## find which years for each source
            files = os.listdir(datadir)
            years = list(map(lambda sub:int("".join([ele for ele in sub if ele.isnumeric()])), files))
            years.sort()

            ## for the combinations that require more memory, parallelize on toc/year/state
            more_mem = [
                "MDCR_AM",
                "MDCR_ED",
                "MDCR_IP",
                "MDCD_AM",
                "MSCAN_AM"
            ]
            if "_".join([source, toc]) in more_mem:
                for yr in years:
                    print("   > " + str(yr) + " (creating state-specific jobs)")
                    for st in states:
                        ## add task to workflow
                        big_task = detrunc_template.create_task(
                            name = "_".join(["detrunc_create", source, toc, str(yr), st]),
                            python = sys.executable,
                            script_path = detrunc_create_script,
                            source = source, 
                            toc = toc, 
                            year = yr,
                            state = st,
                            PRI = PRI,
                            MVI = MVI,
                            compute_resources={
                                "cores": CORES, 
                                "memory": MEM,
                                "stdout": log_dir_source,
                                "stderr": log_dir_source
                            }
                        )
                        detrunc_create_tasks.append(big_task)
            
            ## otherwise, just parallelize on toc/year
            else: 
                for yr in years:
                    print("   > " + str(yr))
                    ## add task to workflow
                    other_task = detrunc_template.create_task(
                        name = "_".join(["detrunc_create", source, toc, str(yr)]),
                        python = sys.executable,
                        script_path = detrunc_create_script,
                        source = source, 
                        toc = toc, 
                        year = yr,
                        state = "all",
                        PRI = PRI,
                        MVI = MVI,
                        compute_resources={
                                "cores": CORES, 
                                "memory": MEM,
                                "stdout": log_dir_source,
                                "stderr": log_dir_source
                            }
                    )
                    detrunc_create_tasks.append(other_task)

    agg_task = agg_template.create_task(
        name = "detrunc_agg",
        upstream_tasks = detrunc_create_tasks,
        python = sys.executable,
        script_path = detrunc_agg_script,
        MVI = MVI,
        compute_resources={
                "cores": 10, 
                "memory": "100G",
                "stdout": log_dir_root,
                "stderr": log_dir_root
            }
    )

    print("Adding tasks to workflow")
    workflow.add_tasks(detrunc_create_tasks)
    workflow.add_tasks([agg_task])

    print('Binding to database')
    workflow.bind()
    WFID = str(workflow.workflow_id)
    print("URL")

    print("Running workflow")
    wf_run = workflow.run(seconds_until_timeout = 259200)
    if wf_run == "D":
        mark_best(table = "map_version", table_id = MVI)
        print("----------------------")
        print("DETRUNC map created!")
        print("----------------------")
        
    else:
        raise RuntimeError("The create_detrunc workflow did not complete successfully.")
