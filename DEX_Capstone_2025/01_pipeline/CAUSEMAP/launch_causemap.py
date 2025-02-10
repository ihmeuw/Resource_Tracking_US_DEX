## ==================================================
## Author(s): Sawyer Crosby
## Date: Jan 31, 2025
## Purpose: Launch the CAUSEMAP step in parallel on a SLURM cluster
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
## Get phase_run_id and causemap map_version_id
PRIs = get_phase_run_id()
PRI = int(PRIs.loc[(PRIs["status"].isin(["Best", "Active"])), "phase_run_id"].max()) ## defaults to latest, set manually as needed
MVI = int(get_map_metadata(maps = ["CAUSEMAP"], status = ['best']).map_version_id.values[0]) ## defaults to best, set manually as needed

## User to confirm IDs are correct
check = "".join([
    "---------------------------------------------",
    "\nConfirm correct ids:",
    "\n    > run_id: %s ?\n    > map_version_id: %s ?" % (str(PRI), str(MVI)), 
    "\n[yes/no]: "
])
user_input = input(check)
print("---------------------------------------------")
if user_input != "yes":
    raise ValueError('Must confirm with "yes" to run')
## ----------------------------------------

## List of states
states = [
    "-1", "AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DC", "DE", 
    "FL", "GA", "HI", "IA", "ID", "IL", "IN", "KS", "KY", "LA", "MA", 
    "MD", "ME", "MI", "MN", "MO", "MS", "MT", "NC", "ND", "NE", "NH", 
    "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA", "RI", "SC", "SD", 
    "TN", "TX", "UT", "VA", "VT", "WA", "WI", "WV", "WY"
]

## List of DEX ages and sexes, -1 is included to count NAs
ages = [-1, 0, 1] + list(range(5,100,5))
sexes = [-1, 1, 2]

## determing memory allocation -- (roughly based on 2*MaxRSS of largest job)
## (Some of these are parallelized by state or state+age+sex -- see below)
MEMORY = {
    "SIDS": "20G", 
    "SEDD": "20G", 
    "NIS": "20G", 
    "NEDS": "20G",
    "MSCAN": "60G",
    "KYTHERA": "60G",
    "MDCD": "100G",
    "MDCR": "100G",
    "CHIA_MDCR": "20G"
}

print("Setting up config")
raw_config = get_config()
config = parsed_config(raw_config, key = "CAUSEMAP", run_id = PRI, map_version_id = MVI) 

print("Location scripts and directories")
causemap_script = os.path.dirname(os.path.abspath(__file__)) + "/causemap.py"
user = getpass.getuser()
log_dir_root = "FILEPATH"

if __name__ == "__main__":

    print("Parsing arguments")
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--sources", nargs="+", type=str, required=True, choices=["ALL", "SIDS", "SEDD", "NIS", "NEDS", "KYTHERA", "MSCAN", "MDCD", "MDCR", "CHIA_MDCR"], help="source")
    parser.add_argument("-x", "--RX", type=str, required=False, choices=["include", "exclude", "only_rx"], help="Run for RX?", default = "include")
    parser.add_argument("-w", "--wipe", type=str, required=False, choices=["yes", "no"], help="Wipe old outputs", default = "yes")
    args = vars(parser.parse_args())
    print(args)

    sources = args["sources"]
    rx_option = args["RX"]
    wipe = args["wipe"]
    if sources == ["ALL"]:
        sources =  ["SIDS", "SEDD", "NIS", "NEDS", "KYTHERA", "MSCAN", "MDCD", "MDCR", "CHIA_MDCR"]
    
    ## !!! CONFIRM SOURCES ARE CORRECT
    check2 = "".join([
        "---------------------------------------------",
        "\nConfirm launch of CAUSEMAP application for:",
        "\n    > Sources:   %s" % (", ".join(sources)), 
        "\n    > RX inclusion option:   %s" % (rx_option),
        "\n    > Clearing previous outputs:   %s" % (wipe),
        "\n[yes/no]: "
    ])
    user_input2 = input(check2)
    print("---------------------------------------------")
    if user_input2 != "yes":
        raise ValueError('Must confirm with "yes" to run')
    ## ----------------------------------------

    print("Preparing directories")
    if wipe == "yes": ## clear then remake folders
        for source in sources:
            print(source)
            # logs
            print(" -> Clearing logs")
            log_dir_source = log_dir_root + source
            shutil.rmtree(log_dir_source, ignore_errors=True)
            Path(log_dir_source).mkdir(exist_ok=True,parents=True)
            # data
            out_dir = config["data_output_dir"][source]
            print(" -> Wiping data")
            shutil.rmtree(out_dir, ignore_errors=True)
            Path(out_dir).mkdir(exist_ok=True,parents=True)
            
    else:  ## just ensure folders exist
        for source in sources:
            print(source)
            log_dir_source = log_dir_root + source
            Path(log_dir_source).mkdir(exist_ok=True,parents=True)
            out_dir = config["data_output_dir"][source]
            Path(out_dir).mkdir(exist_ok=True,parents=True)

    print("Defining workflow and executor")
    tool = Tool(name="CAUSEMAP")
    wf_uuid = uuid.uuid4()
    workflow = tool.create_workflow(name=f"causemap{wf_uuid}")

    print("Defining templates")
    causemap_template = tool.get_task_template( 
        default_compute_resources={
            "queue": "all.q",
            "project": "proj_dex"
        },
        template_name="causemap",
        default_cluster_name="slurm",
        command_template="{python} {script_path} --source {source} --sub_dataset {sub_dataset} --year {year} --age {age} --sex {sex} --state {state} --PRI {PRI} --MVI {MVI}",
        node_args=["source", "sub_dataset", "year", "age", "sex", "state", "PRI", "MVI"],
        op_args=["python", "script_path"],
    )

    print("Creating causemap tasks")
    causemap_tasks = [] ## keep list of task IDs
    for source in sources:
        print(source)

        ## starting mem/cores -> works for most things
        CORES = 1
        MEM = MEMORY[source]
        RUNTIME = "10h"

        log_dir_source = log_dir_root + source
        sub_datasets = config["sub_dataset"]
                
        ## sources with sub datasets (MDCR/MDCD/MSCAN/KYTHERA)
        if source in sub_datasets: 

            if rx_option == "only_rx":
                ## filter to only sub-datasets with "rx" string in the name
                sub_datasets[source] = [x for x in sub_datasets[source] if "rx" in x]
            elif rx_option == "exclude":
                ## remove sub-datasets with "rx" string in the name
                sub_datasets[source] = [x for x in sub_datasets[source] if not "rx" in x]
            
            for sub in sub_datasets[source]:
                print(" - " + sub)
                
                ## find which years for each source + subdataset
                if source in ["MDCR", "MDCD", "CHIA_MDCR"]:
                    datadir = config["data_input_dir"][source] + sub + "/best/*/"
                    file_example = os.listdir(glob.glob(datadir)[0])[0]
                    if file_example.startswith("year_id"):
                        files = os.listdir(glob.glob(datadir)[0])
                        files = [x for x in files if "year_id" in x]
                        years = list(map(lambda x:int("".join([ele for ele in x if ele.isnumeric()])), files))
                    else:
                        files = glob.glob(datadir + "*/*") ## need to go two layers deep here...
                        files = set([f.rsplit("/", 1)[1] for f in files])
                        files = [x for x in files if "year_id" in x]
                        years = list(map(lambda x:int("".join([ele for ele in x if ele.isnumeric()])), files))
                elif source in ["MSCAN", "KYTHERA"]:
                    datadir = config["data_input_dir"][source] + "best_" + sub + "/"
                    files = os.listdir(glob.glob(datadir)[0])
                    files = [x for x in files if "year_id" in x or "MKTSCN" in x] ## filter out other files
                    years = list(map(lambda x:int("".join([ele for ele in x if ele.isnumeric()])), files))
                years.sort()

                ## for some sources, parallelize on year/state
                more_mem = [
                    "MDCR_hop",
                    "MDCR_rx",
                    "MDCR_rx_part_c",
                    "MDCD_ot",
                    "MDCD_ot_taf",
                    "MDCD_rx_taf", 
                    "MSCAN_main", 
                    "MSCAN_rx", 
                    "KYTHERA_main", 
                    "KYTHERA_rx"
                ]
                if "_".join([source, sub]) in more_mem:
                    for yr in years:
                        print("     > " + str(yr) + " (creating state-specific jobs)")
                        for st in states:
                            if st in ["CA", "FL", "NY", "TX", "OH", "PA"]: ## bump mem for big states
                                MEM = "300G" 
                            if "_".join([source, sub]) in ["KYTHERA_main", "MSCAN_main", "MDCD_ot_taf", "MDCD_dv_taf"]: ## bump runtime for a few
                                RUNTIME = "18h"
                            ## add task to workflow
                            task_sub_state = causemap_template.create_task( 
                                name = "_".join(["causemap", source, sub, str(yr), st]),
                                python = sys.executable,
                                script_path = causemap_script,
                                source = source, 
                                sub_dataset = sub, 
                                year = yr,
                                age = 'all',
                                sex = 'all',
                                state = st,
                                PRI = PRI, 
                                MVI = MVI,
                                compute_resources={
                                    "cores": CORES, 
                                    "memory": MEM,
                                    "runtime": RUNTIME,
                                    "stdout": log_dir_source,
                                    "stderr": log_dir_source
                                }                            
                            )
                            causemap_tasks.append(task_sub_state)
                
                ## otherwise, just parallelize on year
                else: 
                    for yr in years:
                        print("     > " + str(yr))
                        ## add task to workflow
                        task_sub = causemap_template.create_task( 
                            name = "_".join(["causemap", source, sub, str(yr)]),
                            python = sys.executable,
                            script_path = causemap_script,
                            source = source, 
                            sub_dataset = sub, 
                            year = yr,
                            age = 'all',
                            sex = 'all',
                            state = "all",
                            PRI = PRI, 
                            MVI = MVI,
                            compute_resources={
                                "cores": CORES, 
                                "memory": MEM,
                                "runtime": RUNTIME,
                                "stdout": log_dir_source,
                                "stderr": log_dir_source
                            }                        
                        )
                        causemap_tasks.append(task_sub)
        
        ## sources without sub-datasets (HCUP)
        else:

            ## find which years for each source
            datadir = config["data_input_dir"][source]
            files = os.listdir(datadir)
            years = list(map(lambda sub:int("".join([ele for ele in sub if ele.isnumeric()])), files))
            years.sort()

            ## otherwise, just parallelize on year
            for yr in years:
                print("     > " + str(yr))
                ## add task to workflow
                task_no_sub = causemap_template.create_task(
                    name = "_".join(["causemap", source, str(yr)]),
                    python = sys.executable,
                    script_path = causemap_script,
                    source = source, 
                    sub_dataset = "none", 
                    year = yr,
                    age = "all",
                    sex = "all",
                    state = "all",
                    PRI = PRI, 
                    MVI = MVI,
                    compute_resources={
                        "cores": CORES,
                        "memory": MEM,
                        "runtime": RUNTIME,
                        "stdout": log_dir_source,
                        "stderr": log_dir_source
                    }                
                )
                causemap_tasks.append(task_no_sub)

    print("Adding tasks to workflow")
    workflow.add_tasks(causemap_tasks)

    print('Binding to database')
    workflow.bind()
    WFID = str(workflow.workflow_id)
    print("URL")

    print("Running workflow")
    wf_run = workflow.run(seconds_until_timeout = 259200)
    if wf_run == "D":
        try:
            update_phase_run(phase_run_id = PRI, map_version_id = MVI) ## if workflow succeeds, update database to show map was applied
        except Exception as e: 
            print(e)
        print("-----------------")
        print("CAUSEMAP applied!")
        print("-----------------")
    else:
        raise RuntimeError("The CAUSEMAP submitter workflow did not complete successfully.")
