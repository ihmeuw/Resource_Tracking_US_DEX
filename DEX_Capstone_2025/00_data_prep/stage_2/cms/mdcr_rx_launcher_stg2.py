# Author(s): Meera Beauchamp, Drew DeJarnatt
import pandas as pd
import itertools
from pathlib import Path
from datetime import datetime
from datetime import date
import getpass
import glob
getpass.getuser()

from helpers import jobmon_submitter 
from constants import paths as path

def run_cms_submitter():
    test=0 
    path_10 = FILEPATH
    files_10 = glob.glob(path_10)
    
    path_14 = FILEPATH
    files_14 = glob.glob(path_14)

    path_15 = FILEPATH
    files_15 =  glob.glob(path_15)
    
    path_16 = FILEPATH
    files_16 = glob.glob(path_16)
    
    path_19 = FILEPATH 
    files_19 = glob.glob(path_19)

    command_args = ("outpath",'yr','file')

    wf_name = f'dex_cms_stg2_mdcr_rx_{datetime.now().strftime("%Y-%m-%d_%H%M%S")}'

    # Intializing Jobmon tool and workflow
    wf, ttemp = jobmon_submitter.workflow('s2_rx_mdcr', wf_name, command_args)

    if test ==1:
        a=list(itertools.product([2010], [FILEPATH])) 
        print(len(a))
        print('test = 1')
        
    elif test ==0: 
        a=list(itertools.product([2010], files_10))
        b=list(itertools.product([2014], files_14))
        c=list(itertools.product([2015], files_15))
        d=list(itertools.product([2016], files_16))
        e=list(itertools.product([2019], files_19))
        a=a+b+c+d+e 
        print(len(a))
        print('test = 0')
    
    
    for params in a:
        
        # add task to workflow
        task = create_task(ttemp, wf.name, params)
        wf.add_task(task)


    # Running the workflow
    wf_run = wf.run()

    if wf_run != "D":
        raise RuntimeError("The submitter workflow did not complete successfully.")
        
def create_task(ttemp,wf_name, params):
    
    # Assinging task-specific parameters
    task_name = "stg2_mdcr_rx"
    script = Path(__file__).resolve().parent / "helpers/mdcr/mdcr_rx_1.py" #get the parent directory of this file

    today = str(date.today())
    output_filename = FILEPATH
    # Basing output path on dataset of interest
    outpath = FILEPATH
    logpath = Path(FILEPATH) / wf_name / task_name 
    logpath.mkdir(parents=True, exist_ok=True)

    # Creating dictionary of computation parameters
    compute_params = {
        "queue": "long.q",
        "cores": 4,
        "memory": '100G',#150
        "runtime": "5400s",
        "stdout": logpath.as_posix(),
        "stderr": logpath.as_posix(),
        "project": "proj_dex",
        "constraints": "archive",
    }
    
    # Creating dictionary of task parameters
    task_params = {
        "script": script,
        "outpath": outpath,
        'yr':params[0],
        'file': params[1]
    }
    # create task
    task = jobmon_submitter.task(ttemp, task_name, compute_params, task_params) 
    return task

if __name__ == "__main__":
    run_cms_submitter()
