# Author(s): Meera Beauchamp, Drew DeJarnatt
import pandas as pd
import itertools
from pathlib import Path
from datetime import datetime
from datetime import date
import getpass
import glob
import os
getpass.getuser()

from dex_us_county.00_data_prep.stage_2.cms.helpers import jobmon_submitter 

def run_cms_submitter():
    yr = [2002, 2004, 2006, 2008, 2010, 2012, 2014, 2016]
    df=['medpar']
    
    command_args = ("outpath",'yr','df')#
    wf_name = f'stg2_medpar_{datetime.now().strftime("%Y-%m-%d_%H%M%S")}'
    # Intializing Jobmon tool and workflow
    wf, ttemp = jobmon_submitter.workflow('s2_medpar', wf_name, command_args)
    a=list(itertools.product(yr,df))
    
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
    task_name = "s2_medpar_"+str(params).replace('(','').replace(')','').replace("'",'').replace(' ',"_")
    script = Path(__file__).resolve().parent / "helpers/mdcr/mdcr_nf_medpar_1.py" #get the parent directory of this file

    today = str(date.today())
    output_filename = 'nf_medpar_stg2_'+ today+'.parquet'
    logpath = Path(FILEPATH) / wf_name / task_name 
    logpath.mkdir(parents=True, exist_ok=True)

    # Creating dictionary of computation parameters
    compute_params = {
        "queue": "long.q",
        "cores": 4,
        "memory": '150G',
        "runtime": "5400s",
        "stdout": logpath.as_posix(),
        "stderr": logpath.as_posix(),
        "project": "proj_dex",
        "constraints": "archive",
    }
    
    # Creating dictionary of task parameters
    outpath = FILEPATH
    task_params = {
        "script": script,
        "outpath": outpath,
        'yr':params[0],
        'df':params[1]
    }
    # create task
    task = jobmon_submitter.task(ttemp, task_name, compute_params, task_params)
    os.system(f"ln -sf {file_path} {best}")
    
    return task



if __name__ == "__main__":
    run_cms_submitter()