# Author(s): Meera Beauchamp, Drew DeJarnatt
import pandas as pd
import itertools
from pathlib import Path
from datetime import datetime
from datetime import date
import getpass
import glob
user = getpass.getuser()

from dex_us_county.00_data_prep.stage_2.cms.helpers import jobmon_submitter 

def run_cms_submitter():
    test=0 

    #these are SSA codes - https://resdac.org/cms-data/variables/beneficiary-residence-ssa-state-code-ffs
    state=[1,2,3,4,5,6,7,8,9,10,
             11,12,13,14,15,16,17,18,19,20,
             21,22,23,24,25,26,27,28,29,30,
             31,32,33,34,35,36,37,38,39,40,
             41,42,43,44,45,46,47,48,49,50,
             51,52,53,54,55,56,57,58,59,60,
             61,62,63,64,65,97,98,99,-1] #All possible locations, both US and non-US, 68 locations
    
    yr = [2016, 2019]
    subdataset_no_st = ['ip_part_c','hha_part_c','nf_part_c'] #don't need to parallelize over state
    subdataset_st = ['carrier_part_c','hop_part_c'] #don't need to parallelize over state
    
    if test == 0:
        a=list(itertools.product([1],yr,subdataset_no_st,[test]))
        b=list(itertools.product(state,yr,subdataset_st,[test])) 
        a=a+b
        print(len(a)) 
        print('test=0')
    elif test ==1:
        a=list(itertools.product([1],yr,subdataset_no_st,[test]))
        b=list(itertools.product(state,yr,subdataset_st,[test])) 
        a=a+b
        print(len(a))
        print('test=1')
    
    command_args = ("outpath",'state','yr', 'subdataset','test')

    wf_name = f'stg2_partc_{datetime.now().strftime("%Y-%m-%d_%H%M%S")}'

    # Intializing Jobmon tool and workflow
    wf, ttemp = jobmon_submitter.workflow(f"st2_partc", wf_name, command_args)
    
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
    task_name = "stg2_"+'_'.join((str(i) for i in params))
    script = Path(__file__).resolve().parent / "helpers/mdcr/mdcr_part_c_1.py" #get the parent directory of this file

    test = params[3]
    if test == 1:
        today = str(date.today()) +'test'
    elif test == 0:
        today = str(date.today())
    output_filename = 'mdcr_partc_'+ today+'.parquet'
    
    logpath = Path(FILEPATH) / wf_name / task_name 
    logpath.mkdir(parents=True, exist_ok=True)
    
    #adjust resources based on dataset and state
    num_cores = 4
    mem = '30G'
    state = params[0]
    yr = params[1]
    subdataset = params[2]
    if subdataset == 'carrier_part_c':
        subdataset1='carrier'
    elif subdataset == 'hha_part_c':
        subdataset1='HHA'
    elif subdataset == 'hop_part_c':
        subdataset1='hop'
    elif subdataset == 'ip_part_c':
        subdataset1='IP'
    elif subdataset == 'nf_part_c':
        subdataset1='NF'
    else:
        subdataset1=subdataset

    if subdataset in ['hop_part_c','carrier_part_c']:
        num_cores =4
        mem = '40G'
        if state in [5,33,10,14,12,53, 64,65,-1]:
            num_cores = 9
            mem = '155G'
        if state in [48, 2, 63]:
            num_cores = 10
            mem = '750G'

    compute_params = {
        "queue": "all.q",
        "cores": num_cores,
        "memory": mem, 
        "runtime": "4600s",
        "stdout": logpath.as_posix(),
        "stderr": logpath.as_posix(),
        "project": "proj_dex",
        "constraints": "archive",
    }
    
    # Creating dictionary of task parameters
    if test ==1:
        outpath = FILEPATH
    elif test ==0:
        outpath = FILEPATH
    # Creating dictionary of task parameters
    task_params = {
        "script": script,
        "outpath": outpath,
        'state':params[0], 
        'yr':params[1],
        'subdataset':params[2],
        'test':params[3]
    } 
    # create task
    task = jobmon_submitter.task(ttemp, task_name, compute_params, task_params) 
    return task

if __name__ == "__main__":
    run_cms_submitter()
