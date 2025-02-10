# Author(s): Meera Beauchamp, Drew DeJarnatt
import pandas as pd
import itertools
from pathlib import Path
from datetime import datetime
from datetime import date
import getpass

from dex_us_county.00_data_prep.stage_2.cms.helpers import jobmon_submitter 

def run_cms_submitter():
    state_00=['AK','AL','AR','AZ','CA','CO','CT','DC','DE','FL',
              'GA','HI','IA','ID','IL','IN','KS','KY','LA','MA',
              'MD','ME','MI','MN','MO','MS','MT','NC','ND','NE',
              'NH','NJ','NM','NV','NY','OH','OK','OR','PA','RI',
              'SC','SD','TN','TX','UT','VA','VT','WA','WI','WV','WY'] #51 states
    
    state_10=['AK','AL','AR','AZ','CA','CO','CT','DC','DE','FL',
              'GA','HI','IA','ID','IL','IN','KY','LA','MA','MD',
              'MI','MN','MO','MS','MT','NC','ND','NE','NH','NJ',
              'NM','NV','NY','OH','OK','OR','PA','RI','SC','SD',
              'TN','TX','UT','VA','VT','WA','WI','WV','WY'] #49 states no KS or ME
    
    state_14=['CA','GA','IA','ID','LA','MI','MN','MO','MS','NJ',
              'PA','SD','TN','UT','VT','WV','WY'] #17 states
    subdatasets = ['ip_max','ot_max'] 
    
    a=list(itertools.product(state_00,[2000],subdatasets)) 
    b=list(itertools.product(state_10,[2010],subdatasets))
    c=list(itertools.product(state_14,[2014],subdatasets))
    d=list(itertools.product(['-1'],[2000,2010,2014],['ip_max']))

    a=a+b+c+d
    print(len(a))

    
    command_args = ("outpath",'state','yr','subdataset')

    wf_name = f's2_max_{datetime.now().strftime("%Y-%m-%d_%H%M%S")}'

    # Intializing Jobmon tool and workflow
    wf, ttemp = jobmon_submitter.workflow('s2_max', wf_name, command_args)
    
    
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
    task_name = "stg2_mdcd_"+str(params).replace('(','').replace(')','').replace("'",'').replace(' ',"_")
    script = Path(__file__).resolve().parent / "helpers/mdcd/max_1.py" #get the parent directory of this file
    
    today = str(date.today())
    output_filename = 'mdcd_max_stg2_'+today+'.parquet'
    
    logpath = Path('FILEPATH') / wf_name / task_name
    logpath.mkdir(parents=True, exist_ok=True)
    
    core_num = 4
    mem = '50G'
    state = params[0]
    yr=params[1]
    subdataset = params[2]
    
    if subdataset == 'ot_max':
        mem = '75G'
        if state in ['CA','FL','NJ','NY','TX']:
            core_num = 12
            mem = '540' 
            if (yr == 2014) & (state == 'CA'):
                mem = '575'  
        elif state in ['AZ','OH','IL','MI','PA','NC']:
            mem = '300'
        elif state in ['GA','LA','MA','MN','MO','MS','NJ','OK','TN','WA']:
            mem = '150'

    # Creating dictionary of computation parameters
    compute_params = {
        "queue": "all.q",
        "cores": core_num,
        "memory": mem,
        "runtime": "12600s",
        "stdout": logpath.as_posix(),
        "stderr": logpath.as_posix(),
        "project": "proj_dex",
        "constraints": "archive",
    }
    
    # Creating dictionary of task parameters
    if subdataset == 'ip_max':
        dataset = 'IP'
    elif subdataset =='ot_max':
        dataset ='OT'
    
    task_params = {
        "script": script,
        "outpath": 'FILEPATH'+output_filename,
        'state':params[0],
        'yr':params[1],
        'subdataset':params[2]
    }
    # create task
    task = jobmon_submitter.task(ttemp, task_name, compute_params, task_params) 
    return task

if __name__ == "__main__":
    run_cms_submitter()