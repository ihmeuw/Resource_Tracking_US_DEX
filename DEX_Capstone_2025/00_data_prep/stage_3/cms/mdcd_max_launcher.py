#####################################################################################
##PURPOSE: CMS Medicaid MAX Stage 3 Launcher
##AUTHOR(S): Meera Beauchamp, Drew DeJarnatt
#####################################################################################
import pandas as pd
import itertools
from pathlib import Path
from datetime import datetime
from datetime import date
import getpass
import os
from dex_us_county.00_data_prep.stage_3.cms.helpers import jobmon_submitter 

def run_cms_submitter(): 
    state_2000 =['AL','AK','AR','AZ','CA','CO','CT','DC','DE','FL',
                 'GA','HI','IA','ID','IL','IN','KS','KY','LA','MA',
                 'MD','ME','MI','MN','MO','MS','MT','NC','ND','NE',
                 'NH','NJ','NM','NV','NY','OH','OK','OR','PA','RI',
                 'SC','SD','TN','TX','UT','VA','VT','WA','WI','WV','WY']  #51 states for 2000 
    
    state_2010 =['AK','AL','AR','AZ','CA','CO','CT','DC','DE','FL',
                 'GA','HI','IA','ID','IL','IN','KY','LA','MA',
                 'MD','MI','MN','MO','MS','MT','NC','ND','NE',
                 'NH','NJ','NM','NV','NY','OH','OK','OR','PA','RI',
                 'SC','SD','TN','TX','UT','VA','VT','WA','WI','WV','WY'] # states for 2010 - 49, no KS or ME
    
    state_2014 =['CA','GA','IA','ID','LA','MI','MN','MO','MS','NJ',
                 'PA','SD','TN','UT','VT','WV','WY'] #17 for 2014

    sex=[1,2] 
    subdatasets = ['ot_max','ip_max']

    toc=['IP','ED','OTH','HH','NF','UNK','AM','DV']
    command_args = ("outpath",'state','yr','sex','toc','subdataset')

    wf_name = f'stg3_mdcd_max_{datetime.now().strftime("%Y-%m-%d_%H%M%S")}'

    # Intializing Jobmon tool and workflow
    wf, ttemp = jobmon_submitter.workflow('stg3_mdcd_max', wf_name, command_args)
    #ot
    a=list(itertools.product(state_2000,[2000], sex, toc,['ot_max']))
    b=list(itertools.product(state_2010,[2010], sex, toc,['ot_max']))
    c=list(itertools.product(state_2014,[2014], sex, toc,['ot_max']))
    
    #ip
    d=list(itertools.product(state_2000,[2000], sex, ['IP'],['ip_max']))
    e=list(itertools.product(state_2010,[2010], sex, ['IP'],['ip_max']))
    f=list(itertools.product(state_2014,[2014], sex, ['IP'],['ip_max']))

    a=a+b+c+d+e+f
    print(len(a))
    
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
    task_name = "stg3_max"+str(params).replace('(','').replace(')','').replace("'",'').replace(' ',"_").replace(',','')
    script = Path(__file__).resolve().parent / "helpers/mdcd/mdcd_max_stg3.py" #get the parent directory of this file
    
    today = str(date.today())
    output_filename = 'mdcd_max_'+ today+'.parquet'
    logpath = Path('FILEPATH') / wf_name / task_name 
    logpath.mkdir(parents=True, exist_ok=True)
    
    core_num = 4
    mem = '10'
    state = params[0]
    subdataset = params[4]
    subdataset1 = subdataset[:2]
    
    if subdataset == 'ip_max':
        if state in ['CA','FL','NJ','NY','TX']:
            mem = '50'
        elif state in ['AZ','OH','IL','MI']:
            mem = '30'
        elif state in ['GA','KS','LA','MA','MN','MO','MS','NC','NJ','OK','PA','TN','WA']:
            mem = '25'
    
    elif subdataset == 'ot_max':
        mem = '150'
        if state in ['CA','FL','NJ','NY','TX']:
            core_num = 8
            mem = '500'
        elif state in ['AZ','OH','IL','MI']:
            core_num = 6
            mem = '300'
        elif state in ['GA','LA','MA','MN','MO','MS','NC','NJ','OK','PA','TN','WA']:
            core_num = 5
            mem = '200'

    # Creating dictionary of computation parameters
    compute_params = {
        "queue": "long.q",
        "cores": core_num,
        "memory": mem,
        "runtime": "10000s",
        "stdout": logpath.as_posix(),
        "stderr": logpath.as_posix(),
        "project": "proj_dex",
        "constraints": "archive",
    }
    
    # Creating dictionary of task parameters
    outpath = 'FILEPATH'+subdataset1+'/'+output_filename
    task_params = {
        "cores":compute_params["cores"],
        "script": script,
        "outpath": outpath,
        'state':params[0],
        'yr':params[1],
        'sex':params[2],
        'toc':params[3],
        'subdataset':params[4]
    }
    # create task
    task = jobmon_submitter.task(ttemp, task_name, compute_params, task_params) 
    
    return task

if __name__ == "__main__":
    run_cms_submitter()
