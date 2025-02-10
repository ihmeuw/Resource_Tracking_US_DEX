#####################################################################################
##PURPOSE: CMS Medicaid TAF Stage 3 Launcher
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
    test = 0 #0 or 1
    states =['AL','AK','AR','AZ','CA','CO','CT','DC','DE','FL',
             'GA','HI','IA','ID','IL','IN','KS','KY','LA','MA',
             'MD','ME','MI','MN','MO','MS','MT','NC','ND','NE',
             'NH','NJ','NM','NV','NY','OH','OK','OR','PA','RI',
             'SC','SD','TN','TX','UT','VA','VT','WA','WI','WV','WY']  #51 states 
    yrs = [2016,2019]
    sex=[2,1] #
    subdatasets = ['ip_taf','ltc_taf','ot_taf','rx_taf']

    toc=['IP','ED','OTH','HH','NF','UNK','AM','DV']
    partition = [1,2,3,4,5]#only needed for OT and RX
    command_args = ("outpath",'state','yr','sex','toc','subdataset','partition')

    wf_name = f'stg3_mdcd_taf_{datetime.now().strftime("%Y-%m-%d_%H%M%S")}'

    # Intializing Jobmon tool and workflow
    wf, ttemp = jobmon_submitter.workflow('stg3_mdcd_taf', wf_name, command_args)
    if test ==0:
        a=list(itertools.product(states,yrs, sex, ['IP'],['ip_taf'],[1])) 
        b=list(itertools.product(states,yrs, sex, ['NF'],['ltc_taf'],[1]))
        c=list(itertools.product(states,yrs, sex, toc,['ot_taf'],partition))
        d=list(itertools.product(states,yrs, sex, ['RX'],['rx_taf'],partition))
        print('test = 0')

    elif test == 1:
        a=list(itertools.product(['ME'],[2016],[1], ['IP'],['ip_taf'],[1])) 
        b=list(itertools.product(['ME'],[2016],[1], ['NF'],['ltc_taf'],[1]))
        c=list(itertools.product(['ME'],[2016],[1], ['NF','DV'],['ot_taf'],partition))
        d=list(itertools.product(['ME'],[2016],[1], ['RX'],['rx_taf'],partition))
        print('test = 1')

    a=a+b+c+d
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
    task_name = "stg3_taf"+str(params).replace('(','').replace(')','').replace("'",'').replace(' ',"_").replace(',','')
    script = Path(__file__).resolve().parent / "helpers/mdcd/mdcd_taf_stg3.py" #get the parent directory of this file
    
    today = str(date.today())
    output_filename = 'mdcd_taf_'+ today+'.parquet'
    logpath = Path(f"FILEPATH/{getpass.getuser()}/FILEPATH") / wf_name / task_name 
    logpath.mkdir(parents=True, exist_ok=True)
    
    core_num = 4
    mem = '30' 
    state = params[0]
    subdataset = params[4]
    toc=params[3]
    if subdataset == 'ltc_taf':
        subdataset1 = 'nf_taf'
    else:
        subdataset1 = subdataset
    

    if state in ['CA','FL','NJ','NY','TX']:
        mem = '150'
        if toc == 'AM':
            mem = '100'
    elif state in ['AZ','CO','OH','IL','MI']:
        mem = '80'
    elif state in ['GA','LA','MA','MN','MO','MS','NC','NJ','OK','PA','TN','WA']:
        mem = '40'

    # Creating dictionary of computation parameters
    compute_params = {
        "queue": "all.q",
        "cores": core_num,
        "memory": mem,
        "runtime": "14000s",
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
        'subdataset':params[4],
        'partition':params[5]
    }
    # create task
    task = jobmon_submitter.task(ttemp, task_name, compute_params, task_params)

    return task

if __name__ == "__main__":
    run_cms_submitter()
