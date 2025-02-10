# Author(s): Meera Beauchamp, Drew DeJarnatt
import pandas as pd
import itertools
from pathlib import Path
from datetime import datetime
from datetime import date
import getpass
import os
import glob
getpass.getuser()

from dex_us_county.00_data_prep.stage_2.cms.helpers import jobmon_submitter 

def run_cms_submitter():
    test=0 # 0 or 1
    state= ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 
            'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA',
            'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE',
            'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI',
            'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY','-1'] 
    state1 = state.copy()
    state1.remove('CA')
    state_rx = state.copy()
    state_rx.remove('-1')

    yr= [2016,2019]
    subdatasets = ['ip_taf','ltc_taf']
    part_rx = ['001','002','003','004','005']

    length =19
    n = 50
    part_ot = list(range(1,n*(length+1),n)) #start, stop, step
    
    #California requires greater partitions 
    length =99#49
    n = 10#20
    part_ot_ca = list(range(1,n*(length+1),n)) #start, stop, step
    
    #To make compatible with part_rx which is string
    part_ot = list(map(str, part_ot)) # makes every element in the list a string
    part_ot_ca = list(map(str, part_ot_ca)) # makes every element in the list a string
    
    if test == 0:
        ip_ltc =list(itertools.product(state,[2016,2019],subdatasets,['000']))
        rx = list(itertools.product(state_rx,[2016,2019],['rx_taf'],part_rx))
        ot = list(itertools.product(state1,[2016,2019],['ot_taf'],part_ot))
        ot_ca = list(itertools.product(['CA'],[2016,2019],['ot_taf'],part_ot_ca))
        
        
        l = list(itertools.product(['TN'],[2016],['ot_taf'], ['201']))
        s = list(itertools.product(['-1'],[2016],['ot_taf'], ['751']))
        w = list(itertools.product(['-1'],[2019],['ot_taf'], ['301']))
        z = list(itertools.product(['-1'],[2019],['ot_taf'], ['701']))

        
        a=ot+ot_ca+ip_ltc+rx
        print('test = 0')
        print(len(a))

    elif test ==1:
        ip_ltc =list(itertools.product(['ME'],[2016],subdatasets,['000']))
        rx = list(itertools.product(['ME'],[2016],['rx_taf'],['001']))
        ot = list(itertools.product(['UT'],[2016],['ot_taf'],['251']))
        a=ip_ltc+rx +ot
        print('test = 1')
        print(len(a))
    
    command_args = ("outpath",'state','yr','subdataset','part')

    wf_name = f'stg2_taf_{datetime.now().strftime("%Y-%m-%d_%H%M%S")}'

    # Intializing Jobmon tool and workflow
    wf, ttemp = jobmon_submitter.workflow('s2_taf', wf_name, command_args)
    
    
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
    task_name = "stg2_taf_"+str(params).replace('(','').replace(')','').replace("'",'').replace(' ',"_")
    script = Path(__file__).resolve().parent / "helpers/mdcd/taf_1.py" #get the parent directory of this file

    today = str(date.today())
    output_filename = 'mdcd_taf_'+ today+'.parquet'
    
    logpath = Path('FILEPATH') / wf_name / task_name 
    logpath.mkdir(parents=True, exist_ok=True)
    
    #subdatasets and states are drastically different sizes, so update resource request based on this
    core_num = 4
    mem = '5'#75
    state = params[0]
    subdataset=params[2]
    if subdataset == 'ip_taf':
        dataset = 'IP'
    elif subdataset =='ltc_taf':
        dataset = 'LTC'
    elif subdataset =='rx_taf':
        dataset = 'RX'
    elif subdataset =='ot_taf':
        dataset = 'OT'
    
    if state in ['CA','FL','NJ','NY','TX']:
        core_num = 5
        mem = '75'
    elif state in ['CO','GA','MA','MI','MN','MO','MS','NC','NJ','PA','TN','WA']:
        mem = '25'
        
    if subdataset == 'ot_taf':
        core_num = 5
        mem = '135'

        if state in ['CA','FL','NJ','NY','TX']:
            core_num = 7
            mem = '300'
        elif state in ['AZ','OH','MS']:
            core_num = 6
            mem = '200'
        elif state in ['GA','IL','LA','MA','MI','MN','MO','NC','NJ','OK','PA','TN','WA']:
            mem = '170'
    
    if subdataset == 'rx_taf':
        core_num = 5
        mem = '200'
        if state in ['CA','FL','NJ','NY','TX']:
            core_num = 7
            mem = '400'
        elif state in ['GA','MI','MN','MO','MS','NC','NJ','PA','TN','WA']:
            mem = '225'
    
    # Creating dictionary of computation parameters
    compute_params = {
        "queue": "all.q",
        "cores": core_num,
        "memory": mem,
        "runtime": "5400s",
        "stdout": logpath.as_posix(),
        "stderr": logpath.as_posix(),
        "project": "proj_dex",
        "constraints": "archive",
    }

    
    # Creating dictionary of task parameters
    task_params = {
        "script": script,
        "outpath": FILEPATH,
        'state':params[0],
        'yr':params[1],
        'subdataset':params[2],
        'part':params[3]
    }
    # create task
    task = jobmon_submitter.task(ttemp, task_name, compute_params, task_params) 
    return task

if __name__ == "__main__":
    run_cms_submitter()