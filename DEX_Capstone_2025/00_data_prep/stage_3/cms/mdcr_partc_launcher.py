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
from dex_us_county.00_data_prep.stage_3.cms.helpers import jobmon_submitter 

getpass.getuser()

def run_cms_submitter():
    test = 0
    state= ['AK','AL','AR','AZ','CA','CO','CT','DC','DE','FL','GA','HI','IA','ID','IL','IN','KS','KY','LA','MA','MD',
           'ME','MI','MN','MO','MS','MT','NC','ND','NE','NH','NJ','NM','NV','NY','OH','OK','OR','PA','RI','SC',
           'SD','TN','TX','UT','VA','VT','WA','WI','WV','WY']
    yr=[2016,2019]
    sex=[1,2] 
    command_args = ("outpath",'state','yr','sex', 'toc', 'subdataset')

    wf_name = f's3_partc{datetime.now().strftime("%Y-%m-%d_%H%M%S")}'

    # Intializing Jobmon tool and workflow
    wf, ttemp = jobmon_submitter.workflow('stg3_partc', wf_name, command_args)
    
    #outpath, state, yr,sex, toc, subdataset
    if test == 0:
        ip=list(itertools.product(state,yr,sex, ['IP'], ['ip_part_c'])) 
        hh=list(itertools.product(state,yr,sex, ['HH'], ['hha_part_c']))
        nf=list(itertools.product(state,yr,sex, ['NF'], ['nf_part_c']))
        car=list(itertools.product(state,yr,sex, ['AM','ED','DV','HH','IP','NF','OTH','UNK'], ['carrier_part_c']))
        hop=list(itertools.product(state,yr,sex, ['AM','ED','DV'], ['hop_part_c']))
        a = ip + hh + nf + car + hop
        print(len(a))
        print('test=0')
        
    elif test == 1:
        ip= list(itertools.product(['ID'],[2016],[1], ['IP'], ['ip_part_c'])) 
        hh= list(itertools.product(['ID'],[2016],[1], ['HH'], ['hha_part_c']))
        nf= list(itertools.product(['ID'],[2016],[1], ['NF'], ['nf_part_c']))
        car=list(itertools.product(['ID'],[2016],[1], ['DV','HH'], ['carrier_part_c']))
        hop=list(itertools.product(['ID'],[2016],[1], ['ED','DV'], ['hop_part_c']))
        a = ip + hh + nf + car + hop
        print(len(a))
        print('test=1')
    
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
    task_name = "mdcrc_"+str(params).replace('(','').replace(')','').replace("'",'').replace(' ',"_").replace(',','')
    script = Path(__file__).resolve().parent / "helpers/mdcr/mdcr_part_c_stg3.py" #get the parent directory of this file

    today = str(date.today())
    output_filename = 'mdcr_partc_'+ today+'.parquet'
    logpath = Path(f"FILEPATH") / wf_name / task_name
    logpath.mkdir(parents=True, exist_ok=True)
    
    core_num = 5
    mem = '20'
    state = params[0]
    subdataset = params[4]
    
    if state in ['CA','FL','NJ','NY','PA','TX','OH','MI']:
        core_num = 6
        mem = '70'
    elif state in ['AZ','IL']:
        core_num = 5
        mem = '40'
    elif state in ['GA','LA','MA','MN','MO','MS','NC','NJ','OK','TN','WA']:
        mem = '30'

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
        "cores":compute_params["cores"],
        "script": script,
        "outpath": 'FILEPATH'+subdataset+'FILEPATH'+output_filename,
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
