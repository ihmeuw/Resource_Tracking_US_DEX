# Author(s): Meera Beauchamp, Drew DeJarnatt
import pandas as pd
import itertools
from pathlib import Path
from datetime import datetime
from datetime import date
import getpass
import glob
getpass.getuser()

from dex_us_county.00_data_prep.stage_2.cms.helpers import jobmon_submitter 

def run_cms_submitter():
    test=0
    chia = 0
    
    states= [1,2,3,4,5,6,7,8,9,10,
             11,12,13,14,15,16,17,18,19,20,
             21,22,23,24,25,26,27,28,29,30,
             31,32,33,34,35,36,37,38,39,41,
             42,43,44,45,46,47,49,50,51,52,53,
             67,68,69,70,71,72,73,74,80,99,-1] 
    states1 = states.copy()
    states1.remove(-1)
    yrs_carrier_hha_hop = [2000,2010,2014,2015,2016,2019]
    yrs_ip= [2000,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2019]
    yrs_hosp = [2000,2010,2014,2015,2016]
    subdatasets = ['carrier','hop']
    
    sex = ['0','1','2','-1']
    
    if test==1:
        carrier=list(itertools.product([13],[2010],['carrier'],['all'],[test],[chia]))
        hop  =  list(itertools.product([13],[2010],['hop'],['all'],[test],[chia]))
        ip   =  list(itertools.product([13],[2010],['ip'],['all'],[test],[chia]))
        hha  =  list(itertools.product([13],[2010],['hha'],['all'],[test],[chia]))
        hosp =  list(itertools.product([13],[2010],['hosp'],['all'],[test],[chia]))
        nf   =  list(itertools.product([13],[2019],['nf'],['all'],[test],[chia]))
        a=carrier+hop+ip+hha+hosp+nf
        print(len(a))
        print('test=1')
    elif test==0:
        carrier=list(itertools.product(states,yrs_carrier_hha_hop,['carrier'],['all'],[test],[chia]))
        hop=    list(itertools.product(states,[2000,2010,2014,2016],['hop'],['all'],[test],[chia]))
        hop1=   list(itertools.product(states,[2015,2019],['hop'],sex,[test],[chia]))
        ip   =  list(itertools.product(states,yrs_ip,['ip'],['all'],[test],[chia]))
        hha  =  list(itertools.product(states,yrs_carrier_hha_hop,['hha'],['all'],[test],[chia]))
        hosp =  list(itertools.product(states,yrs_hosp,['hosp'],['all'],[test],[chia]))
        nf   =  list(itertools.product(states,[2019],['nf'],['all'],[test],[chia]))
        a= carrier+hop+hop1+ip+hha+hosp+nf
        print(len(a)) 
        print('test=0')
    
    
    if chia == 1:
        states = [22]
        yrs = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022]
        carrier = list(itertools.product(states,yrs,['carrier'],['all'],[test],[chia]))
        hop = list(itertools.product(states,yrs,['hop'],['all'],[test],[chia]))
        ip = list(itertools.product(states,yrs,['ip'],['all'],[test],[chia]))
        hha = list(itertools.product(states,yrs,['hha'],['all'],[test],[chia]))
        hosp = list(itertools.product(states,yrs,['hosp'],['all'],[test],[chia]))
        nf = list(itertools.product(states,yrs,['nf'],['all'],[test],[chia]))
        a = carrier + hop + ip + hha + hosp + nf 
        print(len(a))
        print("chia")
    
    command_args = ("outpath",'state','yr','subdataset','sex','test', 'chia')

    wf_name = f'stg2_mdcr_{datetime.now().strftime("%Y-%m-%d_%H%M%S")}'

    # Intializing Jobmon tool and workflow
    wf, ttemp = jobmon_submitter.workflow('stg2_mdcr', wf_name, command_args)
    
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
    task_name = "stg2_mdcr_" + str(params).replace('(','').replace(')','').replace("'",'').replace(' ',"_")
    script = Path(__file__).resolve().parent / "helpers/mdcr/mdcr_1.py" #get the parent directory of this file
    
    test = params[4]
    chia = params[5]
    # race_imputation = params[6]
    if test == 1:
        today = str(date.today()) +'_test'
    elif test == 0:
        today = str(date.today())
    output_filename = 'mdcr_'+ today+'.parquet'
    if chia == 1:
        output_filename = "chia_" + output_filename
    # Basing output path on dataset of interest
    logpath = Path('FILEPATH') / wf_name / task_name
    logpath.mkdir(parents=True, exist_ok=True)

    #adjust resources based on dataset and state
    num_cores = 6
    mem = '5G'
    state = params[0]
    yr = params[1]
    subdataset = params[2]
    
    
    if subdataset in ['carrier','hop']:
        subdataset1 = subdataset
    elif subdataset == 'ip':
        subdataset1 = 'IP'
    elif subdataset == 'nf':
        subdataset1 ='NF'
    elif subdataset == 'hosp':
        subdataset1 = 'hospice'
    elif subdataset == 'hha':
        subdataset1 = 'HHA'
        
    if subdataset in ['hop','carrier']:
        mem = '75G'
        if (yr == 2008)&(subdataset == 'hop'):
            mem = '350G'
            num_cores = 6
    
    if subdataset in ['ip']:
        mem = '50G'
        if (state==-1)&(yr==2008):
            mem = '350G'
            num_cores = 6
        if state in ['17','50','44']: 
            num_cores = 5
            mem = '100G'
        if yr == 2019:
            mem = '200G'
            num_cores = 6
            if state in ['17','50','44']: 
                num_cores = 8
                mem = '350G'
        if state in ['5','10','33','39','45']: 
            num_cores = 20
            mem = '800G'
    
    if (subdataset != 'ip') & (state in [5,33,10,45]):
        num_cores = 5
        mem = '25G'
    
    if subdataset == 'hop':
        if state in [5,33,10,14,12,45,39,23,-1]:
            num_cores = 7
            mem = '150G'
            if (state == -1) & (yr == 2008):
                mem = '250G'
                num_cores = 9
        if (yr in [2015,2019]) :
            num_cores =6
            mem = '300G'
            if state in [5,33,10,14,12,45,39,23,-1]:
                num_cores = 7
                mem = '450G'
        
    
    # Creating dictionary of computation parameters
    compute_params = {
        "queue": "all.q",
        "cores": num_cores,
        "memory": mem,
        "runtime": "7200",
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
    if chia == 1:
        outpath = FILEPATH  
    
    task_params = {
        "script": script,
        "outpath": outpath,
        'state':params[0],
        'yr':params[1],
        'subdataset':params[2],
        'sex':params[3],
        'test':params[4],
        'chia':params[5]]
    }
    # create task
    task = jobmon_submitter.task(ttemp, task_name, compute_params, task_params) 
    return task

if __name__ == "__main__":
    run_cms_submitter()
