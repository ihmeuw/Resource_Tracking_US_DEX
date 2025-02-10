#####################################################################################
##PURPOSE: CMS Medicaid TAF Stage 3 Launcher
##AUTHOR(S): Meera Beauchamp, Drew DeJarnatt
#####################################################################################

import pandas as pd
import itertools
from pathlib import Path
from datetime import datetime
import getpass
from datetime import date
from helpers import jobmon_submitter 

def run_cms_submitter(): 
    test = 0
    chia = 0
    state=['AK','AL','AR','AZ','CA','CO','CT','DC','DE','FL',
           'GA','HI','IA','ID','IL','IN','KS','KY','LA','MA',
           'MD','ME','MI','MN','MO','MS','MT','NC','ND','NE',
           'NV','NH','NJ','NM','NY','OH','OK','OR','PA','RI',
           'SC','SD','TN','TX','UT','VA','VT','WA','WI','WV','WY'] #'-1',
    #hop and carrier are the only datasets parallelized on sex
    sex=['1','2']
    toc_carrier = ['AM','ED','IP','OTH','HH','RX','NF','UNKNOWN','DV']#These are just the carrier tocs
    yrs_carrier_hha_hop = [2000,2010,2014,2015,2016,2019]
    yrs_ip= [2000,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2019]
    yrs_hosp = [2000,2010,2014,2015,2016]
    yrs_rx = [2010,2014,2015,2016,2019]
    
    if test == 0:
        out_path = 'FILEPATH'
        carrier=list(itertools.product(state,yrs_carrier_hha_hop,sex,toc_carrier,['carrier'], ['0']))
        hop    =list(itertools.product(state,yrs_carrier_hha_hop,sex,['AM','ED','DV'],['hop'], ['0']))
        ip     =list(itertools.product(state,yrs_ip,['all'],['IP'],['ip'], ['0']))
        hha    =list(itertools.product(state,yrs_carrier_hha_hop,['all'],['HH'],['hha'], ['0']))
        hosp   =list(itertools.product(state,yrs_hosp,['all'],['NF'],['hosp'], ['0']))
        nf     =list(itertools.product(state,[2019],['all'],['NF'],['nf'], ['0']))
        rx     =list(itertools.product(state,yrs_rx,sex,['RX'],['rx'], ['0']))
        a= carrier + hop + ip + hha + hosp + nf +rx
        print(len(a))
        print('test=0')

    elif test == 1:
        out_path = 'FILEPATH'
        carrier=list(itertools.product(['ME'],[2016],['1'],['HH'],['carrier'], ['0']))
        hop    =list(itertools.product(['ME'],[2019],['1'],['ED'],['hop'], ['0']))
        ip     =list(itertools.product(['ME'],[2016],['all'],['IP'],['ip'], ['0']))
        hha    =list(itertools.product(['ME'],[2016],['all'],['HH'],['hha'], ['0']))
        hosp   =list(itertools.product(['ME'],[2016],['all'],['NF'],['hosp'], ['0']))
        nf     =list(itertools.product(['ME'],[2019],['all'],['NF'],['nf'], ['0']))
        rx     =list(itertools.product(['ME'],[2019],['1'],['RX'],['rx'], ['0']))
        a= carrier + hop + ip + hha + hosp + nf +rx
        print(len(a))
        print('test=1')

    if chia == 1:
        chia_yrs = [2015,2016,2017,2018,2019,2020,2021,2022]
        out_path = 'FILEPATH'
        carrier=list(itertools.product(['MA'],chia_yrs,sex,toc_carrier,['carrier'], ['1']))
        hop    =list(itertools.product(['MA'],chia_yrs,sex,['AM','ED','DV'],['hop'], ['1']))
        ip     =list(itertools.product(['MA'],chia_yrs,['all'],['IP'],['ip'], ['1']))
        hha    =list(itertools.product(['MA'],chia_yrs,['all'],['HH'],['hha'], ['1']))
        hosp   =list(itertools.product(['MA'],chia_yrs,['all'],['NF'],['hosp'], ['1']))
        nf     =list(itertools.product(['MA'],chia_yrs,['all'],['NF'],['nf'], ['1']))
        a = carrier+hop+ip+hha+hosp+nf
        print(len(a))

    
    command_args = ("outpath",'state','yr','sex','toc','subdataset', 'chia')

    wf_name = f'stg3_mdcr_{datetime.now().strftime("%Y-%m-%d_%H%M%S")}'

    # Intializing Jobmon tool and workflow
    print(out_path)
    wf, ttemp = jobmon_submitter.workflow('s3_mdcr', wf_name, command_args)
    
    for params in a:
        # add task to workflow
        task = create_task(ttemp, wf.name, params, out_path, chia)
        wf.add_task(task)


    # Running the workflow
    wf_run = wf.run()

    if wf_run != "D":
        raise RuntimeError("The submitter workflow did not complete successfully.")
        
def create_task(ttemp,wf_name, params, out_path, chia):
    
    # Assinging task-specific parameters
    task_name = "stg3_"+str(params).replace('(','').replace(')','').replace("'",'').replace(' ',"_").replace(',','')
    script = Path(__file__).resolve().parent / "helpers/mdcr/mdcr_ffs_stg3.py" #get the parent directory of this file
    
    logpath = Path(f"FILEPATH{getpass.getuser()}FILEPATH") / wf_name / task_name 
    logpath.mkdir(parents=True, exist_ok=True)
    
    core_num = 4
    mem = '20'
    state = params[0]
    subdataset=params[4]
    toc =  params[3]
    
    if subdataset in ['hop','carrier']:
        mem='30'
    if state in ['CA','FL','NJ','NY','TX']:
        core_num = 6
        mem = '50'
    elif state in ['AZ','OH']:
        core_num = 5
        mem = '40'
    elif state in ['GA','IL','LA','MA','MI','MN','MO','MS','NC','NJ','OK','PA','TN','WA']:
        mem = '35'
    if toc =='AM':
        mem = '90' 
        if state in ['CA','FL','NJ','NY','TX']:
            core_num = 6
            mem = '300'
        elif state in ['AZ','IL','MA','OH','PA','MI']:
            core_num = 5
            mem = '150'#150#80
        elif state in ['GA','LA','MI','MN','MO','MS','NC','NJ','OK','TN','WA']:
            mem = '125'
    
    if subdataset == 'rx':
        mem = '150'
        num_core = 5

    # Creating dictionary of computation parameters
    compute_params = {
        "queue": "long.q",
        "cores": core_num,
        "memory": mem,
        "runtime": "7000s",
        "stdout": logpath.as_posix(),
        "stderr": logpath.as_posix(),
        "project": "proj_dex",
        "constraints": "archive",
    }
    
    # Creating dictionary of task parameters
    task_params = {
        "cores":compute_params["cores"],
        "script": script,
        "outpath": out_path+subdataset+'/best/'+output_filename,
        'state':params[0],
        'yr':params[1],
        'sex':params[2],
        'toc': params[3],
        'subdataset':params[4],
        'chia':params[5]
    }
    # create task
    task = jobmon_submitter.task(ttemp, task_name, compute_params, task_params) #copy and put jobmon in helpers
    return task

if __name__ == "__main__":
    run_cms_submitter()
