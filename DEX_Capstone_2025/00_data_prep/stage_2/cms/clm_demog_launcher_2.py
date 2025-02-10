# Author(s): Meera Beauchamp, Drew DeJarnatt
import pandas as pd
import itertools
from pathlib import Path
from datetime import datetime
import getpass
getpass.getuser()
from helpers import jobmon_submitter 


def run_cms_data_formatter_submitter():
    chia = 1
    
    #RUN CA and NY separetly parallelizing on sex as well
    state_2000 = ['AK','AL','AR','AZ','CA','CO','CT','DC','DE','FL','GA','HI','IA','ID','IL','IN','KS','KY',
                 'LA','MA','MD','ME','MI','MN','MO','MS','MT','NC','ND','NE','NH','NJ','NM','NV','NY','OH',
                  'OK','OR','PA','RI','SC','SD','TN','TX','UT','VA','VT','WA','WI','WV','WY'] 
    state_2010 =['AK','AL','AR','AZ','CA','CO','CT','DC','DE','FL','GA','HI','IA','ID','IL','IN','KY',
                  'LA','MA','MD','MI','MN','MO','MS','MT','NC','ND','NE','NH','NJ','NM','NV','NY','OH','OK','OR',
                   'PA','RI','SC','SD','TN','TX','UT','VA','VT','WA','WI','WV','WY'] 
    state_2014 = ['CA','GA','IA','ID','LA','MI','MN','MO','MS','NJ','PA','SD','TN','UT','VT','WV','WY'] 
    
    all_states = ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA',
            'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR',
            'PA','RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY'] 
    mdcr_states = ['1','2','3','4','5','6','7','8','9','10',
                    '11','12','13','14','15','16','17','18','19','20',
                    '21','22','23','24','25','26','27','28','29','30',
                    '31','32','33','34','35','36','37','38','39','41',
                    '42','43','44','45','46','47','49','50','51','52',
                    '53','99' ]
    mdcr_yrs = [2000,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2019]

    command_args = ('state', 'yr','mdcd_mdcr', 'chia')

    wf_name = f'2cms_claim_{datetime.now().strftime("%Y-%m-%d_%H%M%S")}'

    # Intializing Jobmon tool and workflow
    wf, ttemp = jobmon_submitter.workflow('f2_cms_claim_demog', wf_name, command_args)
    
    #MAX datasets: 
    a=list(itertools.product(state_2000,[2000],['mdcd'], [chia]))
    b=list(itertools.product(state_2010,[2010],['mdcd'], [chia]))
    c=list(itertools.product(state_2014,[2014],['mdcd'], [chia]))
    #TAF 
    d=list(itertools.product(all_states,[2016,2019],['mdcd'], [chia]))#all_states
    #MDCR
    e=list(itertools.product(mdcr_states,mdcr_yrs,['mdcr'], [chia]))

    a=e#a+b+c+d
    
    # CHIA
    if chia == 1:
        a = list(itertools.product(['22'],[2022],['mdcr'], [chia]))
        
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

    task_name = "f2_cms_claim_" + params[0] + '_' + str(params[1])
    script = Path(__file__).resolve().parent / "helpers/claim_demographic_tables/2_final_unique_demographics.py" #get the parent directory of this file

    logpath = Path('FILEPATH') / wf_name / task_name
    logpath.mkdir(parents=True, exist_ok=True)

    # Creating dictionary of task parameters
    task_params = {
        "script": script,
        'state':params[0],
        'yr':params[1],
        'mdcd_mdcr':params[2],
        'chia':params[3]
    }
    
    #adjust resources based on dataset and state
    num_cores = 6
    MEM = '75G'
    state = params[0]
    if state in ['NY','CA']:
        MEM = '200G'
        num_cores = 10
    
    # Creating dictionary of computation parameters
    compute_params = {
        "queue": "all.q",
        "cores": num_cores,
        "memory": MEM,
        "runtime": "900s",,
        "stdout": logpath.as_posix(),
        "stderr": logpath.as_posix(),
        "project": "proj_dex",
        "constraints": "archive",
    }

    # create task
    task = jobmon_submitter.task(ttemp, task_name, compute_params, task_params)
    return task

if __name__ == "__main__":
    run_cms_data_formatter_submitter()