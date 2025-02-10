# Author(s): Meera Beauchamp, Drew DeJarnatt
import pandas as pd
import itertools
from pathlib import Path
from datetime import datetime
import getpass
import glob
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
    states_no_ca = [state for state in all_states if state != 'CA']
    states_no_ca_ny = [state for state in states_no_ca if state != 'NY']
    
    mdcr_states = ['1','2','3','4','5','6','7','8','9','10',
                    '11','12','13','14','15','16','17','18','19','20',
                    '21','22','23','24','25','26','27','28','29','30',
                    '31','32','33','34','35','36','37','38','39','41',
                    '42','43','44','45','46','47','49','50','51','52',
                    '53','99' ]
    
    yr_TAF = [2016,2019]
    yr_carrier_hha = [2000,2010,2014,2015,2016,2019]
    yr_resuse = [2000,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2019]
    
    # Partitions ------------#
    partition_dummy = '1'
    
    #Partitions for TAF OT
    length =19
    n = 50
    partition_ot_taf = list(range(1,n*(length+1),n)) #start, stop, step
    #Make string
    partition_ot_taf = list(map(str, partition_ot_taf))
    
    #California requires greater partitions 
    length =99#49
    n = 10#20
    partition_ot_taf_ca = list(range(1,n*(length+1),n)) #start, stop, step
    partition_ot_taf_ca = list(map(str, partition_ot_taf_ca)) #Make string
    
    #Partitions for TAF RX
    partition_rx_taf = ['001']
    partition_rx_taf_ca_ny = ['001','002','003','004','005']
    

    command_args = ('mdcd_mdcr','dataset', 'state', 'yr', 'partition')

    wf_name = f'1mdcd_claim_{datetime.now().strftime("%Y-%m-%d_%H%M%S")}'

    # Intializing Jobmon tool and workflow
    wf, ttemp = jobmon_submitter.workflow('1mdcd_claim_demog', wf_name, command_args)
    
    #MDCD Datasets--------------------------------------------
    #mdcd_datasets = ['ip_max','ot_max','ip_taf','ot_taf','ltc_taf','rx_taf']
    #MAX datasets: 
    a=list(itertools.product(['mdcd'],['ip_max','ot_max'],state_2000,[2000], partition_dummy))
    b=list(itertools.product(['mdcd'],['ip_max','ot_max'],state_2010,[2010], partition_dummy))
    c=list(itertools.product(['mdcd'],['ip_max','ot_max'],state_2014,[2014], partition_dummy))
    #TAF non-OT/RX datasets
    d=list(itertools.product(['mdcd'],['ip_taf','ltc_taf'],all_states,yr_TAF, partition_dummy))
    #OT TAF
    e=list(itertools.product(['mdcd'],['ot_taf'],states_no_ca,yr_TAF, partition_ot_taf))
    f=list(itertools.product(['mdcd'],['ot_taf'],['CA'],yr_TAF, partition_ot_taf_ca))
    #RX TAF
    g=list(itertools.product([['mdcd'],'rx_taf'],states_no_ca_ny,yr_TAF,partition_rx_taf))
    h=list(itertools.product(['mdcd'],['rx_taf'],['CA','NY'],yr_TAF, partition_rx_taf_ca_ny))
    #MDCR Datasets--------------------------------------------
    #['carrier_mdcr', 'hha_mdcr', 'hop_mdcr', 'hosp_mdcr','ip_mdcr', 'nf_mdcr',
    # 'carrier_partc', 'hha_partc', 'hop_partc','ip_partc', 'nf_partc']
    partc = ['carrier_partc', 'hha_partc', 'hop_partc','ip_partc', 'nf_partc']
    reuse = ['hop_mdcr','ip_mdcr']
    i = list(itertools.product(['mdcr'],reuse, mdcr_states, yr_resuse, partition_dummy))
    j = list(itertools.product(['mdcr'],['hosp_mdcr'], mdcr_states, [2000,2010,2014,2015,2016], partition_dummy))
    k = list(itertools.product(['mdcr'],['carrier_mdcr'], mdcr_states, yr_carrier_hha, partition_dummy))
    l = list(itertools.product(['mdcr'],partc, mdcr_states, [2016,2019], partition_dummy))
    
    a=list(itertools.product(['mdcr'],['hop_mdcr'], ['18','46','53'], [2019], partition_dummy))
    b = list(itertools.product(['mdcr'],['ip_mdcr'], ['1','3','5','7','9','10','11','12','13','17','19','24','30','38','39','45','47','51','52','99'], [2019], 
    
    a=a+b+c+d+e+f+g+h+i+j+k+l
        
    # CHIA  
    chia_yrs = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022]
    if chia == 1:
        a = list(itertools.product(['mdcr'],['chia_hha_mdcr'], ['22'], chia_yrs, partition_dummy))
        b = list(itertools.product(['mdcr'],['chia_hosp_mdcr'], ['22'], chia_yrs, partition_dummy))
        c = list(itertools.product(['mdcr'],['chia_carrier_mdcr'], ['22'], chia_yrs, partition_dummy))
        d = list(itertools.product(['mdcr'],['chia_nf_mdcr'], ['22'], chia_yrs, partition_dummy))
        e = list(itertools.product(['mdcr'],['chia_op_mdcr'], ['22'], chia_yrs, partition_dummy))
        f = list(itertools.product(['mdcr'],['chia_ip_mdcr'], ['22'], chia_yrs, partition_dummy))
        
        a = a + b + c + d + e + f

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

    task_name = "f1_demog_claim_" + params[0] + '_' + str(params[1])+ '_' +str(params[2])+ '_' +str(params[3]) 
    script = Path(__file__).resolve().parent / "helpers/claim_demographic_tables/1_unique_mdcd_demographics.py" #get the parent directory of this file
    
    logpath = Path('FILEPATH') / wf_name / task_name
    logpath.mkdir(parents=True, exist_ok=True)

    # Creating dictionary of task parameters
    task_params = {
        "script": script,
        'mdcd_mdcr':params[0],
        'dataset':params[1],
        'state':params[2],
        'yr':params[3],
        'partition':params[4]
    }

    #adjust resources based on dataset and state
    num_cores = 4
    MEM = '50G'
    dataset = params[1]
    state = params[2]
    if dataset in ['ot_max','ip_taf','ltc_taf',
                   'carrier_mdcr', 'hop_mdcr', 'ip_mdcr', 'nf_mdcr',
                  'carrier_partc',  'hop_partc','ip_partc', 'nf_partc']:
        MEM = '100G'
        if state in ['NY','CA']:
            num_cores = 5
    elif dataset in ['ot_taf','rx_taf']:
        MEM = '150G'
        num_cores = 10
        if state in ['NY','CA']:
            MEM = '200G'    
    
    # Creating dictionary of computation parameters
    compute_params = {
        "queue": "all.q",
        "cores": num_cores,
        "memory": MEM,
        "runtime": "3600s",
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
