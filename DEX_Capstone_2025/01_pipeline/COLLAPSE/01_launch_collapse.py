## ==================================================
## Author(s): Sawyer Crosby, Meera Beauchamp
## Date: Jan 31, 2025
## Purpose: Launches COLLAPSE step in parallel on a SLURM cluster
## ==================================================

## Although this code includes functionality to collapse by race this functionality was not used.
## For all occurences, the following setting were used:
## ...
race_option = 'no_re' ## (no race/ethincity information was used)
race_col = 'no_re' ## (no race/ethincity information was used)
no_race_source = ['MSCAN', 'HCCI'] ## not used
## ...

throttle = 10000 ## number of jobs to run at once

if race_option == 'no_re' and race_col != 'NA':
    raise ValueError("For 'no_re', race_col should be 'NA'")

TEST_RUN = 0 ## set to 1 if you want to test a run without overwriting data from that run; otherwise set to 0
TEST_LABEL = 'NA' ## MUST BE PURELY ALPHABETICAL (no numbers/symbols) --- this is what the test will be called

## importing modules
import time
import sys
import os
import pandas as pd
from attr import attr
import argparse
import getpass
import shutil
import uuid
from pathlib import Path
from datetime import datetime
from jobmon.client.api import Tool
import re
from dexdbload.pipeline_runs import (
    get_phase_run_id, 
    get_config, 
    parsed_config
)

## set umask so files will be also writable for others
os.umask(0o002)

## Get run_id
PRIs = get_phase_run_id() ## get phase run ids
PRI = int(PRIs.loc[(PRIs['status'].isin(['Best', 'Active'])), 'phase_run_id'].max()) ## defaults to latest, set manually as needed
input_PRI = PRI
if TEST_RUN:
    PRI = str(PRI) +  '.' + TEST_LABEL

print('Setting up config')
raw_config = get_config()
config = parsed_config(config = raw_config, key = 'COLLAPSE', run_id = PRI)
sdenom_config = parsed_config(config = raw_config, key = 'SAMPLE_DENOM', run_id = input_PRI)
metadata = raw_config['METADATA']

print('Defining list of causes')
RES_toc_cause = pd.read_csv(metadata['toc_cause_restrictions_path'])
RES_toc_cause = RES_toc_cause.loc[(RES_toc_cause['include'] == 1) & (RES_toc_cause['gc_nec'] == 0), ['toc', 'acause']].reset_index(drop = True)

print('Location scripts and directories')
user = getpass.getuser()
collapse_script = os.path.dirname(os.path.abspath(__file__)) + '/collapse.py'
root_dir = config['collapse_output_dir']
data_dirs = [
    root_dir + 'tmp'
]
diagnostic_dirs = [
    root_dir + 'missing_spend',
    root_dir + 'missing_denom',
    root_dir + 'combos_pre_collapse',
    root_dir + 'combos_post_collapse'
]
slurm_e_dir = "FILEPATH"
slurm_o_dir = "FILEPATH"
jobmon_e_dir = "FILEPATH"
jobmon_o_dir = "FILEPATH"
log_dirs = [slurm_e_dir, slurm_o_dir, jobmon_e_dir, jobmon_o_dir]

# Loop through the directories
for dir_path in log_dirs:
    # Check if the directory does not exist
    if not os.path.exists(dir_path):
        # Create the directory including any necessary parent directories
        os.makedirs(dir_path)
        print(f"Created directory: {dir_path}")
    else:
        print(f"Directory already exists: {dir_path}")
        
if race_option == 're':
    data_dirs = [dir + '_race' for dir in data_dirs]
    diagnostic_dirs = [dir + '_race' for dir in diagnostic_dirs]
elif race_option == 'both':
    data_dirs = data_dirs + [dir + '_race' for dir in data_dirs]
    diagnostic_dirs = diagnostic_dirs + [dir + '_race' for dir in diagnostic_dirs]

if __name__ == '__main__':

    print('Parsing arguments')
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--sources', nargs='+', type=str, required=True, default = 'ALL', choices=['ALL', 'MDCR', 'MDCD', 'MSCAN', 'KYTHERA', 'NIS', 'NEDS', 'SIDS', 'SEDD', 'CHIA_MDCR', 'MEPS',  'HCCI'], help='Source')
    parser.add_argument('-w', '--wipe', type=int, required=False, default = 1, choices=[1, 0], help='Clear old outputs? (1 = yes / 0 = no)')
    parser.add_argument('-l', '--launch', type=int, required=False, default = 1, choices=[1, 0], help='Actually launch jobs? (1 = yes / 0 = no) -- set to 0 if you only want to wipe old data.')
    args = vars(parser.parse_args())

    sources = args['sources']
    if 'ALL' in sources:
        assert len(sources) == 1
    if sources == ['ALL']:
        sources =  ['MDCR', 'MDCD', 'MSCAN', 'KYTHERA', 'NIS', 'NEDS', 'SIDS', 'SEDD', 'CHIA_MDCR', 'MEPS',  'HCCI']
        if race_option == "re":
            sources = [x for x in sources if x not in no_race_source]  
    wipe = args['wipe']
    launch = args['launch']

    if race_option == 're':
        sources = [x for x in sources if x not in [no_race_source]]

    ## !!! CONFIRM SOURCES ARE CORRECT
    check = ''.join([
        '---------------------------------',
        '\nConfirm arguments:',
        '\n    > run_id: %s' % (str(PRI)), 
        '\n    > race_option: %s' % (race_option), 
        '\n    > race_col: %s' % (race_col), 
        '\n    > sources: %s' % (', '.join(sources)), 
        '\n    > wiping old data: %s' % (str(bool(wipe))),
        '\n    > launching jobs: %s' % (str(bool(launch))), 
        '\n[yes/no]: '
    ])
    user_input = input(check)
    print('---------------------------------')
    if user_input != 'yes':
        raise ValueError('Must confirm with "yes" to run')
    ## ----------------------------------------
    
    ## make sure sample denom exists
    for i in ["KYTHERA", "SIDS", "SEDD", "NIS", "NEDS"]:
        if i in sources:
            if race_option in ['no_re', 'both']: assert os.path.exists(sdenom_config['data_output_dir'][i])
            if race_option in ['re', 'both'] and i not in no_race_source: 
                assert os.path.exists(sdenom_config['race_data_output_dir'][i])

    if wipe:
        print('Clearing old data and logs')
        ## root dirs
        Path(root_dir).mkdir(exist_ok=True,parents=True)
        ## logs (only remove source specific logs)
        print('  > logs')
        for s in sources:
            for l in log_dirs:
                if os.path.exists(Path(l)):
                    os.system('find ' + l + ' -type f -name *' + s + '* -delete')
                Path(l).mkdir(exist_ok=True, parents=True)

        print('  > data')
        # (only remove source specific files)
        for s in sources:
            print('     - ' + s)
            for d in data_dirs:
                if os.path.exists(Path(d)):
                    os.system('find ' + d + ' -type d -name *dataset=' + s + '* -exec rm -rf {} +')
                    os.system('find ' + d + ' -type d -empty -delete')
            for d in diagnostic_dirs:
                if os.path.exists(Path(d)):
                    os.system('find ' + d + ' -maxdepth 1 -type d -name *dataset=' + s + '* -exec rm -rf {} +')
                    os.system('find ' + d + ' -type d -empty -delete')

    ## If launching jobs (not just deleting files)
    if launch:
        ## ensure main paths exist if needed
        print('Making root folders')
        if not os.path.exists(config['collapse_output_dir']):
            os.mkdir(config['collapse_output_dir'])
        if not os.path.exists(config['pipeline_output_dir']):
            os.mkdir(config['pipeline_output_dir'])

        print('Defining workflow and executor')
        tool = Tool(name='COLLAPSE')
        wf_uuid = uuid.uuid4()
        workflow = tool.create_workflow(
            name=f'collapse{wf_uuid}', 
            max_concurrently_running=throttle
        )

        print('Defining template')
        collapse_template = tool.get_task_template(
            default_compute_resources={
                'queue': 'long.q',
                'runtime': '4h',
                'project': 'proj_dex',
            },
            template_name='collapse',
            default_cluster_name='slurm',
            command_template='{python} {script_path} --source {source} --toc {toc} --year {year} --acause {acause} --PRI {PRI} --race_option {race_option} --race_col {race_col} --no_race_source {no_race_source}',
            node_args=['source', 'toc', 'year', 'acause', 'PRI', 'race_option', 'race_col', 'no_race_source'],
            op_args=['python', 'script_path'],
        )

        print('Creating collapse tasks')
        collapse_task_ids = [] ## keep list of collapse task IDs
        for source in sources:
            print(source)

            tocs = metadata['tocs'][source] 

            ## have to parallelize by toc, year, and cause for all of these due to 
            ## logic in encounters_per_person where encounters = 0 and denom > 0
            for t in tocs:
                print(' - ' + t)

                if t == 'RX': 
                    MEM = '40G'
                elif source == 'CHIA_MDCR': 
                    MEM = '10G'            
                elif t == "NF" and source == "MDCR": 
                    MEM = '35G'
                elif t in ["AM","DV"] and source in ["KYTHERA","MDCD"]: 
                    MEM = '50G'
                else: 
                    if race_option == 're':
                        MEM = '5G'
                    else:
                        MEM = '25G'  #could this be 10??

                ## get directory
                if t in ['DV', 'RX']:
                    ## parse through MDCR carrier partition as needed
                    if source in ['MDCR', 'CHIA_MDCR']:
                        dir = config['data_input_dir_DV_RX'][source] + 'data/carrier=false/' + 'toc=' + t + '/' 
                    else:
                        dir = config['data_input_dir_DV_RX'][source] + 'data/' + 'toc=' + t + '/'
                else:
                    dir = config['data_input_dir'][source] + 'data/' + 'toc=' + t + '/'

                ## fix to use actual PRI not test name
                pattern = re.compile(r'.+[a-zA-Z]')
                if pattern.match(str(PRI)):
                    input_PRI = str(PRI).split('.')[0]
                    dir = dir.replace('run_' + PRI, 'run_' + input_PRI)
                
                ## get years
                files = os.listdir(dir)
                years = list(map(lambda x:int(''.join([ele for ele in x if ele.isnumeric()])), files)) 
                years.sort()

                ## remove 2014 for MDCR ED
                if source == 'MDCR' and t == 'ED':
                    years = [x for x in years if x != 2014]

                ## remove older years for KYTHERA
                if source == 'KYTHERA':
                    years = [x for x in years if x >= 2016]

                ## get possible causes
                causes = list(RES_toc_cause.loc[RES_toc_cause['toc'] == t, 'acause'].drop_duplicates().values)

                for y in years:
                    print('    > ' + str(y))

                    for a in causes: 
                        task_collapse_acause = collapse_template.create_task(
                            name = '_'.join(['collapse', source, t, str(y), a]),
                            python = sys.executable,
                            script_path = collapse_script,
                            source = source, 
                            toc = t, 
                            year = y,
                            acause = a,
                            PRI = PRI,
                            race_option = race_option,
                            race_col = race_col,
                            no_race_source = ','.join(no_race_source), ## join 
                            compute_resources={
                                'cores': 1, 
                                'memory': '50G',
                                'stdout': jobmon_o_dir,
                                'stderr': jobmon_e_dir, 
                                'standard_output': slurm_o_dir, 
                                'standard_error': slurm_e_dir
                            }
                        )
                        collapse_task_ids.append(task_collapse_acause) ## append task to list of tasks
        
        print('Adding tasks to workflow')
        workflow.add_tasks(collapse_task_ids)

        print('Binding to database')
        workflow.bind()
        WFID = str(workflow.workflow_id)
        print("URL")

        print('Running workflow')
        wf_run = workflow.run(seconds_until_timeout = 259200)
        if wf_run == 'D':
            print('---------------------------')
            print('COLLAPSE workflow finished!')
            print('---------------------------')
        else:
            raise RuntimeError('The COLLAPSE submitter workflow did not complete successfully.')
