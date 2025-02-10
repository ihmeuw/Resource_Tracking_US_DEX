## ==================================================
## Author(s): Sawyer Crosby, Meera Beauchamp
## Date: Jan 31, 2025
## Purpose: Launches the repartition step of COLLAPSE in parallel on a SLURM cluster
## ==================================================

## Although this code includes functionality to collapse by race, this functionality was not used.
## For all occurences, the following setting were used:
## ...
race_opt = "no_re" ## (no race/ethincity information was used)
## ...

## throttle
throttle = 10000 ## jobmon default = 10,000

## make sure these are consisten with what you used in 01_launch_collase.py
TEST_RUN = 0
TEST_LABEL = 'NA' 

## importing modules
import sys
import os
import pandas as pd
pd.options.mode.chained_assignment = None
from attr import attr
import getpass
import uuid
import shutil
from pathlib import Path
from datetime import datetime
from jobmon.client.api import Tool
import argparse
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

if TEST_RUN:
    PRI = str(PRI) +  '.' + TEST_LABEL
    
print('Setting up config')
raw_config = get_config()
config = parsed_config(config = raw_config, key = 'COLLAPSE', run_id = PRI)
metadata = raw_config['METADATA']

print('Defining list of causes, tocs, and metrics')
## getting restrictions
RES_toc_cause = pd.read_csv(metadata['toc_cause_restrictions_path'])
RES_toc_cause = RES_toc_cause.loc[(RES_toc_cause['include'] == 1) & (RES_toc_cause['gc_nec'] == 0), ['toc', 'acause']].reset_index(drop = True)
RES_toc_metric = pd.read_csv(metadata['toc_metric_restrictions_path'])
## --- make list of tocs
tocs = RES_toc_metric['toc'].unique().tolist()

# print('Location scripts and directories')
user = getpass.getuser()
repartition_script = os.path.dirname(os.path.abspath(__file__)) + '/repartition.py'
data_dirs = [
    config['collapse_output_dir'] + 'data', ## passed to comorb
    config['pipeline_output_dir'] + 'data' ## final for model
]
diagnostic_dirs = [
    config['collapse_output_dir'] + 'combos_post_repartition'
]
log_dir = "FILEPATH"
# Check if the directory does not exist
if not os.path.exists(log_dir):
    # Create the directory including any necessary parent directories
    os.makedirs(log_dir)
    print(f"Created directory: {log_dir}")
else:
    print(f"Directory already exists: {log_dir}")

## adjust to be race specific
if race_opt == 're':
    data_dirs = [dir + '_race' for dir in data_dirs]
    diagnostic_dirs = [dir + '_race' for dir in diagnostic_dirs]
elif race_opt == 'both':
    data_dirs = data_dirs + [dir + '_race' for dir in data_dirs]
    diagnostic_dirs = diagnostic_dirs + [dir + '_race' for dir in diagnostic_dirs]

## create tasks
if __name__ == '__main__':
    
    print('Parsing arguments')
    parser = argparse.ArgumentParser()
    parser.add_argument('-w', '--wipe', type=int, required=False, default = 1, choices=[1, 0], help='Clear old outputs? (1 = yes / 0 = no)')
    parser.add_argument('-l', '--launch', type=int, required=False, default = 1, choices=[1, 0], help='Actually launch jobs? (1 = yes / 0 = no) -- set to 0 if you only want to wipe old data.')
    args = vars(parser.parse_args())
    wipe = args['wipe']
    launch = args['launch']

    ## !!! CONFIRM IDS ARE CORRECT
    check1 = ''.join([
        '---------------------------------------------',
        '\nConfirm launch of collapse REPARTITION for:',
        '\n    > run_id: %s' % (str(PRI)), 
        '\n    > race_option: %s' % (str(race_opt)), 
        '\n    > wiping old data: %s' % (str(bool(wipe))),
        '\n    > launching jobs: %s' % (str(bool(launch))), 
        '\n[yes/no]: '
    ])
    
    if input(check1) != 'yes':
        raise ValueError('Must confirm with "yes" to run')

    if wipe:
        print('Clearing old files')
        print(' > logs')
        shutil.rmtree(log_dir, ignore_errors = True)
        Path(log_dir).mkdir(exist_ok = True, parents = True)
        print(' > data')
        for d in data_dirs:
            shutil.rmtree(d, ignore_errors = True)
            Path(d).mkdir(exist_ok = True, parents = True)
        print(' > diagnostics')
        for d in diagnostic_dirs:
            shutil.rmtree(d, ignore_errors = True)
            Path(d).mkdir(exist_ok = True, parents = True)
    
    ## If launching jobs (not just deleting files)
    if launch:

        print('Defining workflow and executor')
        tool = Tool(name='repartition')
        wf_uuid = uuid.uuid4()
        workflow = tool.create_workflow(name=f'collapse{wf_uuid}')

        print('Defining template')
        repartition_template = tool.get_task_template(
            default_compute_resources={
                'queue': 'all.q',
                'runtime': '2h',
                'project': 'proj_dex',
            },
            template_name='repartition',
            default_cluster_name='slurm',
            command_template='{python} {script_path} --acause {acause} --toc {toc} --metric {metric} --PRI {PRI} --race_option {race_option}',
            node_args=['acause', 'toc', 'metric', 'PRI', 'race_option'],
            op_args=['python', 'script_path'],
        )

        print('Creating repartition tasks')
        repartition_task_ids = [] ## keep list of repartition task IDs
        
        if race_opt == 're':
            mem = '1G'
        elif race_opt == 'no_re':
            mem = '3G'
        elif race_opt == 'both':
            mem = '4G'

        for t in tocs:
            print(' - ', t, ' (making cause/metric jobs)')

            ## get list of causes
            causes = list(RES_toc_cause.loc[RES_toc_cause['toc'] == t, 'acause'].values)
            ## get list of metrics
            metrics = list(RES_toc_metric.loc[RES_toc_metric['toc'] == t, 'metric'].values)

            for a in causes:
                for m in metrics:
                    task_repartition = repartition_template.create_task(
                        name = '_'.join(['repartition', a, t, m]),
                        python = sys.executable,
                        script_path = repartition_script,
                        acause = a,
                        toc = t,
                        metric = m,
                        PRI = PRI,
                        race_option = race_opt, 
                        compute_resources={
                            'cores': 4, 
                            'memory': mem, 
                            'stdout': log_dir,
                            'stderr': log_dir
                        }
                    )
                    repartition_task_ids.append(task_repartition)
        
        print('Adding tasks to workflow')
        workflow.add_tasks(repartition_task_ids)

        print('Binding to database')
        workflow.bind()
        WFID = str(workflow.workflow_id)
        print("URL")

        print('Running workflow')
        wf_run = workflow.run(seconds_until_timeout = 259200)
        if wf_run == 'D':
            print('------------------------------')
            print('Repartition workflow finished!')
            print('------------------------------')
        else:
            raise RuntimeError('The repartition submitter workflow did not complete successfully.')
