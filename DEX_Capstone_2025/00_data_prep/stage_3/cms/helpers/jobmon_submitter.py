import sys

from jobmon.client.api import Tool


def workflow(tool_name, wf_name, command_args):
    """
    Returns a Jobmon workflow with default values
    """
    # Creating tool and workflows with specified names
    tool = Tool(name=tool_name)
    workflow = tool.create_workflow(workflow_args=wf_name, name=wf_name)

    # Formatting command template from desired arguments
    command_template = (
        "OMP_NUM_THREADS={cores} {python} {script} " + "".join([f"--{i} {{{i}}} " for i in command_args])[:-1]
    )

    # Creating task template
    task_temp = tool.get_task_template(
        template_name=tool_name,
        command_template=command_template,
        node_args=list(command_args),
        op_args=["cores","python", "script"],
    )

    return workflow, task_temp


def task(task_temp, task_name, compute_params, task_params):
    """
    Returns a Jobmon task with specified compute resources and task parameters
    """
    # Creating task with specified parameters
    task = task_temp.create_task(
        name=task_name,
        cluster_name="slurm",
        compute_resources=compute_params,
        python=sys.executable,
        **task_params,
    )
    return task