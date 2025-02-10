import argparse
from datetime import datetime
from pathlib import Path
from jobmon.client.api import Tool
import sys
import pandas as pd
from pathlib import Path
import getpass
import numpy as np


def check_valid_payer_combo(d, pp_d):
    """Reads in pri_payer/toc/payer df and checks the given a dictionary of parameters
    has a valid pri_payer/toc/payer combination. Returns True or False.
    """
    return d["payer"] in pp_d[(d["pri_payer"], d["toc"])]


def agg_median_files(model_ver):
    median_levels = ["1_age_yr_loc", "2_geo_age_yr_loc", "3_cause_geo_age_yr_loc"]
    for level in median_levels:
        print(f"Aggregating all median files for {level}.")
        basepath = Path("FILEPATH")
        filepaths = [
            i for i in basepath.iterdir() if i.suffix == ".csv" and i.stem != "nas"
        ]

        if level == "1_age_yr_loc":
            group_cols = [
                "geo",
                "toc",
                "acause",
                "metric",
                "pri_payer",
                "payer",
                "sex_id",
                "origin",
            ]
        elif level == "2_geo_age_yr_loc":
            group_cols = [
                "toc",
                "acause",
                "metric",
                "pri_payer",
                "payer",
                "sex_id",
                "origin",
            ]
        elif level == "3_cause_geo_age_yr_loc":
            group_cols = ["toc", "metric", "pri_payer", "payer", "sex_id", "origin"]
        else:
            raise RuntimeError("Unexpected agg_type.")

        df_list = []
        for path in filepaths:

            if level == "1_age_yr_loc":
                col_name = str(path.stem).split("_")[3:]
            else:
                col_name = str(path.stem).split("_")[2:]
            if len(col_name) > 1:
                col_name = ["_".join(col_name)]

            if level == "1_age_yr_loc":
                df_list.append(
                    pd.read_csv(path)
                    .drop_duplicates()
                    .set_index(group_cols)[["med"]]
                    .rename(columns={"med": col_name[0]})
                )
            else:
                df_list.append(
                    pd.read_csv(path)
                    .drop_duplicates()
                    .set_index(group_cols)[["med"]]
                    .rename(columns={"med": col_name[0]})
                )

        median_df = (
            pd.concat(df_list, axis=0)
            .reset_index()
            .groupby(group_cols, as_index=False)
            .sum(min_count=1)
        )

        median_df.to_csv(
            "FILEPATH",
            index=False,
        )


def run_impute_submitter(model_ver, draws, max_model_year):

    ##############################
    # JOBMON AND CONSTANTS SETUP #
    ##############################
    # Setting up Jobmon workflow/tool info
    job_tool = Tool(name="Impute")
    wf_name = f'dex_IMPUTE_{datetime.now().strftime("%Y-%m-%d_%H%M%S")}'
    wf = job_tool.create_workflow(workflow_args=wf_name, name=wf_name)

    # Columns to group by (all non-value columns, non-location columns, and non-age/year columns)
    arg_cols = ["toc", "acause", "metric", "pri_payer", "sex_id", "payer"]

    # Value columns that we are imputing. Currently set to 50 draws (draw_n) and median (median)
    val_cols = ["mean"] + ["draw_" + str(i) for i in range(1, draws + 1)]

    # Creating imputation task temp
    impute_task_temp = create_task_temp(
        job_tool,
        task_temp_name="impute_data",
        script_path=Path(__file__).resolve().parent / "helpers" / "impute_worker.py",
        script_args=arg_cols + ["geo", "model_version", "draws", "max_model_year"],
    )

    ##############################
    # MEDIAN AGGREGATION
    ##############################
    median_path = Path("FILEPATH")
    if (
        (median_path / "FILEPATH").is_file()
        and (median_path / "FILEPATH").is_file()
        and (median_path / "FILEPATH").is_file()
    ):
        print("Medians already aggregated.")
    else:
        print("Aggregating medians.")
        agg_median_files(model_ver)

    # Loading dataframe containing valid data combinations
    # And converting to a dictionary for faster parsing
    pp_df = pd.read_csv("FILEPATH")
    pp_d = (
        pp_df.groupby(["pri_payer", "toc"])["payer"]
        .apply(lambda x: x.unique().tolist())
        .to_dict()
    )

    ###############################################################
    # FINDING PARAMETERS WITH CONVERGENCE ISSUES OR MISSING YEARS #
    ###############################################################
    # Loading in parameter df containing params that were not written or had convergence issues
    params_df = pd.read_csv("FILEPATH")
    params_df = params_df[(params_df["geo"] != "national")]

    # Getting all non-converging params to impute as dicts
    nonconv_combos = list(
        params_df[params_df["convergence"] == "No"][arg_cols + ["geo"]]
        .to_dict("index")
        .values()
    )

    ###############################################
    # FINDING PARAMETERS WITH MEDIAN VALUE ISSUES #
    ###############################################
    # Loading in parameter median df containing implausible and missing params
    params_med_df = pd.read_csv("FILEPATH")
    params_med_df = params_med_df[
        (params_med_df["origin"] == "modeled") & (params_med_df["geo"] != "national")
    ]

    # Loading in implausible threshold values
    thresh_df = pd.read_csv("FILEPATH")

    # Adding implausible threshold values to each row. Threshold value specific to acause/toc/metric is used if possible, otherwise
    # Threshold value for toc/metric is used
    params_med_df = params_med_df.merge(
        thresh_df, on=["toc", "metric", "acause"], how="left"
    ).merge(thresh_df[["toc", "metric", "threshold"]], on=["toc", "metric"], how="left")
    params_med_df["threshold"] = np.where(
        params_med_df["threshold_x"].isnull(),
        params_med_df["threshold_y"],
        params_med_df["threshold_x"],
    )
    params_med_df.drop(columns=["threshold_x", "threshold_y"], inplace=True)

    # Getting all missing/implausible params to impute as dicts
    implaus_combos = list(
        params_med_df[
            (params_med_df[val_cols].isna().any(axis=1))
            | (
                params_med_df[val_cols]
                .gt(params_med_df["threshold"], axis=0)
                .any(axis=1)
            )
        ][arg_cols + ["geo"]]
        .to_dict("index")
        .values()
    )

    # Confirming that all combinations are valid payer combinations
    all_impute_combos = []
    for combo in nonconv_combos + implaus_combos:
        if check_valid_payer_combo(combo, pp_d):
            all_impute_combos.append(combo)

    # Removing any duplicates in the lists of params to impute to avoid multiple writes of data
    all_impute_combos = [
        dict(t) for t in {tuple(sorted(d.items())) for d in all_impute_combos}
    ]

    #######################################
    # SETTING UP JOBMON TASKS AND RUNNING #
    #######################################
    # Creating tasks, one for each set of parameters in the impute_dict
    df_list = []
    task_list = []
    for row_dict in all_impute_combos:

        # Setting logpath and outpath
        logpath = Path(f"FILEPATH/{getpass.getuser()}") / wf_name
        logpath.mkdir(parents=True, exist_ok=True)

        # Creating processing task and adding to workflow
        impute_task = impute_task_temp.create_task(
            name="_".join([str(i) for i in row_dict.values()]),
            cluster_name="slurm",
            compute_resources={
                "queue": "all.q",
                "cores": 2,
                "memory": "25G",
                "runtime": "5000s",
                "stdout": logpath.as_posix(),
                "stderr": logpath.as_posix(),
                "project": "proj_dex",
                "constraints": "archive",
            },
            **{
                "model_version": model_ver,
                "draws": draws,
                "max_model_year": max_model_year,
            },
            **row_dict,
        )
        task_list.append(impute_task)

    # Creating dataframe with all imputed combos
    impute_params_df = pd.DataFrame(all_impute_combos)
    impute_params_df["read_from"] = "imputed"

    # Creating dataframe with all modeled combos that were not imputed
    model_params_df = params_df[params_df["convergence"] == "Yes"][
        arg_cols + ["geo"]
    ].copy()
    model_params_df["read_from"] = "modeled"

    # Combining both dataframes and saving out. This csv can be used to determine where a combination
    # Of parameters should be read from (either modeled folder or imputation folder)
    all_params_df = pd.concat([impute_params_df, model_params_df])
    all_params_df = all_params_df.loc[
        (
            all_params_df[
                ["acause", "geo", "metric", "payer", "pri_payer", "sex_id", "toc"]
            ].duplicated(keep=False)
            == False
        )
        | (all_params_df["read_from"] == "imputed"),
        :,
    ]
    Path("FILEPATH").mkdir(exist_ok=True)
    all_params_df.to_csv(
        "FILEPATH",
        index=False,
    )

    # Adding tasks to workflow
    print(f"Adding tasks to workflow.")
    wf.add_tasks(task_list)

    # Running workflow and printing result.
    print(f"Running workflow.")
    wf_run = wf.run(seconds_until_timeout=86400)
    if wf_run == "D":
        print(f"Workflow completed successfully!")
    else:
        print(f"The workflow did not complete successfully. Please check error logs.")


def create_task_temp(tool, task_temp_name, script_path, script_args):

    # Formatting command template from desired arguments
    command_template = (
        f"{sys.executable} {script_path} "
        + "".join([f"--{arg} {{{arg}}} " for arg in script_args])[:-1]
    )

    # Creating task template
    task_temp = tool.get_task_template(
        template_name=task_temp_name,
        command_template=command_template,
        node_args=script_args,
    )

    return task_temp


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-m",
        "--model_version",
        type=str,
        required=True,
        help="Model version to impute.",
    )
    parser.add_argument(
        "-d",
        "--draws",
        type=int,
        required=True,
        help="Number of draws used in model version.",
    )
    parser.add_argument(
        "-y",
        "--max_model_year",
        type=int,
        required=True,
        help="Max year used in modeling.",
    )
    args = vars(parser.parse_args())
    model_ver = args["model_version"]
    draws = args["draws"]
    max_model_year = args["max_model_year"]

    # Running validation submitter
    run_impute_submitter(model_ver, draws, max_model_year)
