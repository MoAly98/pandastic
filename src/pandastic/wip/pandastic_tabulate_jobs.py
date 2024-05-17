'''
This module is responsible for tabulating whether a job is done or not. It allows
multiple regexes to be provided, along with a label to each regex and will produce
a table with the form:

DSID/JOB | CAMP  | label1 | label2 | ....
XXXX     | mc16x | OK     | NOT OK | ....

Same label can be passed twice if 2 regexes belong to the same job category
and the module will just make one column
'''

# Required Imports
# System
import sys, os, re, json
import argparse
from collections import defaultdict
from pprint import pprint
import pandas as pd
# PanDA
# /cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase/x86_64/PandaClient/1.5.9/lib/python3.6/site-packages/pandaclient/PBookCore.py
from pandaclient import PBookCore
from pandaclient import queryPandaMonUtils
pbook = PBookCore.PBookCore()

# Pandastic
from utils.tools import (merge_dicts, sort_dict, nested_dict_equal, get_camp, get_dsid, get_tag, progress_bar)

# ===============  ArgParsing  ===================================
# ===============  Arg Parser Defaults ===============================
_d_incomplete = ['ready','pending','exhausted','finished','failed','throttled','running','scouting','paused','broken','aborted']
_d_complete   = ['done']
# ===============  Arg Parser Help ===============================
_h_regexes    = 'A space separated list of regexes to that identify various outputs of tasks'
_h_labels     = 'A list of labels to identify what the job/container category is. Default is the regexes'
_h_days       = 'How many days in the past should we look for the jobs?'
_h_users      = 'By default the tasks for the current user are the ones that will be queried. Use this option to query other users.'
_h_outpath    = 'The file to dump information to'
_h_bytask     = 'Should the output tabulate task status? If not, it will tabulate DID status and use regex to identify DIDs not tasks'
_h_complete   = 'What task status should be considered complete?'
_h_incomplete   = 'What task status should be considered complete?'



def argparser():
    '''
    Method to parse the arguments for the script.
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--regexes',     type=str, required=True,            nargs='+', help=_h_regexes)
    parser.add_argument('-o', '--outpath',     type=str, required=True,                       help=_h_outpath)
    parser.add_argument('-l', '--labels',      type=str,                           nargs='+', help=_h_labels)
    parser.add_argument('-d', '--days',        type=int, default=30,                          help=_h_days)
    parser.add_argument('-u', '--users',       type=str, default=[pbook.username], nargs='+', help=_h_users)
    parser.add_argument('--complete',          type=str, default=_d_complete,      nargs='+', help=_h_complete)
    parser.add_argument('--incomplete',        type=str, default=_d_incomplete,    nargs='+', help=_h_incomplete)
    parser.add_argument('--bytask',            action='store_true',                           help=_h_bytask)

    return parser.parse_args()


def process_data(jobs, tabulate_tasks, labels, regexes, state):
    '''
    Method to process the data from the query and save the state of the jobs/containers
    in a dictionary. The state of a container is the state of the job that produced it.

    Parameters
    ----------
    data : dict
        The data from the query
    labels : list
        The list of labels to identify the job/containers category
    regexes : list
        The list of regexes to identify the job category
    state : str
        The state of the job. Can be OK or NOT OK.

    Returns
    -------
    dsid_camp_sim_to_status : dict
        The dictionary with the state of the jobs saved.
    '''

    # Declare the dictionary to fill
    dsid_camp_sim_to_status = defaultdict(lambda: defaultdict(dict))
    progress_bar(len(jobs), 0)
    # loop over the jobs
    for i, job in enumerate(jobs):

        if(len(jobs) > 100):
            if (i+1)%100 == 0: progress_bar(len(jobs), i+1, msg=f'Progress for jobs with status {state}')
        elif len(jobs) > 10 and len(jobs) <= 100:
            if (i+1)%10 == 0:  progress_bar(len(jobs), i+1, msg=f'Progress for jobs with status {state}')
        else:
            progress_bar(len(jobs), i+1, msg=f'Progress for jobs with status {state}')

        # Get the job info
        taskid   = job.get("jeditaskid")
        taskname = job.get("taskname")
        status   = job.get("status")
        datasets = job.get("datasets")
        if tabulate_tasks:
            # Fill the dictionary with the state of the job using appropriate keys
            dsid_camp_sim_to_status = save_state(dsid_camp_sim_to_status, taskname, labels, regexes, state)

        else:

            # Declare a list of containers that don't match any regex
            hated_containers = []
            # Loop over the datasets in the job
            for ds in datasets:
                # Skip the dataset if it's not an output dataset
                if ds.get("type") != 'output': continue
                contname = ds.get("containername")

                # For optimisation, skip the dataset if it's in a hated container
                # === Note datasets live in containers,
                # multiple datasets can live in the same container ===
                if contname not in hated_containers:
                    # Keep track of continer before modification
                    dsid_to_camp_to_status_before = dsid_camp_sim_to_status
                    dsid_camp_sim_to_status = save_state(dsid_camp_sim_to_status, contname, labels, regexes, state)
                    # If no new entries are made to the dictionary, then the container is doesn't match any regex
                    if nested_dict_equal(dsid_to_camp_to_status_before, dsid_camp_sim_to_status):
                        hated_containers.append(contname)
                        continue
                else:
                    continue

    return dsid_camp_sim_to_status


def save_state(dict_to_fill, task_or_did, labels, regexes, state):
    '''
    Method to save the state of a job in a dictionary. The dictionary is
    indexed by (dsid, campaign, sim) and the value is a dictionary with
    the labels as keys and the state as values.

    Parameters
    ----------
    dict_to_fill : dict
        The dictionary to fill with the state of the job
    taskname : str
        The name of the task
    labels : list
        The list of labels to identify the job category
    regexes : list
        The list of regexes to identify the job category
    state : str
        The state of the job

    Returns
    -------
    dict_to_fill : dict
        The dictionary with the state of the job saved

    '''
    # Zip the labels and regexes together
    labels_regexes_zip = zip(labels, regexes)
    # Loop simultaneously over the labels and regexes
    for label, regex in labels_regexes_zip:
        # If the regex doesn't match the taskname, skip to the next regex
        if re.match(regex, task_or_did) is None:   continue
        # Extract the DSID, campaign and sim from the taskname
        campaign = get_camp(task_or_did)
        dsid     = get_dsid(task_or_did)
        tag      = get_tag(task_or_did)

        if   'a' in tag: sim = 'AFII'
        elif 's' in tag: sim = 'FS'
        else: sim = 'unknown'

        # Fill the dictionary
        dict_to_fill[(dsid,campaign,sim)][label] = state
        dict_to_fill[(dsid,campaign,sim)][label+ ' Name'] = task_or_did

    return dict_to_fill


def fill_missing_state(dict_to_fill, labels):
    '''
    Method to fill the dictionary with the state of the jobs/dids with the
    state 'NOT OK' if the job is missing from the dictionary. A missing
    job/did is a job/did that doesn't match any or one of the regexes.
    For example, a job matching Level-0 regex, but not Level-1 because
    Level-1 has not beeen ran yet, then Level-1 will be filled with 'NOT OK'.

    Parameters
    ----------
    dict_to_fill : dict
        The dictionary to fill with the state of the job
    labels : list
        The list of labels to identify the job/did category

    Returns
    -------
    dict_to_fill : dict
        The dictionary with the state of the job/did saved

    '''
    # Loop over the keys in the dictionary
    for k in dict_to_fill:
        # Loop over the labels for different categories
        for label in labels:
            # If a category is missing, fill it with 'NOT OK'
            if label not in dict_to_fill[k]:
                dict_to_fill[k][label] = 'NOT OK'


def prepare_multiindex_for_export(df):

    '''
    The code takes a pandas DataFrame as input and returns a new DataFrame
    that has a flattened multi-index, suitable for export.
    It sorts the input DataFrame by the first index level, then by the second index level
    and so on, until it reaches the last index level. After each sort,
    it adds the current index level to the DataFrame as a new column and removes
    duplicates by replacing the values in the newly added column with NaN where necessary.
    Finally, it resets the index and drops it, so the output DataFrame only contains the data.

    Parameters
    ----------
    df : pandas DataFrame
        The DataFrame to be flattened

    Returns
    -------
    new_df : pandas DataFrame
        The flattened DataFrame
    '''

    # Create a copy of the dataframe
    new_df = df.copy()

    # Loop over the indicies in reverse order
    for i in range(df.index.nlevels, 0, -1):
        # Sort the dataframe by the current index level
        new_df = new_df.sort_index(level=i-1)

    # Declare a dictionary to store the columns to replace
    replace_cols = dict()

    # Loop over the indicies again(again from inside out)
    for i in range(new_df.index.nlevels):
        # Get the values for the current index level
        idx = new_df.index.get_level_values(i)
        # Add the current index level as a new column
        new_df.insert(i, idx.name, idx)
        # Replace the values in the newly added column with NaN where duolicates occur
        replace_cols[idx.name] = new_df[idx.name].where(~new_df.duplicated(subset=new_df.index.names[:i+1]))

    # Replace the values in the newly added columns
    for col, ser in replace_cols.items():
        new_df[col] = ser

    # Reset the index and drop it
    return new_df.reset_index(drop=True)


def run():
    '''
    Main function to run the script
    '''
    # Parse the command line arguments
    args = argparser()

    # Get the list of users
    users = args.users
    # Get the number of days to query
    days = args.days
    # Get the output path for the summary
    outpath = args.outpath
    os.makedirs('/'.join(outpath.split('/')[:-1]), exist_ok=True)

    # Get the list of what defines complete and incomplete jobs
    complete = '|'.join(args.complete)
    incomplete = '|'.join(args.incomplete)

    # Get the list of labels that categorise each regex, ensure list is same length as regexes
    if args.labels is not None:
        assert len(args.labels) == len(args.regexes), "ERROR:: Labels and regexes must have same length"
    else:
        args.labels = args.regexes

    # Declare lists to store the data from the queries
    all_done, all_notdone = [], []

    print("INFO:: Querying PanDAs for jobs...")
    # loop over the users
    for user in users:
        # Query the PanDAs for the jobs that are completed
        _, done_url, done_data = queryPandaMonUtils.query_tasks( username=user,
                                                                 days=days,
                                                                 status=complete)

        all_done.extend(done_data)

        print(f"INFO:: PanDAs query URL for done tasks from user {user}: {done_url}")

        # Query the PanDAs for the jobs that are not completed
        _, notdone_url, notdone_data = queryPandaMonUtils.query_tasks( username=user,
                                                                       days=days,
                                                                       status=incomplete)

        print(f"INFO:: PanDAs query URL for not-done tasks from user {user}: {notdone_url}")
        all_notdone.extend(notdone_data)

    # Build the dictionary for all_done and all_notdone jobs which maps the (DSID, CAMP, TAG) to the state (OK/NOT OK)
    # and then merge the two dictionaries
    print("INFO:: Merging completed and not completed jobs into one dictionary")
    all_jobs = merge_dicts( process_data(all_done, args.bytask, args.labels, args.regexes, "OK"),
                            process_data(all_notdone, args.bytask, args.labels, args.regexes, "NOT OK"))

    print("INFO:: Fill the dictionary with the state of the jobs/dids with the state 'NOT OK' if the job is missing from the dictionary")
    # Fill the dictionary with the state of the jobs/dids with the state 'NOT OK' if the job is missing from the dictionary
    fill_missing_state(all_jobs, args.labels)

    print("INFO:: Sorting the dictionary by DSID, campaign and sim alphabetically")
    # Sort the dictionary by DSID, campaign and sim alphabetically
    all_jobs = sort_dict(all_jobs)

    if all_jobs == {}:
        print("WARNING:: No jobs found!")
        return

    # Convert the dictionary to a pandas multi-index dataframe
    df = pd.DataFrame.from_dict(all_jobs, orient="index").rename_axis(["DSID", "Campaign", "FS/AFII"])
    # Prepare the multi-index dataframe for export
    df = prepare_multiindex_for_export(df)
    print("INFO:: You will find the output in the following file: ", outpath)
    # Save the dataframe to a csv file
    df.to_csv(outpath, index = False)


if __name__ == "__main__":  run()

