# Required Imports
# System
import sys, os, json, re
import json
import argparse
from datetime import datetime
from pprint import pprint
from collections import defaultdict
# PanDA: /cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase/x86_64/PandaClient/1.5.9/lib/python3.6/site-packages/pandaclient/PBookCore.py
from pandaclient import PBookCore
from pandaclient import queryPandaMonUtils
pbook = PBookCore.PBookCore()
from rucio import client as rucio_client
didcl = rucio_client.didclient.DIDClient()

# # Pandastic
from utils.tools import ( get_dsid, get_tag )

# ===============  ArgParsing  ===================================
# ===============  Arg Parser Help ===============================
_h_didsfiles              = 'A space separated listo of text files where each line is a container that we need to check if it is related to a job in some status'
_h_type                   = 'Type of dataset being deleted .. is it the task input or output?'
_h_days                   = 'The number of days in the past to look for jobs in'
_h_users                  = 'The grid usernames under for which the jobs should be searched (often your normal name with spaces replaced by +)'
_h_usetask                = 'Should the regex be used to filter PanDA jobs? Specify task statuses to look for here'
_h_tags                   = 'A list of tags to look for in the taskname before we compare dsids and atlas tags with datasets'
_h_outdir                 = 'Output directory for the output files. Default is the current directory'

# ===============  Arg Parser Choices ===============================
_choices_usetasks =  ['submitted', 'defined', 'activated',
                      'assigned', 'starting', 'running',
                      'merging', 'finished', 'failed',
                      'cancelled', 'holding', 'transferring',
                      'closed', 'aborted', 'unknown', 'all',
                      'throttled', 'scouting', 'scouted', 'done',
                      'tobekilled', 'ready', 'pending', 'exhausted', 'paused']
def argparser():
    '''
    Method to parse the arguments for the script.
    '''
    parser = argparse.ArgumentParser("This is used to check if a group of datasets are related to a job in some status by comparing dsids and tags")
    parser.add_argument('-t', '--tags',               type=str,   required=True,   nargs='+',            help=_h_tags)
    parser.add_argument('-s', '--didsfiles',          type=str,   required=True,   nargs='+',            help=_h_didsfiles)
    parser.add_argument('--usetask',                  nargs='+',  required=True, choices = _choices_usetasks,            help=_h_usetask)
    parser.add_argument('-d', '--days',               type=int,   default=30,                            help=_h_days)
    parser.add_argument('-u', '--grid-user',          nargs='+',  default=[pbook.username],              help=_h_users)
    parser.add_argument('--outdir',                   type=str,   default='./',                          help=_h_outdir)

    return parser.parse_args()

def run():
    '''
    Main method
    '''
    # ========  Get the arguments ============ #
    args = argparser()

    outdir = args.outdir
    os.makedirs(outdir, exist_ok=True)

    didsfiles = args.didsfiles

    usetasks  = '|'.join(args.usetask) if args.usetask is not None else None

    if usetasks is not None:

        # ===========
        # Get jobs that are in specified statuses
        # ===========

        users     = args.grid_user
        days      = args.days

        all_tasks = []
        for user in users:
            print(f"INFO:: Looking for tasks which are {usetasks} on the grid for user {user} in the last {days} days")
            # Find all PanDA jobs that are done for the user and period specified
            _, url, tasks = queryPandaMonUtils.query_tasks( username=user, days=days, status=usetasks)
            tasks = filter_tasks(tasks, args.tags)
            all_tasks.extend(tasks)

            # Tell the user the search URL if they want to look
            print(f"INFO:: PanDAs query URL: {url}")

    all_dids = []
    for file in didsfiles:
        with open(file, 'r') as didf:
            dids = didf.readlines()
        all_dids.extend(dids)

    dids_related_to_tasks = []
    for did in all_dids:
        dsid = get_dsid(did)
        atlas_tag  = get_tag(did)
        if any((dsid in task) and (atlas_tag in task) for task in all_tasks):
            print(f"INFO:: Horray, dataset {did} is related to a job with status {usetasks}")
            dids_related_to_tasks.append(did)

    now = datetime.now().strftime("%Y%m%d_%H__%M__%S")
    with open(f"{outdir}/dids_related_to_tasks_{now}.txt", 'w') as f:
        for did in dids_related_to_tasks:
            f.write(did)

    print(len(dids_related_to_tasks), "datasets are related to jobs with status", usetasks)

def filter_tasks(tasks, tags):
    filtered_tasks = []
    for task in tasks:
        if not any(tag in task.get('taskname') for tag in tags):
            continue
        else:
            filtered_tasks.append(task.get('taskname'))

    return filtered_tasks

if __name__ == "__main__":  run()
