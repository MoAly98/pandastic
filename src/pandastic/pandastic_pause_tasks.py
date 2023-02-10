# Required Imports
# System
import sys, os, json, re
sys.path.insert(0,'/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase/x86_64/PandaClient/1.5.33/lib/python3.6/site-packages/')
import json
import argparse
from datetime import datetime
from pprint import pprint
from collections import defaultdict
# PanDA: /cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase/x86_64/PandaClient/1.5.9/lib/python3.6/site-packages/pandaclient/PBookCore.py
from pandaclient import PBookCore
from pandaclient import queryPandaMonUtils
import pandaclient.Client as Client

pbook = PBookCore.PBookCore()

# Pandastic
from tools import ( draw_progress_bar, merge_dicts )


# ===============  ArgParsing  ===================================
# ===============  Arg Parser Help ===============================
_h_regex      = 'A regex in the panda *taskname* to be used to find the jobs/datasets to delete rules for'
_h_days       = 'The number of days in the past to look for jobs in'
_h_users      = 'The grid usernames under for which the jobs should be searched (often your normal name with spaces replaced by +)'
_h_submit     = 'Should the code submit the pausing/unpausing command'
_h_outdir     = 'Output directory for the output files. Default is the current directory'
_h_fromfiles  = 'Files containing lists of tasks to pause/unpause'
_h_unpause    = 'Unpause the tasks after deleting the rules'
_h_pause      = 'Which job statuses to unpause'
_h_mincomp    = 'Minimum percentage completion for jobs that should be paused/unpaused'
_h_maxcomp    =  'Maximum percentage completion for jobs that should be paused/unpaused'

# ===============  Arg Parser Choices ===============================
_choices_pause =  ['submitted', 'defined', 'activated',
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
    parser = argparse.ArgumentParser("This is used to delete rules for datasets using RUCIO client")
    parser.add_argument('-s', '--regex',              type=str,   required=True,   nargs='+',  help=_h_regex)
    parser.add_argument('-d', '--days',               type=int,   default=30,                  help=_h_days)
    parser.add_argument('-u', '--grid-user',          nargs='+',  default=[pbook.username],    help=_h_users)
    parser.add_argument('--pause',                    nargs='+',  choices=_choices_pause,      help=_h_pause)
    parser.add_argument('--submit',                   action='store_true',                     help=_h_submit)
    parser.add_argument('--unpause',                  action='store_true',                     help=_h_unpause)
    parser.add_argument('--outdir',                   type=str,   default='./',                help=_h_outdir)
    parser.add_argument('--fromfiles',                type=str,   nargs='+',                   help=_h_fromfiles)
    parser.add_argument('--mincomp',                  type=float,                              help=_h_mincomp)
    parser.add_argument('--maxcomp',                  type=float,                              help=_h_maxcomp)
    return parser.parse_args()

def run():
    '''
    Main method
    '''
    # ========  Get the arguments ============ #
    args = argparser()

    # Prepare the output directory
    outdir = args.outdir
    os.makedirs(outdir, exist_ok=True)

    regexes   = args.regex

    pause     = '|'.join(args.pause) if args.pause is not None else None
    unpause   = args.unpause
    fromfiles = args.fromfiles

    if pause is not None:

        assert (unpause is False),  "ERROR: Cannot specify both --pause and --unpause. Exiting."
        assert (fromfiles is None), "ERROR: Cannot specify both --pause and --fromfiles. Exiting."
    else:
        assert (unpause) or (fromfiles is not None), "ERROR: Must specify either --pause or --unpause or --fromfiles. Exiting."

    to_un_pause = []

    if fromfiles is not None:
        # Get the datasets from the files
        tasks = get_lines_from_files(fromfiles)
        for task in tasks:
            if not any(re.match(regex, task) for regex in regexes): continue
            to_un_pause.append(task)

    elif pause is not None or unpause :

        # ===========
        # If we are using PanDA tasks, get the task names
        # ===========

        users     = args.grid_user
        days      = args.days

        for user in users:
            if not unpause: statuses = pause
            else: statuses = 'paused'

            print(f"INFO:: Looking for tasks with statuses {statuses} on the grid for user {user} in the last {days} days")
            # Find all PanDA jobs that are done for the user and period specified
            _, url, tasks = queryPandaMonUtils.query_tasks( username=user, days=days, status=statuses)

            # Tell the user the search URL if they want to look
            print(f"INFO:: PanDAs query URL: {url}")
            tasks = [t for t in tasks if any(re.match(regex, t.get('taskname')) for regex in regexes)]
            to_un_pause.extend(tasks)

    # ==================================================== #
    # ================= Pause/Unpause tasks ============== #
    # ==================================================== #
    # Keep track of the deletions (what, where, ruleid)
    actual_pause_summary = defaultdict(str)
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    npause = 0

    # Prepare output files for monitoring
    tasks_monit_file   = open(f'{outdir}/monit_pause_tasks_{now}.txt', 'w')


    if unpause: action = 'Unpausing'
    else:   action = 'Pausing'
    # Loop over the datasets in scope
    for task in to_un_pause:

            request_id     = task.get('reqid')
            taskname       = task.get('taskname')
            taskid         = task.get('jeditaskid')

            # Work out the percentage completion of task using nfiles and nfilesfinished
            nfiles         = float(task.get('nfiles'))
            nfilesfinished = float(task.get('nfilesfinished'))

            # Skip task if it doesn't meet the required completion percentage
            try:
                percentage_done = nfilesfinished*100/nfiles
            except ZeroDivisionError:
                percentage_done = 0

            if args.mincomp is not None:
                if percentage_done < args.mincomp: continue
            if args.maxcomp is not None:
                if percentage_done > args.maxcomp: continue

            # Tell the user what we are doing
            print(f"INFO:: {action} the task {taskname} which is  {percentage_done} % complete")

            # Only really add the rule if --submit is used
            if args.submit and unpause: Client.resumeTask(taskid) # pbook.execute_workflow_command('resume', request_id)
            if args.submit and not unpause: pbook.execute_workflow_command('suspend', request_id)

            # Write to monitoring scripts
            tasks_monit_file.write(taskname+'\n')

            # Keep track of number of rule deletions
            npause += 1
            # Keep track of what we deleted exactly
            actual_pause_summary[taskname] = request_id

    # Close the files for monitoring
    tasks_monit_file.close()

    # Dump the deletion summary to a json file
    with open(f'{outdir}/tasks_to_un_pause_{now}.json', 'w') as f:
        json.dump(actual_pause_summary, f, indent=4)

    # Summarise to user
    print(f"INFO:: TOTAL NUMBER OF TASKS TO UNDERGO {action}: {npause}")

if __name__ == "__main__":  run()
