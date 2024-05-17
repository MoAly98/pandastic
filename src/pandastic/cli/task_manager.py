#!python3

# Required Imports
# System
import sys, os, json, re
import argparse
from datetime import datetime
from collections import defaultdict
# PanDA: /cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase/x86_64/PandaClient/1.5.9/lib/python3.6/site-packages/pandaclient/PBookCore.py
from   pandaclient import PBookCore
from   pandaclient import queryPandaMonUtils
import pandaclient.Client as Client

pbook = PBookCore.PBookCore()
pbook.init()
# Pandastic
from pandastic.utils.tools import ( draw_progress_bar, merge_dicts )


# ===============  ArgParsing  ===================================
# ===============  Arg Parser Help ===============================
_h_action     = 'The action to perform on the task'
_h_regex      = 'A regex in the panda *taskname* to be used to find the jobs/datasets to delete rules for'
_h_days       = 'The number of days in the past to look for jobs in'
_h_users      = 'The grid usernames under for which the jobs should be searched (often your normal name with spaces replaced by +)'
_h_usetasks   = 'Specify task statuses to look for here'
_h_fromfiles  = 'Files containing lists of tasks to act on'
_h_mincomp    = 'Minimum percentage completion for jobs that should be acted on'
_h_maxcomp    =  'Maximum percentage completion for jobs that should be acted on'

_h_submit     = 'Should the code submit the pausing/unpausing command'
_h_outdir     = 'Output directory for the output files. Default is the current directory'
_h_newargs    = 'New arguments to pass to the retry method'

# ===============  Arg Parser Choices ===============================
_choices_usetasks =  ['submitted', 'defined', 'activated',
                      'assigned', 'starting', 'running',
                      'merging', 'finished', 'failed',
                      'cancelled', 'holding', 'transferring',
                      'closed', 'aborted', 'unknown', 'all',
                      'throttled', 'scouting', 'scouted', 'done',
                      'tobekilled', 'ready', 'pending', 'exhausted', 'paused',
                      'broken', 'submitting', 'finishing', 'aborting', 'passed', 'any']
_action_choices  = ['find', 'pause', 'unpause', 'retry', 'kill']

def argparser():
    '''
    Method to parse the arguments for the script.
    '''
    parser = argparse.ArgumentParser("This is used to perform actions on PanDA tasks")
    parser.add_argument('action',             choices=_action_choices,                 help=_h_action)
    parser.add_argument('-s', '--regexes',    type=str,   required=True,   nargs='+',  help=_h_regex)
    parser.add_argument('-d', '--days',       type=int,   default=30,                  help=_h_days)
    parser.add_argument('-u', '--grid-user',  nargs='+',  default=[pbook.username],    help=_h_users)
    parser.add_argument('--fromfiles',        type=str,   nargs='+',                   help=_h_fromfiles)
    parser.add_argument('--mincomp',          type=float,                              help=_h_mincomp)
    parser.add_argument('--maxcomp',          type=float,                              help=_h_maxcomp)
    parser.add_argument('--outdir',           type=str,   default='./',                help=_h_outdir)
    parser.add_argument('--submit',           action='store_true',                     help=_h_submit)

    req_usetasks = 'unpause' not in sys.argv and '--fromfiles' not in sys.argv
    parser.add_argument('--usetasks',         nargs='+',  required=req_usetasks,
                                              choices = _choices_usetasks,  help=_h_usetasks)
    if '--newargs' in sys.argv:
        parser.add_argument('--newargs',      type=str,                             help=_h_newargs)

    return parser.parse_args()

def run():
    '''
    Main method
    '''
    # ========  Get the arguments ============ #
    args = argparser()

    action = args.action

    # Prepare the output directory
    outdir = args.outdir
    os.makedirs(outdir, exist_ok=True)

    regexes   = args.regexes

    usetasks  = '|'.join(args.usetasks) if args.usetasks is not None else None
    if action == 'unpause': usetasks = 'paused'

    fromfiles = args.fromfiles

    assert (usetasks is not None) or (fromfiles is not None), "ERROR: Must specify either --usetasks or --fromfiles. Exiting."

    to_act_on = []

    if fromfiles is not None:
        # Get the datasets from the files
        tasks = get_lines_from_files(fromfiles)
        for task in tasks:
            if not any(re.match(regex, task) for regex in regexes): continue
            to_act_on.append(task)

    else:

        # ===========
        # If we are using PanDA tasks, get the task names
        # ===========

        users     = args.grid_user
        days      = args.days

        urls = {}
        for user in users:

            print(f"INFO:: Looking for tasks with statuses {usetasks} on the grid for user {user} in the last {days} days")
            if usetasks == "any": usetasks = None
            # Find all PanDA jobs that are done for the user and period specified
            _, url, tasks = queryPandaMonUtils.query_tasks( username=user, days=days, status=usetasks)

            # Tell the user the search URL if they want to look
            print(f"INFO:: PanDAs query URL: {url}")
            tasks = [t for t in tasks if any(re.match(regex, t.get('taskname')) for regex in regexes)]
            to_act_on.extend(tasks)
            url = re.sub('status=.*&', '', url)
            urls[user.lower()] = url.replace('json=1&', '')

    # ==================================================== #
    # ================= Operate on tasks ============== #
    # ==================================================== #
    # Keep track of the operations (what, where, ruleid)
    actual_op_summary = defaultdict(str)
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    nop = 0

    # Prepare output files for monitoring
    tasks_monit_file   = open(f'{outdir}/monit_{action}_tasks_{now}.txt', 'w')

    taskids = defaultdict(list)
    # Loop over the datasets in scope
    for task in to_act_on:

            request_id     = task.get('reqid')
            taskname       = task.get('taskname')
            taskid         = task.get('jeditaskid')
            user           = task.get('username')

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
            print(f"INFO:: {action} the task {taskname} which is {percentage_done} % complete")

            if args.submit:
                # Only really add the rule if --submit is used
                if action == 'pause':
                    try:    pbook.pause(taskid)
                    except Exception as e:
                        print(f"ERROR:: Failed to {action} task {taskname} with error {e}")
                        continue
                elif action == 'unpause':
                    try:    pbook.resume(taskid)
                    except Exception as e:
                        print(f"ERROR:: Failed to {action} task {taskname} with error {e}")
                        continue
                elif action == 'retry':
                    if '--newargs' in sys.argv:
                        newargs  = json.loads(args.newargs)
                    else:
                        newargs = None
                    try:    pbook.retry(taskid, newOpts=newargs)
                    except Exception as e:
                        print(f"ERROR:: Failed to {action} task {taskname} with error {e}")
                        continue
                elif action == 'kill':
                    try:    pbook.kill(taskid)
                    except Exception as e:
                        print(f"ERROR:: Failed to {action} task {taskname} with error {e}")
                        continue
                elif action == 'find':
                    pass
                else:
                    raise ValueError(f"ERROR:: Action {action} not recognised")

            # Write to monitoring scripts
            tasks_monit_file.write(taskname+'\n')
            taskids[user.lower()].append(str(taskid))

            # Keep track of number of rule deletions
            nop+= 1
            # Keep track of what we deleted exactly
            actual_op_summary[taskname] = request_id

    # Close the files for monitoring
    tasks_monit_file.close()

    # string together the taskids to generate a url
    for user, ids in taskids.items():
        taskids_str = '|'.join(ids)
        url = urls[user]
        url += f'&jeditaskid={taskids_str}'
        print(f"INFO:: URL to {action} tasks for user {user}: {url}")

    # Dump the deletion summary to a json file
    with open(f'{outdir}/tasks_to_{action}_{now}.json', 'w') as f:
        json.dump(actual_op_summary, f, indent=4)

    # Summarise to user
    print(f"INFO:: TOTAL NUMBER OF TASKS TO UNDERGO {action}: {nop}")

if __name__ == "__main__":  run()
