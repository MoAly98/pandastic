'''
This module allows user to retry Panda tasks based on a regex pattern
that are in a given state.
'''

# Required Imports
# System
import sys, os, re, json
import argparse
# Panda
from pandaclient import PBookCore
from pandaclient import queryPandaMonUtils
pbook = PBookCore.PBookCore()
# Pandastic
from tools import ( draw_progress_bar )

# ===============  Arg Parser Help ===============================
_h_regexes   = 'A regex/pattern in the taskname to be used to find the jobs to retry'
_h_days      = 'How many days in the past should we look for the jobs?'
_h_user      = 'By default the tasks for the current user are the ones that will be queried. Use this option to query other users.'
_h_newargs   = 'New arguments to retry the jobs with must be passed as a dictionary inside a single-quote string'
_h_submit    = 'Should the code submit the retry jobs? Default is to run dry'
_h_fromfiles = 'Files containing lists of tasks to retry'
_h_usetasks  = 'Specify task statuses to look for here'

# ===============  Arg Parser Defaults ===============================
_d_usetasks = ['ready','pending','exhausted','finished','failed', 'broken']

# ===============  Arg Parser Choices ===============================
_choices_usetasks =  ['submitted', 'defined', 'activated',
                      'assigned', 'starting', 'running',
                      'merging', 'finished', 'failed',
                      'cancelled', 'holding', 'transferring',
                      'closed', 'aborted', 'unknown', 'all',
                      'throttled', 'scouting', 'scouted', 'done',
                      'tobekilled', 'ready', 'pending', 'exhausted', 'paused']

def argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--regexes',    type=str, nargs = '+', required=True, help=_h_regexes)
    parser.add_argument('-d', '--days',       type=int, default=30,                 help=_h_days)
    parser.add_argument('-u', '--users',      type=str, default=[pbook.username],   help=_h_user)
    parser.add_argument('--submit',           action='store_true',                  help=_h_submit)
    parser.add_argument('--newargs',          type=str,                             help=_h_newargs)
    parser.add_argument('--fromfiles',        type=str,                             help=_h_fromfiles)
    parser.add_argument('--usetasks',         nargs='+', default=_d_usetasks,       help=_h_usetasks)

    return parser.parse_args()

def run():

    args = argparser()

    regexes     = args.regexes
    users       = args.users
    usetasks    = '|'.join(args.usetasks)
    days        = args.days
    newargs     = args.newargs
    fromfiles   = args.fromfiles

    if fromfiles is not None:
        filetasks = get_lines_from_files[fromfiles]
        tasks = []
        for t in filetasks:
            if not any(re.match(rgx, t) for rgx in regexes): continue
            _, url, jeditask = queryPandaMonUtils.query_tasks(taskname=t)
            tasks.append(task)
    else:
        tasks = []
        for usr in users:
            print(f"INFO:: Querying tasks with statuses {usetasks} for user {usr} in the past {days} days")
            _, url, usrtasks = queryPandaMonUtils.query_tasks(username=usr,
                                                              days=days,
                                                              status=usetasks)

            tasks.extend(usrtasks)

        tasks = [t for t in tasks if any(re.match(rgx, t.get("taskname")) for rgx in regexes)]



    do_retry(tasks, args.submit, newargs)
    print(f"INFO:: Found {len(tasks)} tasks to retry")

def do_retry(tasks, submit, newargs):

    if newargs is not None:
        newargs  = json.loads(newargs)

    for i, task in enumerate(tasks):
        draw_progress_bar(len(tasks), i, "Progress in retrying tasks" )
        taskid = task.get("jeditaskid")
        taskname = task.get("taskname")
        status = task.get("status")

        print(f"INFO:: TO RETRY: {taskname}, Status = {status}")

        if submit:  pbook.retry(taskid, newOpts=newargs)


if __name__ == "__main__":  run()
