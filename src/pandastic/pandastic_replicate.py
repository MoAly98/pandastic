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
# Rucio
from rucio import client as rucio_client
import rucio
# Pandastic
from tools import ( dataset_size, bytes_to_best_units, draw_progress_bar, merge_dicts, get_lines_from_files)
from common import ( get_rses_from_regex, has_replica_on_rse, has_rule_on_rse, RulesAndReplicasReq )

# ===============  Rucio Clients ================
rulecl = rucio_client.ruleclient.RuleClient()
didcl = rucio_client.didclient.DIDClient()
rsecl = rucio_client.rseclient.RSEClient()
replicacl = rucio_client.replicaclient.ReplicaClient()

# ===============  ArgParsing  ===================================
# ===============  Arg Parser Help ===============================
_h_regex                  = 'A regex in the panda *taskname* to be used to find the jobs to replicate datasets from'
_h_rses                   =  'The RSEs to replicate to (create rules there)'
_h_rule_on_rse            = 'List of RSEs that the DID must have rule on *any* of them before we replicate it'
_h_replica_on_rse         = 'List of RSEs that the DID must have replica on *any* of them before we replicate it'
_h_rule_or_replica_on_rse = 'Use if both replica_on_rse and rule_on_rse are used, to specify that if either are satisfied, replication will happen.\
                             Not using this means both must be satified satisfied'
_h_scopes                 = 'Scopes to look for the DIDs in if --usetask is not used'
_h_type                   = 'Type of dataset being replicated .. is it the task input or output?'
_h_days                   = 'The number of days in the past to look for jobs in'
_h_users                  = 'The grid usernames under for which the jobs should be searched (often your normal name with spaces replaced by +)'
_h_life                   = 'How long is should the lifetime of the dataset be on its destination RSE'
_h_did                    = 'Subset of the jobs following the pattern to keep'
_h_submit                 = 'Should the code submit the replication jobs? Default is to run dry'
_h_usetask                = 'Should the regex be used to filter PanDA jobs? Specify task statuses to look for here'
_h_containers             = 'Should the code only replicate containers? Default is to replicate individual datasets. DID regex will be used to match the container name.'
_h_outdir                 = 'Output directory for the output files. Default is the current directory'
_h_fromfiles              = 'Files containing lists of datasets to replicate \n \
                             If this is used, only --regex, --rses, --submit, --outdir arguments are used as well.\n\
                             The regex will be used to filter the datasets, and rses will be used get datasets to replicate'

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
    parser = argparse.ArgumentParser("This is used to replicate datasets using RUCIO client")
    parser.add_argument('-s', '--regex',              type=str,   required=True,   nargs='+',            help=_h_regex)
    parser.add_argument('-r', '--rses',               type=str,    required=True,   nargs='+',            help=_h_rses)
    parser.add_argument('-d', '--days',               type=int,   default=30,                            help=_h_days)
    parser.add_argument('-u', '--grid-user',          nargs='+',  default=[pbook.username],              help=_h_users)
    parser.add_argument('-l', '--life',               type=int,   default = 3600,                        help= _h_life)
    parser.add_argument('--did',                      type=str,                                          help=_h_did)
    parser.add_argument('--scopes',                   nargs='+',                                         help=_h_scopes)
    parser.add_argument('--rule_on_rse',              nargs='+',                                         help=_h_rule_on_rse)
    parser.add_argument('--replica_on_rse',           nargs='+',                                         help=_h_replica_on_rse)
    parser.add_argument('--rule_or_replica_on_rse',   action='store_true',                               help=_h_rule_or_replica_on_rse)
    parser.add_argument('--usetask',                  nargs='+', choices = _choices_usetasks,            help=_h_usetask)
    parser.add_argument('--type',                     type=str,   choices=['OUT','IN'],                  help=_h_type)
    parser.add_argument('--containers',               action='store_true',                               help=_h_containers)
    parser.add_argument('--submit',                   action='store_true',                               help=_h_submit)
    parser.add_argument('--outdir',                   type=str,   default='./',                          help=_h_outdir)
    parser.add_argument('--fromfiles',                type=str,   nargs='+',                             help=_h_fromfiles)

    return parser.parse_args()

def add_rule(ds, rse, lifetime, scope):
    '''
    Method to add a rule for a dataset

    Parameters
    ----------
    ds: str
        Dataset to add a rule for
    rse: str
        RSE to add the rule to
    lifetime: int
        Lifetime of the rule
    scope: str
        Scope of the dataset

    Returns
    -------
    rule: str
        The rule ID if the rule was successfully added, else None
    '''

    try:
        rule = rulecl.add_replication_rule([{'scope':scope, 'name': ds.replace('/','')}], 1, rse_expression, lifetime = lifetime)
        print(f'INFO:: DS = {ds} \n RuleID: {rule[0]}')
        return rule[0]

    except rucio.common.exception.DuplicateRule as de:
        print(f"WARNING:: Duplication already done for \n {ds} \n to {rse_expression} ...  skipping!")
        return None

    except rucio.common.exception.ReplicationRuleCreationTemporaryFailed as tfe:
        print(f"WARNING:: Duplication not currently possible for \n {ds} \n to {rse_expression} ...  skipping, try again later!")
        return None

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

    only_cont = args.containers
    scopes    = args.scopes

    usetasks  = '|'.join(args.usetask) if args.usetask is not None else None

    if args.fromfiles is not None and usetasks is not None:
        print("ERROR: Cannot specify both --usetask and --fromfiles. Exiting.")
        exit(1)

    # If specified, make sure the dataset has a rule/replica/both on RSEs with given regex
    existing_copies_req = RulesAndReplicasReq(args.rule_on_rse, args.replica_on_rse, args.rule_or_replica_on_rse)

    # Get the RSEs to replicate to from available RSEs using regexes (if any)
    rses_to_replicate_to = set()

    for rse in args.rses:
        rses_to_replicate_to |= get_rses_from_regex(rse, rsecl)

    assert len(rses_to_replicate_to) > 0, "No RSEs found to replicate to. Exiting."

    to_repl = defaultdict(set)

    if args.fromfiles is not None:
        # Get the datasets from the files
        all_datasets = get_lines_from_files(args.fromfiles)
        for ds in all_datasets:
            scope, did = ds.split(':')
            if not any(re.match(regex, did) for regex in regexes):
                continue
            to_repl[scope].add(did.strip())

    elif usetasks is not None:

        # ===========
        # If we are using PanDA tasks, get the datasets from the tasks
        # ===========

        # Warn user if they specify useless args
        if scopes is not None:
            print("WARNING:: --usetask and --scopes are mutually exclusive. Ignoring --scopes")

        users     = args.grid_user
        days      = args.days
        ds_type   = args.type
        did_regex = args.did

        # If --usetask is used, the dataset type must be specified
        assert ds_type is not None, "ERROR:: --type must be specified if --usetask is used"

        for user in users:
            print(f"INFO:: Looking for tasks which are {usetasks} on the grid for user {user} in the last {days} days")
            # Find all PanDA jobs that are done for the user and period specified
            _, url, tasks = queryPandaMonUtils.query_tasks( username=user, days=days, status=usetasks)

            # Tell the user the search URL if they want to look
            print(f"INFO:: PanDAs query URL: {url}")

            # Workout the containers/datasets to replicate for from the PanDA tasks
            to_repl = merge_dicts(to_repl, get_datasets_from_jobs(tasks, regexes, ds_type, did_regex, only_cont))

    else:
        # =============
        # If no association with PanDA tasks, just use the regexes over all rucio datasets in given scopes
        # =============
        # ==================================================== #
        # Warn user about useless args
        # ==================================================== #
        if args.grid_user != pbook.username:
            print("WARNING:: You are not using --usetask, so --grid_user is useless. Ignoring --grid_user")
        if args.days is not None:
            print("WARNING:: You are not using --usetask, so --days is useless. Ignoring --days")
        if args.type is not None:
            print("WARNING:: You are not using --usetask, so --type is useless. Ignoring --type")
        if args.did is not None:
            print("WARNING:: You are not using --usetask, so --did is useless. Ignoring --did")
        # ==================================================== #

        # if --usetask is not used, the scopes must be specified
        assert scopes is not None, "ERROR:: --scopes must be specified if --usetask is not used"

        for scope in scopes:
            print("INFO:: Looking for datasets in scope", scope, "matching regexes")
            for regex in regexes:
                print("INFO:: Looking for datasets matching regex", regex)
                dids = list(didcl.list_dids(scope, {'name': regex.replace('.*','*').replace('/','')}))

                if len(dids) == 0:
                    print("WARNING:: No datasets found matching regex", regex)
                    continue

                for i, did in enumerate(dids):

                    # ============ Progresss Bar ============= #
                    draw_progress_bar(len(dids), i, f'Progress for collecting dids matching regex {regex}')

                    did_type = didcl.get_metadata(scope, did.replace('/','')).get('did_type')
                    if only_cont and did_type != 'CONTAINER': continue
                    elif not only_cont and did_type != 'DATASET': continue
                    to_repl[scope].add(did)

    # Filter the datasets to replicate based on the existing copies
    to_repl = filter_datasets_by_existing_copies(to_repl, existing_copies_req, norule_on_allrses=args.rses, didcl=didcl)

    # ==================================================== #
    # ================= Delete the datasets ============== #
    # ==================================================== #
    # Keep track of the replications (what, where, ruleid)
    actual_replication_summary = defaultdict(lambda: defaultdict(dict))

    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    nreplicated, totalsize_repl = 0, 0

    # Prepare output files for monitoring
    dids_monit_file = open(f'{outdir}/monit_replication_dids_{now}.txt', 'w')
    ruleid_monit_file = open(f'{outdir}/monit_replication_ruleids_{now}.txt', 'w')

    # Loop over the scopes
    for scope, dids in to_repl.items():
        # Loop over the datasets in scope
        for did in dids:

            # Try to get the size of the dataset, use as proxy to skip datasets
            # found from a task but not existing on Rucio...
            try:
                totalsize_repl += dataset_size(did, scope, didcl)
            except rucio.common.exception.DataIdentifierNotFound:
                print("WARNING:: Dataset not found in Rucio: ", did, "Skipping...")
                continue

            # Loop over the RSEs to replicate to
            for rse in rses_to_replicate_to:

                # Tell the user what we are doing
                print("INFO:: Replicating rules for dataset: ", did)
                print("INFO:: Replicating to RSE: ", rse)
                # Only really add the rule if --submit is used
                if args.submit:
                    ruleid = add_rule(did, rse, args.lifetime, scope)
                else:
                    ruleid = 'NOT_SUBMITTED'


                # Write to monitoring scripts
                dids_monit_file.write(f"{scope}:{did}\n")
                ruleid_monit_file.write(ruleid+'\n')
                # Keep track of number of rule deletions
                nreplicated += 1
                # Keep track of what we replicated exactly
                actual_replication_summary[did][rse]['ruleid'] = ruleid

    dids_monit_file.close()
    ruleid_monit_file.close()

    # Dump the replication summary to a json file
    with open(f'{outdir}/datasets_to_replicate_{now}.json', 'w') as f:
        json.dump(actual_replication_summary, f, indent=4)

    # Summarise to user
    print("INFO:: TOTAL NUMBER OF DATASET RULES TO CREATE: ", nreplicated)
    good_units_size = bytes_to_best_units(totalsize_repl)
    print("INFO:: TOTAL Size OF DATASETS TO REPLICATE: ", good_units_size[0], good_units_size[1])

    # Remove the monit file if --submit is not used
    if not args.submit:
        print("INFO:: --submit not used, so not replicating. Deleting ruleid monit file")
        os.unlink(f'{outdir}/monit_replication_ruleids_{now}.txt')



if __name__ == "__main__":  run()
