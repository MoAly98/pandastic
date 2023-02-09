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
from tools import ( dataset_size, bytes_to_best_units, draw_progress_bar, merge_dicts )
from common import ( get_datasets_from_jobs, filter_datasets_by_existing_copies, get_datasets_from_files,
                     get_rses_from_regex, has_replica_on_rse, has_rule_on_rse, RulesAndReplicasReq)

# ===============  Rucio Clients ================
rulecl = rucio_client.ruleclient.RuleClient()
didcl = rucio_client.didclient.DIDClient()
rsecl = rucio_client.rseclient.RSEClient()
replicacl = rucio_client.replicaclient.ReplicaClient()

# ===============  ArgParsing  ===================================
# ===============  Arg Parser Help ===============================
_h_regex                  = 'A regex in the panda *taskname* to be used to find the jobs/datasets to delete rules for'
_h_rses                   =  'The RSEs to delete rules from'
_h_rule_on_rse            = 'List of RSEs that the DID must have rule on *any* of them before we delete its rules'
_h_replica_on_rse         = 'List of RSEs that the DID must have replica on *any* of them before we delete its rules'
_h_rule_or_replica_on_rse = 'Use if both replica_on_rse and rule_on_rse are used, to specify that if either are satisfied, deletion will happen.\
                             Not using this means both must be satified satisfied'
_h_scopes                 = 'Scopes to look for the DIDs in if --usetask is not used'
_h_type                   = 'Type of dataset being deleted .. is it the task input or output?'
_h_days                   = 'The number of days in the past to look for jobs in'
_h_users                  = 'The grid usernames under for which the jobs should be searched (often your normal name with spaces replaced by +)'
_h_did                    = 'Subset of the jobs following the pattern to keep'
_h_submit                 = 'Should the code submit the rule deletion jobs? Default is to run dry'
_h_usetask                = 'Should the regex be used to filter PanDA jobs? Specify task statuses to look for here'
_h_containers             = 'Should the code only delete containers rules? Default is to delete individual dataset rules. DID regex will be used to match the container name with --usetask'
_h_matchfiles             = 'Strict requirment when finding datasets from PanDA tasks that input and output number of files match for a given task before we delete rules of containers/datasets associated with it'
_h_outdir                 = 'Output directory for the output files. Default is the current directory'
_h_fromfiles              = 'Files containing lists of datasets to delete rules for.\n \
                             If this is used, only --regex, --rses, --submit, --outdir arguments are used as well.\n\
                             The regex will be used to filter the datasets, and rses will be used get rule IDs to delete'

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
    parser = argparse.ArgumentParser("This is used to delete rules for datasets using RUCIO client")
    parser.add_argument('-s', '--regex',              type=str,   required=True,   nargs='+',            help=_h_regex)
    parser.add_argument('-r', '--rses',               type=str,   required=True,   nargs='+',            help=_h_rses)
    parser.add_argument('-d', '--days',               type=int,   default=30,                            help=_h_days)
    parser.add_argument('-u', '--grid-user',          nargs='+',  default=[pbook.username],              help=_h_users)
    parser.add_argument('--did',                      type=str,                                          help=_h_did)
    parser.add_argument('--scopes',                   nargs='+',                                         help=_h_scopes)
    parser.add_argument('--rule_on_rse',              nargs='+',                                         help=_h_rule_on_rse)
    parser.add_argument('--replica_on_rse',           nargs='+',                                         help=_h_replica_on_rse)
    parser.add_argument('--rule_or_replica_on_rse',   action='store_true',                               help=_h_rule_or_replica_on_rse)
    parser.add_argument('--usetask',                  nargs='+',  choices = _choices_usetasks,           help=_h_usetask)
    parser.add_argument('--type',                     type=str,   choices=['OUT','IN'],                  help=_h_type)
    parser.add_argument('--containers',               action='store_true',                               help=_h_containers)
    parser.add_argument('--matchfiles',               action='store_true',                               help=_h_matchfiles)
    parser.add_argument('--submit',                   action='store_true',                               help=_h_submit)
    parser.add_argument('--outdir',                   type=str,   default='./',                          help=_h_outdir)
    parser.add_argument('--fromfiles',                type=str,   nargs='+',                             help=_h_fromfiles)
    return parser.parse_args()


def get_ruleids_to_delete(did, rses_to_delete_from, rse_regexes, scope):
    '''
    Method to get the rule IDs to delete for a dataset and associated RSEs

    Parameters
    ----------
    did: str
        dataset name
    rses_to_delete_from: list
        list of RSEs to delete from

    Returns
    -------
    rule_ids_rses_zip: generator
        zipped list of rule IDs and RSEs to delete from
    '''

    ruleids_to_delete = []
    found_rses_to_delete_from = []

    # Get the rules for the dataset
    rules = list(didcl.list_did_rules(scope, did.replace('/','')))
    # Get the rule IDs to delete
    for rule in rules:
        rse_for_rule = rule.get('rse_expression')
        rule_id = rule['id']
        if rse_for_rule in rses_to_delete_from or any(re.match(rse_rgx, rse_for_rule) for rse_rgx in rse_regexes):
            ruleids_to_delete.append(rule_id)
            found_rses_to_delete_from.append(rse_for_rule)

    rule_ids_rses_zip = zip(ruleids_to_delete,found_rses_to_delete_from)
    return rule_ids_rses_zip

def delete_rule(ruleid):
    '''
    Method to delete a rule for a dataset

    Parameters
    ----------
    ruleid: str
        rule ID to delete
    '''
    try:
        rulecl.delete_replication_rule(ruleid, purge_replicas=True)
    except:
        print(f"WARNING:: Rule deletion failed for rule ID {ruleid} ...  skipping!")

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

    # Get the RSEs to delete rules from, from available RSEs using regexes (if any)
    rses_to_delete_from = set()
    for rse in args.rses:
        rses_to_delete_from |= get_rses_from_regex(rse, rsecl)

    to_delete = defaultdict(set)

    if args.fromfiles is not None:
        # Get the datasets from the files
        all_datasets = get_lines_from_files(args.fromfiles)
        for ds in all_datasets:
            scope, did = ds.split(':')
            if not any(re.match(regex, did) for regex in regexes):
                continue
            to_delete[scope].add(did.strip())


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

            # Workout the containers/datasets to delete rules for from the PanDA tasks
            to_delete = merge_dicts(to_delete, get_datasets_from_jobs(tasks, regexes, ds_type, did_regex, only_cont, args.matchfiles, didcl))
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

                    # Fast track -- skip datasets with no rrules on any of the regexed rses we delete from
                    if not any(has_rule_on_rse(did, scope, rse, didcl) for rse in args.rses):
                        continue

                    # Only process containers/datasets based on user specified option
                    did_type = didcl.get_metadata(scope, did.replace('/','')).get('did_type')
                    if only_cont and did_type != 'CONTAINER': continue
                    elif not only_cont and did_type != 'DATASET': continue
                    to_delete[scope].add(did)

    # Filter the datasets based on the existing copies requirement
    to_delete = filter_datasets_by_existing_copies(to_delete, existing_copies_req)

    # ==================================================== #
    # ================= Delete the datasets ============== #
    # ==================================================== #
    # Keep track of the deletions (what, where, ruleid)
    actual_deletion_summary = defaultdict(lambda: defaultdict(dict))
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    ndeleted, totalsize_del = 0, 0

    # Prepare output files for monitoring
    dids_monit_file = open(f'{outdir}/monit_deletion_dids_{now}.txt', 'w')
    ruleid_monit_file = open(f'{outdir}/monit_deletion_ruleids_{now}.txt', 'w')

    # Prepare in case a dataset has no rules
    no_valid_rules = True

    # Loop over the scopes
    for scope, dids in to_delete.items():
        # Loop over the datasets in scope
        for did in dids:

            # Find the rules to delete and rses to delete them from
            rule_ids_rses_zip = get_ruleids_to_delete(did, rses_to_delete_from, args.rses, scope)

            # Try to get the size of the dataset, use as proxy to skip datasets
            # found from a task but not existing on Rucio...
            try:
                totalsize_del += dataset_size(did, scope, didcl)
            except rucio.common.exception.DataIdentifierNotFound:
                print("WARNING:: Dataset not found in Rucio: ", did, "Skipping...")
                continue

            # Loop over the rules to delete and rse to delete them from
            for ruleid, rse in rule_ids_rses_zip:
                # If we are here, at least one rule was found for the dataset
                no_valid_rules = False

                # Tell the user what we are doing
                print("INFO:: Deleting rules for dataset: ", did)
                print(f"INFO:: Deleting rule ID {ruleid} on RSE {rse}")

                # Only really add the rule if --submit is used
                if args.submit: delete_rule(ruleid)

                # Write to monitoring scripts
                dids_monit_file.write(f"{scope}:{did}\n")
                ruleid_monit_file.write(ruleid+'\n')
                # Keep track of number of rule deletions
                ndeleted += 1
                # Keep track of what we deleted exactly
                actual_deletion_summary[did][rse]['ruleid'] = ruleid

            # Tell the user if no rules were found for the dataset
            if no_valid_rules:
                print("WARNING:: No rules to delete for dataset: ", did)
                continue
    # Close the files for monitoring
    dids_monit_file.close()
    ruleid_monit_file.close()

    # Dump the deletion summary to a json file
    with open(f'{outdir}/datasets_to_delete_{now}.json', 'w') as f:
        json.dump(actual_deletion_summary, f, indent=4)

    # Summarise to user
    print("INFO:: TOTAL NUMBER OF RULES TO DELETE RULES FOR: ", ndeleted)
    good_units_size = bytes_to_best_units(totalsize_del)
    print("INFO:: TOTAL Size OF DATASETS TO DELETE RULES FOR: ", good_units_size[0], good_units_size[1])

    # Remove the monit file if --submit is not used
    if not args.submit:
        print("INFO:: --submit not used, so not deleting rules. Deleting ruleid monit file")
        os.unlink(f'{outdir}/monit_deletion_ruleids_{now}.txt')

if __name__ == "__main__":  run()
