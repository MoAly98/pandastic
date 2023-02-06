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
from tools import (has_replica_on_rse, has_rule_on_rse, get_rses_from_regex, RulesAndReplicasReq)

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

# ===============  Arg Parser Choices ===============================
_choices_usetasks =  ['submitted', 'defined', 'activated',
                      'assigned', 'starting', 'running',
                      'merging', 'finished', 'failed',
                      'cancelled', 'holding', 'transferring',
                      'closed', 'aborted', 'unknown', 'all',
                      'throttled', 'scouting', 'scouted', 'done',
                      'tobekilled', 'ready', 'pending', 'exhausted']
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
    parser.add_argument('--usetask',                  nargs='+', choices = _choices_usetasks,            help=_h_usetask)
    parser.add_argument('--type',                     type=str,   choices=['OUT','IN'],                  help=_h_type)
    parser.add_argument('--containers',               action='store_true',                               help=_h_containers)
    parser.add_argument('--submit',                   action='store_true',                               help=_h_submit)

    return parser.parse_args()


def get_datasets_from_jobs(jobs, regexes, cont_type, did_regex, del_cont):
    '''
    Method to get the datasets from the jobs.

    Parameters
    ----------
    jobs: list
        List of jobs to search through
    regexes: list
        List of regexes to match the taskname of the jobs
    did_regex: str
        Regex to match the dataset names
    cont_type: str
        Type of dataset to look for. Either 'IN' or 'OUT'
    del_cont: bool
        Should the code delete only container rules? if False, delete individual dataset rules

    Returns
    -------
    datasets: list
        Preliminary list of datasets to delete rules for
    '''

    if cont_type == 'OUT':  look_for_type = 'output'
    else:   look_for_type = 'input'

    # List to hold names of DIDs to delete rules for
    datasets = set()
    task_to_nfiles_out = defaultdict(lambda: defaultdict(int))
    task_to_saved_ds   = defaultdict(list)
    hated_containers = set()
    for job in jobs:
        taskname = job.get("taskname")
        if all(re.match(rf'{rgx}', taskname) is None for rgx in regexes):    continue


        for ds in job.get("datasets"):

            # Get the type of the dataset
            dstype = ds.get("type")
            # Get the name of the dataset
            dsname = ds.get("datasetname")
            # Skip ds if it has no files to avoid rucio headaches
            if ds.get("nfilesfinished") < 1 : continue
            # Get the name of the dataset parent container
            contname = ds.get("containername")
            # Get the scope from the container name
            scope = ".".join(contname.split(".")[:2])

            # For optimisation, skip the dataset if it's in a hated container
            # === Note datasets live in containers,
            # multiple datasets can live in the same container ===
            if contname in hated_containers:    continue

            # Number of files in the dataset
            ds_nfiles = len(list(didcl.list_files(scope, dsname.split(':')[-1])))
            # Skip the type of dataset we don't care about
            if(dstype != look_for_type):
                task_to_nfiles_out[taskname][dstype] += ds_nfiles
                continue

            task_to_nfiles_out[taskname][dstype] += ds_nfiles

            # Also skip if another dataset added this container
            if contname in datasets:    continue

            if not del_cont:
                # Check if the dataset name matches the DID regex
                if did_regex is not None:
                    # Skip the dataset if it doesn't match the DID regex
                    if re.match(did_regex, dsname) is None: continue

                task_to_saved_ds[taskname].append(dsname)
                # If we are not deleting containers rules, add the dataset
                datasets.add(dsname)
            else:
                # Check if the dataset name matches the DID regex
                if did_regex is not None:
                    # Skip the dataset if the container doesn't match the DID regex
                    if re.match(did_regex, contname) is None:
                        hated_containers.add(contname)
                        continue

                task_to_saved_ds[taskname].append(contname)
                # If we are deleting containers rules, add the container
                datasets.add(contname)

    # for task, dstype_to_nfiles in task_to_nfiles_out.items():
    #     if dstype_to_nfiles['input'] != dstype_to_nfiles['output']:
    #         print(f"WARNING:: Task {task} has different number of input and output files. IN = {dstype_to_nfiles['input']}, OUT = {dstype_to_nfiles['output']}")
    #     for ds in task_to_saved_ds[task]:
    #         if ds in datasets:
    #             print(f"WARNING:: Skipping the dataset {ds} for that reason...")
    #             datasets.remove(ds)
    return datasets

def filter_datasets_to_delete(
    datasets : list,
    rses: str,
    rules_and_replicas_req: RulesAndReplicasReq,
):
    '''
    Method to filter the datasets to delete based on the rules and replicas requirements.

    Parameters
    ----------
    datasets: list
        List of datasets to delete
    rses: str
        RSE to delete from
    scope: str
        Scope of the datasets
    rules_and_replicas_req: RulesAndReplicasReq
        Rules and replicas requirements

    Returns
    -------
    filtered_datasets: list
        List of datasets to delete rules for that pass the rules and replicas requirements
    '''

    filtered_datasets = set()
    for ds in datasets:

        # Get the scope from the dataset using didclient
        scope = ".".join(ds.split(".")[:2])


        # First need to check if thee dataset has a rule on the RSE we want to delete from
        rsefrom_to_remove =[]

        # for rse in rses:

        #     ds_has_rule_on_rsefrom = has_rule_on_rse(ds, scope, rse, didcl)

        #     if not ds_has_rule_on_rsefrom:
        #         print(f"WARNING: Dataset {ds} does not have a rule on the RSE {rse} we want to delete from. Skipping deletion from RSE.")
        #         rsefrom_to_remove.append(rse)
        #         continue

        # for removal in rsefrom_to_remove:
        #     rses.remove(removal)

        # if rses == []:
        #     print(f"WARNING: Dataset {ds} doesn't have a rule on any of the RSEs we want to delete from. Skipping dataset.")
        #     continue

        # Now we check if the dataset has a rule on the RSE that is required to have a rule on it

        req_existing_rule_exists = True # Set to True by default so that if no RSEs are specified, the check passes
        if rules_and_replicas_req.rule_on_rse is not None:
            # Set to False so that if RSEs are specified, and none of them have a rule, the check fails
            req_existing_rule_exists = False
            for rse in rules_and_replicas_req.rule_on_rse:
                req_existing_rule_exists = has_rule_on_rse(ds, scope, rse, didcl)
                if req_existing_rule_exists: break

        # Now we check if the dataset has a replica on the RSE that is required to have a replica on it

        req_existing_replica_exists = True # Set to True by default so that if no RSEs are specified, the check passes
        if rules_and_replicas_req.replica_on_rse is not None:
            # Set to False so that if RSEs are specified, and none of them have a replica, the check fails
            req_existing_replica_exists = False
            for rse in rules_and_replicas_req.replica_on_rse:
                req_existing_replica_exists = has_replica_on_rse(ds, scope, rse, replicacl)
                if req_existing_replica_exists: break

        # If user is asking for either a rule or a replica but necessarily both, then we check if either exists
        if rules_and_replicas_req.rule_or_replica_on_rse:
            if not (req_existing_rule_exists or req_existing_replica_exists):
                print(f"WARNING: Dataset {ds} does not satisfy existing rules and replicas requirements. Skipping.")
                continue
        # If user is asking for both a rule and a replica, then we check if both exist
        else:
            if not (req_existing_rule_exists and req_existing_replica_exists):
                print(f"WARNING: Dataset {ds} does not satisfy existing rules and replicas requirements. Skipping.")
                continue

        filtered_datasets.add(ds)

    return filtered_datasets

def get_ruleids_to_delete(did, rses_to_delete_from):
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

    # Get the scope from the dataset using didclient
    scope = ".".join(did.split(".")[:2])

    # Get the rules for the dataset
    rules = didcl.list_did_rules(scope, did.replace('/',''))

    # Get the rule IDs to delete
    for rule in rules:
        rse_for_rule = rule.get('rse_expression')
        rule_id = rule['id']
        if rse_for_rule in rses_to_delete_from:
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
        rulecl.delete_replication_rule(rule_id, purge_replicas=True)
    except:
        print(f"WARNING:: Rule deletion failed for rule ID {ruleid} ...  skipping!")

    # except rucio.common.exception.DuplicateRule as de:
    #     print(f"WARNING:: Duplication already done for \n {ds} \n to {rse_expression} ...  skipping!")

    #     return None

    # except rucio.common.exception.ReplicationRuleCreationTemporaryFailed as tfe:
    #     print(f"WARNING:: Duplication not currently possible for \n {ds} \n to {rse_expression} ...  skipping, try again later!")
    #     return None

def run():
    '''
    Main method
    '''
    # ========  Get the arguments ============ #
    args = argparser()

    regexes   = args.regex

    only_cont = args.containers
    scopes    = args.scopes

    # If specified, make sure the dataset has a rule/replica/both on RSEs with given regex
    existing_copies_req = RulesAndReplicasReq(args.rule_on_rse, args.replica_on_rse, args.rule_or_replica_on_rse)

    to_delete = set()

    if args.usetask is not None:

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
            print(f"INFO:: Looking for tasks which are DONE on the grid for user {user} in the last {days} days")
            # Find all PanDA jobs that are done for the user and period specified
            _, url, tasks = queryPandaMonUtils.query_tasks( username=user, days=days, status='done')

            # Tell the user the search URL if they want to look
            print(f"INFO:: PanDAs query URL: {url}")

            # Workout the containers/datasets to delete rules for from the PanDA tasks
            to_delete |= get_datasets_from_jobs(tasks, regexes, ds_type, did_regex, only_cont)

    else:
        # =============
        # If no association with PanDA tasks, just use the regexes over all rucio datasets in given scopes
        # =============

        if args.grid_user != pbook.username:
            print("WARNING:: You are not using --usetask, so --grid_user is useless. Ignoring --grid_user")

        if args.days is not None:
            print("WARNING:: You are not using --usetask, so --days is useless. Ignoring --days")
        if args.type is not None:
            print("WARNING:: You are not using --usetask, so --type is useless. Ignoring --type")
        if args.did is not None:
            print("WARNING:: You are not using --usetask, so --did is useless. Ignoring --did")

        # if --usetask is not used, the scopes must be specified
        assert scopes is not None, "ERROR:: --scopes must be specified if --usetask is not used"

        for scope in scopes:
            for regex in regexes:
                for did in didcl.list_dids(scope, {'name': regex.replace('.*','*').replace('/','')}):
                    did_type = didcl.get_metadata(scope, did.replace('/','')).get('did_type')
                    if only_cont and did_type != 'CONTAINER': continue
                    elif not only_cont and did_type != 'DATASET': continue
                    to_delete.add(did)

    # Filter the datasets to delete rules for based on the existing copies
    to_delete = list(filter_datasets_to_delete(to_delete, args.rses, existing_copies_req))

    print("INFO:: TOTAL NUMBER OF DATASETS TO DELETE RULES FOR: ", len(to_delete))

    # Get the RSEs to delete rules from, from available RSEs using regexes (if any)
    rses_to_delete_from = set()
    for rse in args.rses:
        rses_to_delete_from |= get_rses_from_regex(rse, rsecl)

    # === Delete the datasets === #
    # Keep track of the deletions (what, where, ruleid)
    actual_deletion_summary = defaultdict(lambda: defaultdict(dict))

    now = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Write the ruleids to a file so we can monitor the deletions
    with open(f'monit_deletion_output_{now}.txt', 'w') as monitf:
        for did in to_delete:
            print("INFO:: Deleting rules for dataset: ", did)

            rule_ids_rses_zip = get_ruleids_to_delete(did, rses_to_delete_from)

            for ruleid, rse in rule_ids_rses_zip:
                print(f"INFO:: Deleting rule ID {ruleid} on RSE {rse}")

                # Only really add the rule if --submit is used
                if args.submit:
                    delete_rule(ruleid)
                    monitf.write(r+'\n')

                actual_deletion_summary[did][rse]['ruleid'] = ruleid

    # Remove the monit file if --submit is not used
    if not args.submit:
        os.remove(f'monit_deletion_output_{now}.txt')
    # Dump the deletion summary to a json file
    with open('datasets_to_delete.json', 'w') as f:
        json.dump(actual_deletion_summary, f, indent=4)

if __name__ == "__main__":  run()
