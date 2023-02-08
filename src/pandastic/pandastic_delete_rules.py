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
from tools import (has_replica_on_rse, has_rule_on_rse, get_rses_from_regex, RulesAndReplicasReq,
                   dataset_size, bytes_to_best_units, progress_bar, merge_dicts)

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
    parser.add_argument('--fromfiles',                type=str,   nargs='+',                             help=_h_outdir)
    return parser.parse_args()


def get_datasets_from_jobs(jobs, regexes, cont_type, did_regex, del_cont, matchfiles, scopes):
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
    matchfiles: bool
        Should the code delete containers/datasets if number of input and output files match for the associated task?
    Returns
    -------
    datasets: list
        Preliminary list of datasets to delete rules for
    '''

    if cont_type == 'OUT':  look_for_type = 'output'
    else:   look_for_type = 'input'

    # List to hold names of DIDs to delete rules for
    datasets = defaultdict(set)
    task_to_nfiles_out = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    task_to_saved_ds   = defaultdict(lambda: defaultdict(list))
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

            # Get the scope from the dsname
            if ':' in dsname:
                scope = dsname.split(':')[0]
            else:
                scope = '.'.join(dsname.split('.')[:2])

            # For optimisation, skip the dataset if it's in a hated container
            # === Note datasets live in containers,
            # multiple datasets can live in the same container ===
            if contname in hated_containers:    continue
            if matchfiles:
                try:
                    # Number of files in the dataset
                    ds_nfiles = len(list(didcl.list_files(scope, dsname.split(':')[-1])))
                    # Skip the type of dataset we don't care about
                    if(dstype != look_for_type):
                        task_to_nfiles_out[scope][taskname][dstype] += ds_nfiles
                        continue
                    task_to_nfiles_out[scope][taskname][dstype] += ds_nfiles
                except rucio.common.exception.DataIdentifierNotFound as e:
                    print(f"Dataset {dsname} not found in Rucio -- skipping")
                    continue



            # # Also skip if another dataset added this container
            # if contname in datasets:    continue
            if not del_cont:
                # Check if the dataset name matches the DID regex
                if did_regex is not None:
                    # Skip the dataset if it doesn't match the DID regex
                    if re.match(did_regex, dsname) is None: continue
                task_to_saved_ds[scope][taskname].append(dsname)

                # If we are not deleting containers rules, add the dataset
                datasets[scope].add(dsname)
            else:
                # Check if the dataset name matches the DID regex
                if did_regex is not None:
                    # Skip the dataset if the container doesn't match the DID regex
                    if re.match(did_regex, contname) is None:
                        hated_containers.add(contname)
                        continue

                task_to_saved_ds[scope][taskname].append(contname)
                # If we are deleting containers rules, add the container
                datasets[scope].add(contname)
    if matchfiles:
        for scope, task_dstype_to_nfiles in task_to_nfiles_out.items():
            for task, dstype_to_nfiles in task_dstype_to_nfiles.items():
                if dstype_to_nfiles['input'] != dstype_to_nfiles['output']:
                    print(f"WARNING:: Task {task} has different number of input and output files. IN = {dstype_to_nfiles['input']}, OUT = {dstype_to_nfiles['output']}")
                for ds in task_to_saved_ds[scope][task]:
                    if ds in datasets[scope]:
                        print(f"WARNING:: Skipping the dataset {ds} for that reason...")
                        datasets[scope].remove(ds)
    return datasets

def filter_datasets_to_delete(
    datasets : 'defaultdict(set)',
    rses: str,
    rules_and_replicas_req: RulesAndReplicasReq,
):
    '''
    Method to filter the datasets to delete based on the rules and replicas requirements.

    Parameters
    ----------
    datasets: defaultdict(set)
        dict with keys as scopes and values as sets of datasets to delete rules for
    rses: str
        RSE to delete from
    scope: str
        Scope of the datasets
    rules_and_replicas_req: RulesAndReplicasReq
        Rules and replicas requirements

    Returns
    -------
    filtered_datasets: defaultdict(set)
        dictionary with keys as scopes and values as sets of datasets to delete rules for
    '''

    filtered_datasets = defaultdict(set)
    for scope, dses in datasets.items():
        for ds in dses:
            # First need to check if thee dataset has a rule on the RSE we want to delete from
            rsefrom_to_remove =[]

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

            filtered_datasets[scope].add(ds)

    return filtered_datasets

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

def get_datasets_from_files(files):
    '''
    Method to get the datasets from a list of files

    Parameters
    ----------
    files: list
        list of files containing datasets

    Returns
    -------
    all_datasets: list
        list of datasets
    '''
    all_datasets = []
    for f in files:
        with open(f,'r') as f:
            datasets = f.readlines()
        all_datasets.extend(datasets)
    return all_datasets

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
        all_datasets = get_datasets_from_files(args.fromfiles)
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
            to_delete = merge_dicts(to_delete, get_datasets_from_jobs(tasks, regexes, ds_type, did_regex, only_cont, args.matchfiles, scopes))
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
            print("INFO:: Looking for datasets in scope", scope, "matching regexes")
            for regex in regexes:
                print("INFO:: Looking for datasets matching regex", regex)
                dids = list(didcl.list_dids(scope, {'name': regex.replace('.*','*').replace('/','')}))
                # Progress Bar
                progress_bar(len(dids), 0)
                for i, did in enumerate(dids):

                    # Progresss Bar
                    if(len(dids) > 100):
                        if (i+1)%100 == 0: progress_bar(len(dids), i+1, msg=f'Progress for collecting dids matching regex {regex}')
                    elif len(dids) > 10 and len(dids) <= 100:
                        if (i+1)%10 == 0:  progress_bar(len(dids), i+1, msg=f'Progress for collecting dids matching regex {regex}')
                    else:
                        progress_bar(len(dids), i+1, msg=f'Progress for collecting dids matching regex {regex}')

                    # Fast track -- skip datasets with no rrules on any of the regexed rses we delete from
                    if not any(has_rule_on_rse(did, scope, rse, didcl) for rse in args.rses):
                        continue

                    # Only process containers/datasets based on user specified option
                    did_type = didcl.get_metadata(scope, did.replace('/','')).get('did_type')
                    if only_cont and did_type != 'CONTAINER': continue
                    elif not only_cont and did_type != 'DATASET': continue
                    to_delete[scope].add(did)

    for scope, to_del_dses in to_delete.items():
        print("INFO:: Found", len(list(to_del_dses)), "datasets to delete rules for, from scope", scope)
        # Filter the datasets to delete rules for based on the existing copies
        to_delete = filter_datasets_to_delete(to_delete, args.rses, existing_copies_req)
        print("INFO:: Found", len(list(to_del_dses)), "datasets to delete rules for after filter from scope", scope)


    # === Delete the datasets === #
    # Keep track of the deletions (what, where, ruleid)
    actual_deletion_summary = defaultdict(lambda: defaultdict(dict))

    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    ndeleted, totalsize_del = 0, 0
    # Write the ruleids to a file so we can monitor the deletions
    dids_monit_file = open(f'{outdir}/monit_deletion_dids_{now}.txt', 'w')
    ruleid_monit_file = open(f'{outdir}/monit_deletion_ruleids_{now}.txt', 'w')
    for scope, dids in to_delete.items():
        for did in dids:
            rule_ids_rses_zip = get_ruleids_to_delete(did, rses_to_delete_from, args.rses, scope)
            for ruleid, rse in rule_ids_rses_zip:

                print("INFO:: Deleting rules for dataset: ", did)
                print(f"INFO:: Deleting rule ID {ruleid} on RSE {rse}")

                # Only really add the rule if --submit is used
                if args.submit:
                    delete_rule(ruleid)

                dids_monit_file.write(f"{scope}:{did}\n")
                ruleid_monit_file.write(ruleid+'\n')
                ndeleted += 1
                totalsize_del += dataset_size(did, scope, didcl)
                actual_deletion_summary[did][rse]['ruleid'] = ruleid

    dids_monit_file.close()
    ruleid_monit_file.close()

    print("INFO:: TOTAL NUMBER OF RULES TO DELETE RULES FOR: ", ndeleted)
    good_units_size = bytes_to_best_units(totalsize_del)
    print("INFO:: TOTAL Size OF DATASETS TO DELETE RULES FOR: ", good_units_size[0], good_units_size[1])

    # Remove the monit file if --submit is not used
    if not args.submit:
        print("INFO:: --submit not used, so not deleting rules. Deleting monit file")
        os.unlink(f'{outdir}/monit_deletion_ruleids_{now}.txt')

    # Dump the deletion summary to a json file
    with open(f'{outdir}/datasets_to_delete_{now}.json', 'w') as f:
        json.dump(actual_deletion_summary, f, indent=4)

if __name__ == "__main__":  run()
