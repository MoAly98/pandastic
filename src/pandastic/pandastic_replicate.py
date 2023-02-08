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

rcl = rucio_client.ruleclient.RuleClient()
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
                      'tobekilled', 'ready', 'pending', 'exhausted']
def argparser():
    '''
    Method to parse the arguments for the script.
    '''
    parser = argparse.ArgumentParser("This is used to replicate datasets using RUCIO client")
    parser.add_argument('-s', '--regex',              type=str,   required=True,   nargs='+',            help=_h_regex)
    parser.add_argument('-r', '--rseto',              type=str,   required=True,   nargs='+',            help=_h_rses)
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

def get_datasets_from_jobs(jobs, regexes, cont_type, did_regex, repl_cont):
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
    repl_cont: bool
        Should the code replicate containers ? If not, individual datasets will be replicated

    Returns
    -------
    datasets: defaultdict(set)
        A dictionary of datasets to replicate for with keys being scope and values being a set of dataset names
    '''

    if cont_type == 'OUT':  look_for_type = 'output'
    else:   look_for_type = 'input'

    # List to hold names of DIDs to replicate
    datasets = defaultdict(set)
    hated_containers = set()
    for job in jobs:
        taskname = job.get("taskname")
        if all(re.match(rf'{rgx}', taskname) is None for rgx in regexes):    continue

        for ds in job.get("datasets"):

            # Get the type of the dataset
            dstype = ds.get("type")
            # Skip the type of dataset we don't care about
            if(dstype != look_for_type):  continue
            # Skip ds if it has no files to avoid rucio headaches
            if ds.get("nfilesfinished") < 1 : continue
            # Get the name of the dataset
            dsname = ds.get("datasetname")

            # Get the name of the dataset parent container
            contname = ds.get("containername")
            # For optimisation, skip the dataset if it's in a hated container
            # === Note datasets live in containers,
            # multiple datasets can live in the same container ===
            if contname in hated_containers:    continue


            # Get the scope from the dsname
            if ':' in dsname:
                scope = dsname.split(':')[0]
            else:
                scope = '.'.join(dsname.split('.')[:2])

            if not repl_cont:
                # Check if the dataset name matches the DID regex
                if did_regex is not None:
                    # Skip the dataset if it doesn't match the DID regex
                    if re.match(did_regex, dsname) is None: continue
                # If we are not replicating containers, add the dataset
                datasets[scope].add(dsname)
            else:
                # Check if the dataset name matches the DID regex
                if did_regex is not None:
                    # Skip the dataset if the container doesn't match the DID regex
                    if re.match(did_regex, contname) is None:
                        hated_containers.add(contname)
                        continue

                # If we are replicating containers, add the container
                datasets[scope].add(contname)

    return datasets

def filter_datasets_to_replicate(
    datasets : defaultdict(set),
    rseto: str,
    rules_and_replicas_req: RulesAndReplicasReq,
):
    '''
    Method to filter the datasets to replicate based on the rules and replicas requirements.

    Parameters
    ----------
    datasets: defaultdict(set)
        Dict with keys as scopes and values as sets of datasets to replicate
    rseto: str
        RSE to replicate to
    rules_and_replicas_req: RulesAndReplicasReq
        Rules and replicas requirements

    Returns
    -------
    filtered_datasets: list
        Dict with keys as scopes and values as sets of datasets to replicate
    '''

    filtered_datasets = defaultdict(set)
    for scope, dses in datasets.items():
        for ds in dses:

            # First need to check if thee dataset has a rule on the RSE we want to replicate to
            rseto_to_remove =[]
            for rse in rseto:
                ds_has_rule_on_rseto = has_rule_on_rse(ds, scope, rse, didcl)

                if ds_has_rule_on_rseto:
                    print(f"WARNING: Dataset {ds} already has a rule on the RSE {rse} we want to replicate to. Skipping replication to RSE.")
                    rseto_to_remove.append(rse)
                    continue

            for removal in rseto_to_remove:
                rseto.remove(removal)

            if rseto == []:
                print(f"WARNING: Dataset {ds} already has a rule on all the RSEs we want to replicate to. Skipping dataset.")
                continue

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
        rule = rcl.add_replication_rule([{'scope':scope, 'name': ds.replace('/','')}], 1, rse_expression, lifetime = lifetime)
        print(f'INFO:: DS = {ds} \n RuleID: {rule[0]}')
        return rule[0]

    except rucio.common.exception.DuplicateRule as de:
        print(f"WARNING:: Duplication already done for \n {ds} \n to {rse_expression} ...  skipping!")
        return None

    except rucio.common.exception.ReplicationRuleCreationTemporaryFailed as tfe:
        print(f"WARNING:: Duplication not currently possible for \n {ds} \n to {rse_expression} ...  skipping, try again later!")
        return None

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

    # Get the RSEs to replicate to from available RSEs using regexes (if any)
    rses_to_replicate_to = set()

    for rse in args.rseto:
        rses_to_replicate_to |= get_rses_from_regex(rse, rsecl)

    assert len(rses_to_replicate_to) > 0, "No RSEs found to replicate to. Exiting."

    to_repl = defaultdict(set)

    if args.fromfiles is not None:
        # Get the datasets from the files
        all_datasets = get_datasets_from_files(args.fromfiles)
        for ds in all_datasets:
            scope, did = ds.split(':')
            if not any(re.match(regex, did) for regex in regexes):
                continue
            to_repl[scope].add(did.strip())

    if usetasks is not None:

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

            # Fast track -- skip datasets with rules on all of the regexed rses we replicate to
            if not all(has_rule_on_rse(did, scope, rse, didcl) for rse in args.rseto):
                continue

            did_type = didcl.get_metadata(scope, did.replace('/','')).get('did_type')
            if only_cont and did_type != 'CONTAINER': continue
            elif not only_cont and did_type != 'DATASET': continue
            to_repl[scope].add(did)

    # Filter the datasets to replicate based on the existing copies
    to_repl = filter_datasets_to_replicate(to_repl, args.rseto, existing_copies_req)

    print("INFO:: EXPECTED NUMBER OF DATASETS TO REPLICATE: ", len(to_repl))

    # === Replicate the datasets === #
    # Keep track of the replications (what, where, ruleid)
    actual_replication_summary = defaultdict(lambda: defaultdict(dict))

    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    nreplicated, totalsize_repl = 0, 0
    # Write the ruleids to a file so we can monitor the deletions
    dids_monit_file = open(f'{outdir}/monit_replication_dids_{now}.txt', 'w')
    ruleid_monit_file = open(f'{outdir}/monit_replication_ruleids_{now}.txt', 'w')
    for scope, dids in to_repl.items():
        for did in dids:

            try:
                totalsize_repl += dataset_size(did, scope, didcl)
            except rucio.common.exception.DataIdentifierNotFound:
                print("WARNING:: Dataset not found in Rucio: ", did, "Skipping...")
                continue

            for rse in rses_to_replicate_to:
                print("INFO:: Replicating rules for dataset: ", did)
                print("INFO:: Replicating to RSE: ", rse)
                # Only really add the rule if --submit is used
                if args.submit:
                    ruleid = add_rule(did, rse, args.lifetime, scope)
                    ruleid_monit_file.write(ruleid+'\n')
                else:
                    ruleid = 'NOT_SUBMITTED'

                actual_replication_summary[did][rse]['ruleid'] = ruleid
                dids_monit_file.write(f"{scope}:{did}\n")

                nreplicated += 1

    dids_monit_file.close()
    ruleid_monit_file.close()

    print("INFO:: TOTAL NUMBER OF DATASET RULES TO CREATE: ", nreplicated)
    good_units_size = bytes_to_best_units(totalsize_repl)
    print("INFO:: TOTAL Size OF DATASETS TO REPLICATE: ", good_units_size[0], good_units_size[1])

    # Remove the monit file if --submit is not used
    if not args.submit:
        print("INFO:: --submit not used, so not replicating. Deleting ruleid monit file")
        os.unlink(f'{outdir}/monit_replication_ruleids_{now}.txt')

    # Dump the replication summary to a json file
    with open(f'{outdir}/datasets_to_replicate_{now}.json', 'w') as f:
        json.dump(actual_replication_summary, f, indent=4)

if __name__ == "__main__":  run()
