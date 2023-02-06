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
from tools import (has_replica_on_rse, has_rule_on_rse, get_rses_from_regex)

# ===============  Rucio Clients ================

rcl = rucio_client.ruleclient.RuleClient()
didcl = rucio_client.didclient.DIDClient()
rsecl = rucio_client.rseclient.RSEClient()
replicacl = rucio_client.replicaclient.ReplicaClient()

# ===============  ArgParsing  ===================================
# ===============  Arg Parser Help ===============================
_h_regex    = 'A regex in the panda *taskname* to be used to find the jobs to replicate datasets from'

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

    return parser.parse_args()

'''
    regex: str
        Regex to match the taskname of the jobs if --usetask is used.
        Otherwise will be used to match the dataset name from list of
        datasets for the scopes provided.
    cont_type: str
        Type of dataset to look for. Either 'IN' or 'OUT'
'''

class RulesAndReplicasReq:
    '''
    Class to hold the rules and replicas requirements.
    '''
    def __init__(self, rule_on_rse, replica_on_rse, rule_or_replica_on_rse):
        self.rule_on_rse = rule_on_rse
        self.replica_on_rse = replica_on_rse
        self.rule_or_replica_on_rse = rule_or_replica_on_rse

    def __repr__(self):
        return f"RulesAndReplicasReq(rule_on_rse={self.rule_on_rse}, replica_on_rse={self.replica_on_rse}, rule_or_replica_on_rse={self.rule_or_replica_on_rse})"
    def __str__(self):
        return f"RulesAndReplicasReq(rule_on_rse={self.rule_on_rse}, replica_on_rse={self.replica_on_rse}, rule_or_replica_on_rse={self.rule_or_replica_on_rse})"


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

    Returns
    -------
    datasets: list
        Preliminary list of datasets to replicate
    '''

    if cont_type == 'OUT':  look_for_type = 'output'
    else:   look_for_type = 'input'

    # List to hold names of DIDs to replicate
    datasets = set()
    hated_containers = set()
    for job in jobs[:1]:
        taskname = job.get("taskname")
        if all(re.match(rf'{rgx}', taskname) is None for rgx in regexes):    continue

        for ds in job.get("datasets"):

            # Get the type of the dataset
            dstype = ds.get("type")
            # Skip the type of dataset we don't care about
            if(dstype != look_for_type):  continue

            # Get the name of the dataset
            dsname = ds.get("datasetname")

            # Get the name of the dataset parent container
            contname = ds.get("containername")
            # For optimisation, skip the dataset if it's in a hated container
            if contname in hated_containers:    continue

            # Get the scope from the container name
            scope = ".".join(contname.split(".")[:2])

            if not repl_cont:
                # Check if the dataset name matches the DID regex
                if did_regex is not None:
                    # Skip the dataset if it doesn't match the DID regex
                    if re.match(did_regex, dsname) is None: continue
                # If we are not replicating containers, add the dataset
                datasets.add(dsname)
            else:
                # Check if the dataset name matches the DID regex
                if did_regex is not None:
                    # Skip the dataset if the container doesn't match the DID regex
                    if re.match(did_regex, contname) is None:
                        hated_containers.add(contname)
                        continue

                # If we are replicating containers, add the container
                datasets.add(contname)

    return datasets

def filter_datasets_to_replicate(
    datasets : list,
    rseto: str,
    rules_and_replicas_req: RulesAndReplicasReq,
):
    '''
    Method to filter the datasets to replicate based on the rules and replicas requirements.

    Parameters
    ----------
    datasets: list
        List of datasets to replicate
    rseto: str
        RSE to replicate to
    scope: str
        Scope of the datasets
    rules_and_replicas_req: RulesAndReplicasReq
        Rules and replicas requirements

    Returns
    -------
    filtered_datasets: list
        List of datasets to replicate that pass the rules and replicas requirements
    '''

    filtered_datasets = set()
    for ds in datasets:

        # Get the scope from the dataset using didclient
        scope = ".".join(ds.split(".")[:2])


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

        filtered_datasets.add(ds)

    return filtered_datasets

def add_rule(ds, rse, lifetime):
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

    Returns
    -------
    rule: str
        The rule ID if the rule was successfully added, else None
    '''
    # RSE Definition
    rse_attrs = rsecl.list_rse_attributes(rse_expression)
    rse_tier  = rse_attrs['tier']
    rse_cloud = rse_attrs['cloud']
    rse_site  = rse_attrs['site']
    rse_type  = rse_attrs['type']
    rse_bool  = f'tier={rse_tier}&type={rse_type}&cloud={rse_cloud}&site={rse_site}'

    # SCOPE
    scope = ".".join(ds.split(".")[:2])

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

    to_repl = set()

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

            # Workout the containers/datasets to replicate from the PanDA tasks
            to_repl |= get_datasets_from_jobs(tasks, regexes, ds_type, did_regex, only_cont)

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
                    to_repl.add(did)

    # Filter the datasets to replicate based on the existing copies
    to_repl = list(filter_datasets_to_replicate(to_repl, args.rseto, existing_copies_req))

    print("INFO:: TOTAL NUMBER OF DATASETS TO REPLICATE: ", len(to_repl))

    # Get the RSEs to replicate to from available RSEs using regexes (if any)
    rses_to_replicate_to = set()
    for rse in args.rseto:
        rses_to_replicate_to |= get_rses_from_regex(rse, rsecl)

    # === Replicate the datasets === #
    # Keep track of the replications (what, where, ruleid)
    actual_replication_summary = defaultdict(lambda: defaultdict(dict))

    now = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Write the ruleids to a file so we can monitor the replication
    with open(f'monit_replicate_output_{now}.txt', 'w') as monitf:
        for did in to_repl:
            print("INFO:: Replicating dataset: ", did)
            for rse in rses_to_replicate_to:
                print("INFO:: Replicating to RSE: ", rse)

                # Only really add the rule if --submit is used
                if args.submit:
                    ruleid = add_rule(did, rse, args.lifetime)
                    if ruleid is not None:
                        monitf.write(r+'\n')
                else:
                    ruleid = 'DRY'
                actual_replication_summary[did][rse]['ruleid'] = ruleid
    # Remove the monit file if --submit is not used
    if not args.submit:
        os.remove(f'monit_replicate_output_{now}.txt')
    # Dump the replication summary to a json file
    with open('datasets_to_replicate.json', 'w') as f:
        json.dump(actual_replication_summary, f, indent=4)

if __name__ == "__main__":  run()
