from tools import (has_replica_on_rse, has_rule_on_rse, RulesAndReplicasReq,)
from collections import defaultdict
import re
# =============================================================
# ========================  Classes  ==========================
# =============================================================
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

# =============================================================
# =============== Methods for dataset processing  =============
# =============================================================
def get_datasets_from_jobs(jobs, regexes, cont_type, did_regex, only_cont, matchfiles = False, didcl = None):
    '''
    Method to get datasets associated to GRID jobs.

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
    only_cont: bool
        Should the did regex match only containers? if False, did regex will match datasets
    matchfiles: bool
        Should the code process containers/datasets only if number of input and output files match for the associated task?

    Returns
    -------
    datasets: defaultdict(set)
        A dictionary of datasets to process with keys being scope and values being a set of dataset names
    '''

    # If we're matching files, we need a DID client to get the number of files in the dataset
    if matchfiles: assert didcl is not None, "Must provide a DID client to check input/output file counts"

    # Get the type of dataset to look for
    if cont_type == 'OUT':  look_for_type = 'output'
    else:   look_for_type = 'input'

    # List to hold names of DIDs to be processed
    datasets = defaultdict(set)

    # Dictionary to hold the number of files in the input and output datasets (scope: taskname: {input: nfiles, output: nfiles})
    task_to_nfiles_out = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    # Dictionarty to match a task to the datasets saved from it (scope: taskname: [dsnames])
    task_to_saved_ds   = defaultdict(lambda: defaultdict(list))

    # SET of containers we don't want to process
    hated_containers = set()

    # Loop over the jobs
    for job in jobs:

        # Get the name of the task
        taskname = job.get("taskname")
        # Skip the job if it doesn't match the regex
        if all(re.match(rf'{rgx}', taskname) is None for rgx in regexes):    continue
        # Get the datasets associated to the job
        job_datasets = job.get("datasets")

        for ds in job_datasets:

            # Skip ds if it has no files to avoid rucio headaches
            if ds.get("nfilesfinished") < 1 : continue

            # Get the type of the dataset
            dstype = ds.get("type")
            # Get the name of the dataset
            dsname = ds.get("datasetname")
            # Get the name of the dataset parent container
            contname = ds.get("containername")

            # Get the scope from the dsname (is there a better way?)
            if ':' in dsname:   scope = dsname.split(':')[0]
            else:   scope = '.'.join(dsname.split('.')[:2])


            # === Note datasets live in containers, multiple datasets can live in the same container ===
            # Skip the dataset if we know it's container is in the hated_containers set from another dataset
            if contname in hated_containers:    continue

            # Save information needed to check if the number of input and output files match for the task
            if matchfiles:
                try:

                    # We need to remove the scope from the dataset name to get the number of files
                    # If no ":" in the name, split will return the whole name
                    ds_without_scope = dsname.split(':')[-1]
                    # Number of files in the dataset
                    ds_nfiles = len(list(didcl.list_files(scope, ds_without_scope)))
                    task_to_nfiles_out[scope][taskname][dstype] += ds_nfiles
                except rucio.common.exception.DataIdentifierNotFound as e:
                    print(f"Dataset {dsname} not found in Rucio -- skipping")
                    continue

            # Skip the type of dataset we don't care about
            if(dstype != look_for_type):    continue

            # Skip if another dataset added this container (if we are saving containers)
            if contname in datasets[scope].keys():    continue

            # Default is processing datasets
            to_process = dsname
            if only_cont:   to_process = contname

            # Check if the dataset/container name matches the DID regex
            if did_regex is not None:
                # Skip the dataset/container if it doesn't match the DID regex
                if re.match(did_regex, to_process) is None:
                    # If we are processing containers, we can optimise by
                    # adding the container to the hated_containers set
                    if only_cont:   hated_containers.add(to_process)
                    continue
            # Save the dataset/container to the mapping from task to datasets associated to it
            task_to_saved_ds[scope][taskname].append(to_process)
            # Save the dataset/container to the map from scopes to datasets to process
            datasets[scope].add(to_process)

    # Process information on number of input and output files for each task and remove
    # datasets associated with tasks that have different number of input and output files
    if matchfiles:
        # Loop over the scopes
        for scope, task_dstype_to_nfiles in task_to_nfiles_out.items():
            # Loop over the tasks
            for task, dstype_to_nfiles in task_dstype_to_nfiles.items():
                # Check if the number of input and output files match
                if dstype_to_nfiles['input'] != dstype_to_nfiles['output']:
                    print(f"WARNING:: Task {task} has different number of input and output files. IN = {dstype_to_nfiles['input']}, OUT = {dstype_to_nfiles['output']}")
                # Remove the datasets associated with the task from the list of datasets to process
                for ds in task_to_saved_ds[scope][task]:
                    if ds in datasets[scope]:
                        print(f"WARNING:: Skipping the dataset {ds} for that reason...")
                        datasets[scope].remove(ds)

    return datasets


def filter_datasets_by_existing_copies(
    datasets : 'defaultdict(set)',
    rules_and_replicas_req: RulesAndReplicasReq,
):
    '''
    Method to filter datasets based on rules and replicas requirements.

    Parameters
    ----------
    datasets: defaultdict(set)
        dict with keys as scopes and values as sets of datasets being processed
    rules_and_replicas_req: RulesAndReplicasReq
        Rules and replicas requirements

    Returns
    -------
    filtered_datasets: defaultdict(set)
        dictionary with keys as scopes and values as sets of datasets being processe
    '''

    filtered_datasets = defaultdict(set)
    for scope, dses in datasets.items():
        for ds in dses:

            # We check if the dataset has a rule on the RSE that is required to have a rule on it
            req_existing_rule_exists = True # Set to True by default so that if no RSEs are specified, the check passes
            if rules_and_replicas_req.rule_on_rse is not None:
                # Set to False so that if RSEs are specified, and none of them have a rule, the check fails
                req_existing_rule_exists = False
                for rse in rules_and_replicas_req.rule_on_rse:
                    req_existing_rule_exists = has_rule_on_rse(ds, scope, rse, didcl)
                    if req_existing_rule_exists: break

            # We check if the dataset has a replica on the RSE that is required to have a replica on it

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

def get_datasets_from_files(files):
    '''
    Method to get the datasets from a list of files

    Parameters
    ----------
    files: list
        list of files containing datasets (one per line)

    Returns
    -------
    all_datasets: list
        list of datasets
    '''
    all_datasets = []
    for f in files:
        with open(f,'r') as f:  all_datasets.extend(f.readlines())
    return all_datasets

# =============================================================
# =============== Methods for Rucio rule checking  =============
# =============================================================

def has_rule_on_rse(did, scope, rse, didcl):
    '''
    Method to check if a dataset has a rule on a given RSE

    Parameters
    ----------
    did: str
        Name of the dataset to check
    scope: str
        Scope of the dataset to check
    rse: str
        Name of the RSE to check

    Returns
    -------
    has_rule: bool
        True if the dataset has a rule on the RSE, False otherwise
    '''

    # Find existing rules for the given did
    rules    = list(didcl.list_did_rules(scope, did.replace('/','')))
    for rule in rules:
        if re.match(rse, rule.get("rse_expression")) is not None:
            return True

    # If here, no rule found on RSE
    return False

def has_replica_on_rse(did, scope, rse, replicacl):
    '''
    Method to check if a dataset has a replica on a given RSE. This
    is done by checking if all the files in the dataset have a replica
    on the RSE.

    Parameters
    ----------
    did: str
        Name of the dataset to check
    scope: str
        Scope of the dataset to check
    rse: str
        Name of the RSE to check

    Returns
    -------
    has_replica: bool
        True if the dataset has a replica on the RSE, False otherwise
    '''

    file_replicas = list(replicacl.list_replicas([{'scope': scope, 'name': did.replace('/','')}]))

    file_replica_is_on_req_rse = []

    for file_replica in file_replicas:
        fname = file_replica.get("name")
        file_rses = list(file_replica.get("rses").keys())
        if any(re.match(rse, frse) is not None for frse in file_rses):
            file_replica_is_on_req_rse.append(True)
        else:
            file_replica_is_on_req_rse.append(False)
    if all(file_replica_is_on_req_rse): return True
    else: return False

def get_rses_from_regex(rse_regex, rsecl):
    '''
    Method to get a list of RSEs from a list of RSEs that match a regex

    Parameters
    ----------
    rse_regex: str
        Regex to match RSEs against
    rsecl: rucio.client.rseclient.RSEClient
        RSE client to use to get the list of RSEs

    Returns
    -------
    matching_rses: list
        set of RSEs that match the regex
    '''

    matching_rses = set()

    available_rses = rsecl.list_rses()
    for avail_rse in available_rses:
        if re.match(rse_regex, avail_rse.get('rse')) is not None:
            matching_rses.add(avail_rse.get('rse'))
    return matching_rses