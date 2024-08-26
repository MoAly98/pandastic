#!python3

# Required Imports
# System
import sys, os, json, re
import argparse
from datetime import datetime
from pprint import pprint
from collections import defaultdict
import logging
# PanDA: /cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase/x86_64/PandaClient/1.5.9/lib/python3.6/site-packages/pandaclient/PBookCore.py
from pandaclient import PBookCore
from pandaclient import queryPandaMonUtils
pbook = PBookCore.PBookCore()
# Rucio
from rucio import client as rucio_client
import rucio
# Pandastic
from pandastic.utils.tools import ( draw_progress_bar, get_lines_from_files )
from pandastic.utils.common import ( has_replica_on_rse, has_rule_on_rse, has_rulehist_on_rse,
                     RulesAndReplicasReq)

class DatasetHandler(object):
    """
    This class will hold general methods used in determining the
    set of datasets that need to be processed by actions that
    involve RSEs. These operations include: deletion of rules,
    replication of rules, updating rules and downloading datasets.
    """

    def __init__(self,
                 *,
                 regexes: str,
                 rses: str,
                 rules_replica_req: RulesAndReplicasReq = None,
                 containers: bool = False,
                 # FROM FILES
                 fromfiles: str = None):

        self.regexes = regexes
        self.rses = rses
        self.only_cont = containers
        self.rules_replica_req = rules_replica_req
        self.fromfiles = fromfiles

        self.rulecl = rucio_client.ruleclient.RuleClient()
        self.didcl = rucio_client.didclient.DIDClient()
        self.rsecl = rucio_client.rseclient.RSEClient()
        self.replicacl = rucio_client.replicaclient.ReplicaClient()

    def PrintSummary(self):
        print(f'===================================')
        print(f'Summary of DatasetHandler object:')
        print(f'===================================')
        print(f'Method of extracting datasets: from files')
        print(f'Regexes used to filter the files: {self.regexes}')
        print(f'RSEs considred for action: {self.rses}')
        print(f'Only consider containers: {self.only_cont}')
        print(f'Rules and replicas requirements: {self.rules_replica_req}')
        print(f'===================================')

    def GetDatasets(self):
        datasets = defaultdict(set)
        # Get the datasets from the files
        all_datasets = get_lines_from_files(self.fromfiles)
        for ds in all_datasets:
            if ':' in ds:
                scope, did = ds.split(':')
            else:
                did = ds
                scope = '.'.join(did.split('.')[:2])
            if not any(re.match(regex, did) for regex in self.regexes):
                continue
            datasets[scope].add(did.strip())
        return datasets

    def FilterDatasets(self,
                       datasets : 'defaultdict(set)',
                       norule_on_allrses: list = None,
                       ignore: list = None):
        '''
        Method to filter datasets based on rules and replicas requirements.

        Parameters
        ----------
        self: DatasetHandler
            DatasetHandler object
        datasets: defaultdict(set)
            dict with keys as scopes and values as sets of datasets being processed
        norule_on_allrses: list
            List of RSEs that we don't want datasets to have a rule on all of them
        ignore: list
            List of datasets to ignore
        Returns
        -------
        filtered_datasets: defaultdict(set)
            dictionary with keys as scopes and values as sets of datasets being processe
        '''
        rules_and_replicas_req = self.rules_replica_req
        filtered_datasets = defaultdict(set)
        for scope, dses in datasets.items():
            for ds in dses:
                if ignore is not None and ds.replace('/','') in ignore:
                    print(f"WARNING: Dataset {ds} is in the ignore list. Skipping.")
                    continue

                if norule_on_allrses is not None:
                    # We check if the dataset has a rule on all the RSEs that are required to not have a rule on all of them
                    rses_to_use = self.SkipBcRulesOnAllRses(ds, scope, norule_on_allrses, self.didcl)
                    if rses_to_use == []:
                        print(f"WARNING: Dataset {ds} already has a rule on all the RSEs we don't want to have a rule on. Skipping replication to RSE.")
                        continue
                try:
                    ds_type = self.didcl.get_metadata(scope, ds.replace('/','')).get('did_type')
                    nfiles = len(list(self.didcl.list_files(scope, ds.replace('/',''))))

                    if nfiles == 0:
                        print(f"WARNING: Dataset {ds} has zero files.")
                    parent  = next(self.didcl.list_parent_dids(scope, ds.replace('/','')), None)
                    if parent is not None:
                        parent = parent.get('name')

                except rucio.common.exception.DataIdentifierNotFound:
                    print(f"WARNING: Dataset {ds} not found on Rucio. Skipping.")
                    continue

                # We check if the dataset has a rule on the RSE that is required to have a rule on it
                req_existing_rule_exists = True # Set to True by default so that if no RSEs are specified, the check passes
                if rules_and_replicas_req.rule_on_rse is not None:
                    # Set to False so that if RSEs are specified, and none of them have a rule, the check fails
                    req_existing_rule_exists = False
                    for rse in rules_and_replicas_req.rule_on_rse:
                        req_existing_rule_exists_ds = has_rule_on_rse(ds, scope, rse, self.didcl)

                        # Can a parent container satisfy the condition?
                        req_existing_rule_exists_parent = False
                        if rules_and_replicas_req.cont_rule_req and parent is not None and ds_type == 'DATASET':
                            req_existing_rule_exists_parent = has_rule_on_rse(parent, scope, rse, self.didcl)

                        req_existing_rule_exists = req_existing_rule_exists_ds or req_existing_rule_exists_parent

                        if req_existing_rule_exists: break

                # We check if the dataset has a replica on the RSE that is required to have a replica on it

                req_existing_replica_exists = True # Set to True by default so that if no RSEs are specified, the check passes
                if rules_and_replicas_req.replica_on_rse is not None:
                    # Set to False so that if RSEs are specified, and none of them have a replica, the check fails
                    req_existing_replica_exists = False
                    for rse in rules_and_replicas_req.replica_on_rse:
                        req_existing_replica_exists_ds = has_replica_on_rse(ds, scope, rse, self.replicacl)
                        # Can a parent container satisfy the condition?
                        req_existing_replica_exists_parent = False
                        if rules_and_replicas_req.cont_rule_req and parent is not None and ds_type == 'DATASET':
                            req_existing_replica_exists_parent = has_replica_on_rse(parent, scope, rse, self.replicacl)

                        req_existing_replica_exists = req_existing_replica_exists_ds or req_existing_replica_exists_parent

                        if req_existing_replica_exists: break

                # We check if the dataset has ever had a rule on an RSE where it shouldn't have had a rule ever
                # This is useful if we want to only e.g. replicate datasets that were not replicated and deleted before from an RSE

                req_norulehist = [True] # Set to True by default so that if no RSEs are specified, the check passes
                if rules_and_replicas_req.norulehistory_on_rse is not None:
                    # Set to False so that if RSEs are specified, and none of them have a replica, the check fails
                    req_norulehist = [False]*len(rules_and_replicas_req.norulehistory_on_rse)
                    for i, rse in enumerate(rules_and_replicas_req.norulehistory_on_rse):
                        req_norulehist_ds = not has_rulehist_on_rse(ds, scope, rse, self.rulecl)
                        # Can a parent container satisfy the condition?
                        req_norulehist_parent = False
                        if rules_and_replicas_req.cont_rule_req and parent is not None and ds_type == 'DATASET':
                            req_norulehist_parent = not has_rulehist_on_rse(parent, scope, rse, self.rulecl)

                        req_norulehist[i] = req_norulehist_ds or req_norulehist_parent

                if not all(req_norulehist):
                    continue

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

    def SkipBcRulesOnAllRses(self, ds, scope, rses, didcl):
        '''
        Method to filter out RSEs that already have replicas of the dataset

        Parameters
        ----------
        ds : str
            Name of the dataset to check
        scope : str
            Scope of the dataset to check
        rses : list
            List of RSEs to check
        didcl : rucio.client.didclient.DIDClient
            Rucio DID client

        Returns
        -------
        rses : list
            List of RSEs that don't have replicas of the dataset.  If all RSEs have replicas, returns an empty list
        '''

        rses_to_remove =[]
        usable_rses = rses.copy()
        for rse in rses:
            ds_has_rule_on_rse = has_rule_on_rse(ds, scope, rse, didcl)

            if ds_has_rule_on_rse:
                print(f"WARNING: Dataset {ds} already has a rule on the RSE {rse} on which there shouldn't already be a rule. Skipping RSE...")
                rses_to_remove.append(rse)
                continue
        for removal in rses_to_remove:
            usable_rses.remove(removal)

        return usable_rses

class PandaDatasetHandler(DatasetHandler):

    def __init__(self, *,
                 usetasks: list,
                 ds_type: str,
                 days: int = 30,
                 users: list = [pbook.username],
                 did: list = None,
                 matchfiles: bool = False,
                 production: bool = False,
                 **kwargs):

        super().__init__(**kwargs)
        self.matchfiles = matchfiles
        self.days = days
        self.panda_users = users
        self.type = ds_type
        self.did = did
        self.usetasks = usetasks
        self.production = production
    def PrintSummary(self):
        print(f'===================================')
        print(f'Summary of DatasetHandler object:')
        print(f'===================================')
        print(f'Method of extracting datasets: PanDA')
        print(f'Regexes used to filter the PanDA tasks: {self.regexes}')
        print(f'RSEs considred for action: {self.rses}')
        print(f'Only consider containers: {self.only_cont}')
        print(f'Rules and replicas requirements: {self.rules_replica_req}')
        print(f'Task input and output file count must match: {self.matchfiles}')
        print(f'PanDA users to consider: {self.panda_users}')
        print(f'PanDA tasks to consider: {self.usetasks}')
        print(f'PanDA days to consider: {self.days}')
        print(f'PanDA dataset type to consider: {self.type}')
        print(f'PanDA dataset DID regex to consider: {self.did}')
        print(f'Looking for production datasets (assuming scope is dataXX_xxTeV or mcXX_xxTeV)')
        print(f'===================================')

    def GetDatasetsFromTasks(self, tasks):
        '''
        Method to get datasets associated to GRID jobs.

        Parameters
        ----------
        tasks: list
            List of jobs to search through

        Returns
        -------
        datasets: defaultdict(set)
            A dictionary of datasets to process with keys being scope and values being a set of dataset names
        '''

        regexes = self.regexes
        ds_type = self.type
        did_regexes = self.did
        only_cont = self.only_cont
        matchfiles = self.matchfiles
        didcl = self.didcl

        # If we're matching files, we need a DID client to get the number of files in the dataset
        if matchfiles: assert self.didcl is not None, "Must provide a DID client to check input/output file counts"

        # Get the type of dataset to look for
        if ds_type == 'OUT':  look_for_type = 'output'
        else:   look_for_type = 'input'

        # List to hold names of DIDs to be processed
        datasets = defaultdict(set)

        # Dictionary to hold the number of files in the input and output datasets (scope: taskname: {input: nfiles, output: nfiles})
        task_to_nfiles_out = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
        # Dictionarty to match a task to the datasets saved from it (scope: taskname: [dsnames])
        task_to_saved_ds   = defaultdict(lambda: defaultdict(list))

        # SET of containers we don't want to process
        hated_containers = set()

        ntasks = 0
        # Loop over the jobs
        for i, task in enumerate(tasks):

            # =========  Progress Bar =========
            draw_progress_bar(len(tasks), i, f'Progress for collecting dids from tasks')

            print(task)
            # Get the name of the task
            taskname = task.get("taskname")
            # Skip the task if it doesn't match the regex
            if all(re.match(rf'{rgx}', taskname) is None for rgx in regexes):   continue
            # Get the datasets associated to the task
            task_datasets = task.get("datasets")

            for ds in task_datasets:
                # Get the type of the dataset
                dstype = ds.get("type")
                # Get the name of the dataset
                dsname = ds.get("datasetname")
                # Get the name of the dataset parent container
                contname = ds.get("containername")

                # Skip the type of dataset we don't care about
                if(dstype != look_for_type):    continue
                # Get the scope from the dsname (is there a better way?)

                if ':' in dsname:   scope = dsname.split(':')[0]
                else:   scope = '.'.join(dsname.split('.')[:1]) if self.production else '.'.join(dsname.split('.')[:2])

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


                # Skip if another dataset added this container (if we are saving containers)
                if contname in datasets[scope]:    continue

                # Default is processing datasets
                to_process = dsname
                if only_cont:   to_process = contname

                # Check if the dataset/container name matches the DID regex
                if did_regexes is not None:

                    # Skip the dataset/container if it doesn't match the DID regex
                    if all(re.match(did_regex, to_process)  is None for did_regex in did_regexes):
                        # If we are processing containers, we can optimise by
                        # adding the container to the hated_containers set
                        if only_cont:   hated_containers.add(to_process)
                        continue
                # Save the dataset/container to the mapping from task to datasets associated to it
                task_to_saved_ds[scope][taskname].append(to_process)
                # Save the dataset/container to the map from scopes to datasets to process
                datasets[scope].add(to_process)
            # Increment the number of tasks we've processed
            ntasks += 1
        print(f"INFO:: Retrieved datasets from {ntasks} tasks..")

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

    def GetTasksFromPanda(self):
        """
        This method will get the tasks from the Panda server
        """

        users = self.panda_users
        days  = self.days
        ds_type = self.type

        # If --usetask is used, the dataset type must be specified
        assert ds_type is not None, "ERROR:: --type must be specified if --usetask is used"

        all_users_tasks = []
        for user in users:
            print(f"INFO:: Looking for tasks which are {self.usetasks} on the grid for user {user} in the last {days} days")
            # Find all PanDA tasks that are done for the user and period specified
            if self.usetasks == "any":
                usetasks = None
            else:
                usetasks = self.usetasks
            _, url, tasks = queryPandaMonUtils.query_tasks(username=user, days=days, status=usetasks)

            print(len(tasks), "tasks found")
            # Tell the user the search URL if they want to look
            print(f"INFO:: PanDAs query URL: {url}")

            all_users_tasks.extend(tasks)

        return all_users_tasks

    def GetDatasets(self):
        """
        This method will get the datasets from the tasks
        """

        tasks =    self.GetTasksFromPanda()
        datasets = self.GetDatasetsFromTasks(tasks)

        return datasets

class RucioDatasetHandler(DatasetHandler):

    def __init__(self, scopes, **kwargs):
        super().__init__(**kwargs)
        self.scopes = scopes

    def GetDatasets(self):
        datasets = defaultdict(set)
        for scope in self.scopes:
            print("INFO:: Looking for datasets in scope", scope, "matching regexes")
            for regex in self.regexes:
                print("INFO:: Looking for datasets matching regex", regex)
                dids = list(self.didcl.list_dids(scope, {'name': regex.replace('.*','*').replace('/','')}))

                if len(dids) == 0:
                    print("WARNING:: No datasets found matching regex", regex)
                    continue

                for i, did in enumerate(dids):

                    # ============ Progresss Bar ============= #
                    draw_progress_bar(len(dids), i, f'Progress for collecting dids matching regex {regex}')

                    did_type = self.didcl.get_metadata(scope, did.replace('/','')).get('did_type')
                    if self.only_cont and did_type != 'CONTAINER': continue
                    elif not self.only_cont and did_type != 'DATASET': continue
                    datasets[scope].add(did)

        return datasets
    def PrintSummary(self):
        print(f'===================================')
        print(f'Summary of DatasetHandler object:')
        print(f'===================================')
        print(f'Method of extracting datasets: Rucio')
        print(f'Regexes used to filter the datasets: {self.regexes}')
        print(f'RSEs considred for action: {self.rses}')
        print(f'Only consider containers: {self.only_cont}')
        print(f'Rules and replicas requirements: {self.rules_replica_req}')
        print(f'Scopes considered: {self.scopes}')

        print(f'===================================')




