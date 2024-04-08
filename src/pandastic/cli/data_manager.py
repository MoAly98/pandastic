#!python3

'''
This module is a dataset manager which links PanDA and Rucio. It's functionality is to
delete, update, replicate rules for datasets that t he user specifies. It can also download
datasets. The user can extract the datasets to be processed either from a PanDA job report
or from a Rucio dataset list or from simply providing a list of datasets in a file.
'''

# Required Imports
# System
import os, json, re, urllib3
import argparse
from datetime import datetime
from collections import defaultdict
# PanDA: /cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase/x86_64/PandaClient/1.5.9/lib/python3.6/site-packages/pandaclient/PBookCore.py
from pandaclient import PBookCore
from pandaclient import queryPandaMonUtils
pbook = PBookCore.PBookCore()
pbook.init()
# Rucio
from rucio import client as rucio_client
import rucio
import rucio.client.downloadclient as downloadclient

# Pandastic
from pandastic.utils.tools import ( dataset_size, bytes_to_best_units, draw_progress_bar, get_lines_from_files, SetEncoder )
from pandastic.utils.common import ( get_rses_from_regex, RulesAndReplicasReq )
from pandastic.actions.delete_actions import ( get_ruleids_to_delete, delete_rule )
from pandastic.actions.replicate_actions import ( add_rule )
from pandastic.actions.filelist_actions import ( list_replicas )
from pandastic.actions.update_actions import ( get_ruleids_to_update, update_rule )
from pandastic.utils.dataset_handlers import (DatasetHandler, RucioDatasetHandler, PandaDatasetHandler)

# ===============  Rucio Clients ================
rulecl     = rucio_client.ruleclient.RuleClient()
didcl      = rucio_client.didclient.DIDClient()
rsecl      = rucio_client.rseclient.RSEClient()
replicacl  = rucio_client.replicaclient.ReplicaClient()

downloadcl = downloadclient.DownloadClient()

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ===============  ArgParsing  ===================================
# ===============  Arg Parser Help ===============================
_h_action                 = 'The action to perform on the datasets'
_h_regex                  = 'A regex in the panda *taskname*/*did* to be used to find the jobs to replicate datasets from'
_h_rses                   =  'The RSEs to process to (create rules there)'
_h_rule_on_rse            = 'List of RSEs that the DID must have rule on *any* of them before we process it'
_h_replica_on_rse         = 'List of RSEs that the DID must have replica on *any* of them before we process it'
_h_rule_or_replica_on_rse = 'Use if both replica_on_rse and rule_on_rse are used, to specify that if either are satisfied, replication will happen.\
                             Not using this means both must be satified satisfied'
_h_scopes                 = 'Scopes to look for the DIDs in if --usetask is not used'
_h_type                   = 'Type of dataset being processd .. is it the task input or output?'
_h_days                   = 'The number of days in the past to look for jobs in'
_h_users                  = 'The grid usernames under for which the jobs should be searched (often your normal name with spaces replaced by +)'
_h_life                   = 'How long is should the lifetime of the dataset be on its destination RSE'
_h_did                    = 'Subset of the jobs following the pattern to keep'
_h_submit                 = 'Should the code submit the replication jobs? Default is to run dry'
_h_usetask                = 'Should the regex be used to filter PanDA jobs? Specify task statuses to look for here'
_h_containers             = 'Should the code only process containers? Default is to process individual datasets. DID regex will be used to match the container name.'
_h_matchfiles             = 'Strict requirment when finding datasets from PanDA tasks that input and output number of files match for a given task before we delete rules of containers/datasets associated with it'
_h_outdir                 = 'Output directory for the output files. Default is the current directory'
_h_fromfiles              = 'Files containing lists of datasets to process \n \
                             If this is used, only --regex, --rses, --submit, --outdir arguments are used as well.\n\
                             The regex will be used to filter the datasets, and rses will be used get datasets to process'
_h_cont_rule_req          = 'If a container has a rule on a given RSE, should we process the datasets in it?\
                             This is used when applying the filter using existing rules/replicas.'
_h_nohist_on_rse          = 'Ignore datasets that has ever had rules on given RSEs!'
_h_notinfiles             = 'Files containing lists of datasets to ignore'
_h_maxlifeleft            = 'Maximum lifetime left for a rule to be processed (useful for update of rules)'
_h_noscopeinout           = 'Do not use scope in dataset names stored in the output file'
# ===============  Arg Parser Choices ===============================
_choices_usetasks =  ['submitted', 'defined', 'activated',
                      'assigned', 'starting', 'running',
                      'merging', 'finished', 'failed',
                      'cancelled', 'holding', 'transferring',
                      'closed', 'aborted', 'unknown', 'all',
                      'throttled', 'scouting', 'scouted', 'done',
                      'tobekilled', 'ready', 'pending', 'exhausted', 'paused',
                      'broken', 'submitting', 'finishing', 'aborting', 'passed', 'any']
_action_choices  = ['listfiles', 'find', 'replicate', 'delete', 'update', 'download']

def argparser():
    '''
    Method to parse the arguments for the script.
    '''
    parser = argparse.ArgumentParser("This is used to process datasets using RUCIO client")
    parser.add_argument('action',                     type=str,   choices=_action_choices,               help=_h_action)
    parser.add_argument('-s', '--regex',              type=str,   required=True,   nargs='+',            help=_h_regex)
    parser.add_argument('-r', '--rses',               type=str,   nargs='+',                             help=_h_rses)
    parser.add_argument('-d', '--days',               type=int,   default=30,                            help=_h_days)
    parser.add_argument('-u', '--grid-user',          nargs='+',  default=[pbook.username],              help=_h_users)
    parser.add_argument('-l', '--lifetime',           type=int,   default = 36000,                        help= _h_life)
    parser.add_argument('--did',                      nargs='+',                                         help=_h_did)
    parser.add_argument('--scopes',                   nargs='+',                                         help=_h_scopes)
    parser.add_argument('--rule_on_rse',              nargs='+',                                         help=_h_rule_on_rse)
    parser.add_argument('--replica_on_rse',           nargs='+',                                         help=_h_replica_on_rse)
    parser.add_argument('--norulehist_on_rse',        nargs='+',                                         help=_h_nohist_on_rse)
    parser.add_argument('--rule_or_replica_on_rse',   action='store_true',                               help=_h_rule_or_replica_on_rse)
    parser.add_argument('--contrulereq',              action='store_true',                               help=_h_cont_rule_req)
    parser.add_argument('--usetasks',                 nargs='+', choices = _choices_usetasks,            help=_h_usetask)
    parser.add_argument('--type',                     type=str,   choices=['OUT','IN'],                  help=_h_type)
    parser.add_argument('--containers',               action='store_true',                               help=_h_containers)
    parser.add_argument('--matchfiles',               action='store_true',                               help=_h_matchfiles)
    parser.add_argument('--submit',                   action='store_true',                               help=_h_submit)
    parser.add_argument('--outdir',                   type=str,   default='./',                          help=_h_outdir)
    parser.add_argument('--fromfiles',                type=str,   nargs='+',                             help=_h_fromfiles)
    parser.add_argument('--notinfiles',               type=str,   nargs='+',                             help=_h_notinfiles)
    parser.add_argument('--maxlifeleft',              type=str,                                          help=_h_maxlifeleft)
    parser.add_argument('--noScopeInOut',             action='store_true',                               help=_h_noscopeinout)
    parser.add_argument('--downto',                   type=str,                                          help="Where to download to")
    parser.add_argument('--prod',                     action='store_true',                               help="Is this a production dataset")
    return parser.parse_args()

def run():
    """" Main method """
    args = argparser()

    action = args.action

    regexes    = args.regex

    # Prepare list of RSEs concerned
    rses       = args.rses
    if action != 'download' and action != 'find' and action != 'listfiles':
        assert rses is not None, f"ERROR: RSEs must be specified for action {action}"
    rses      = rses if rses is not None else []
    usable_rses = set()
    for rse in rses:
        usable_rses |= get_rses_from_regex(rse, rsecl)
    if action != 'download' and action != 'find' and action != 'listfiles':
        assert len(usable_rses) > 0, "No RSEs found to replicate to. Exiting."

    if action == 'replicate' or action == 'update':
        assert args.lifetime is not None, f"ERROR: Lifetime must be specified for action {action}"

    outdir     = args.outdir
    os.makedirs(outdir, exist_ok=True)
    only_cont  = args.containers
    submit     = args.submit
    existing_copies_req = RulesAndReplicasReq(args.rule_on_rse,
                                              args.replica_on_rse,
                                              args.rule_or_replica_on_rse,
                                              args.contrulereq,
                                              args.norulehist_on_rse)

    # Workout how the datasets to be processed will be retrived
    fromfiles = args.fromfiles
    usetasks  = '|'.join(args.usetasks) if args.usetasks is not None else None

    if fromfiles is not None and usetasks is not None:
        print("ERROR: Cannot specify both --usetask and --fromfiles. Exiting.")
        exit(1)
    if args.fromfiles is not None:
        dataset_handler = DatasetHandler(regexes = regexes,
                                         rses = rses,
                                         containers = only_cont,
                                         rules_replica_req = existing_copies_req,
                                         fromfiles = args.fromfiles,)
    elif usetasks is not None:
        # If --usetask is used, the dataset type must be specified
        assert args.type is not None, "ERROR:: --type must be specified if --usetask is used"

        dataset_handler = PandaDatasetHandler(regexes = regexes,
                                                  rses = rses,
                                                  containers = only_cont,
                                                  rules_replica_req = existing_copies_req,
                                                  matchfiles = args.matchfiles,
                                                  days = args.days,
                                                  users = args.grid_user,
                                                  usetasks = usetasks,
                                                  ds_type  = args.type,
                                                  did  = args.did,
                                                  production = args.prod)

    else:
        # if --usetask is not used, the scopes must be specified
        assert args.scopes is not None, "ERROR:: --scopes must be specified if --usetask is not used"

        dataset_handler = RucioDatasetHandler(regexes = regexes,
                                              rses = rses,
                                              containers = only_cont,
                                              rules_replica_req = existing_copies_req,
                                              scopes = args.scopes)

    dataset_handler.PrintSummary()
    datasets = dataset_handler.GetDatasets()

    ignore_datasets = []
    if args.notinfiles is not None:
        ignore_datasets = get_lines_from_files(args.notinfiles)
    # Filter the datasets
    datasets = (dataset_handler.FilterDatasets(datasets, norule_on_allrses=rses, ignore=ignore_datasets)
               if action == 'replicate' else dataset_handler.FilterDatasets(datasets, ignore=ignore_datasets))
    # ==================================================== #
    # ================= Process the datasets ============== #
    # ==================================================== #
    # Keep track of the action (what, where, ruleid)
    action_summary = defaultdict(lambda: defaultdict(dict))

    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    nprocessed, totalsize_processed = 0, 0

    # Prepare output files for monitoring
    dids_monit_file = open(f'{outdir}/monit_{action}_dids_{now}.txt', 'w')
    ruleid_monit_file = open(f'{outdir}/monit_{action}_ruleids_{now}.txt', 'w')
    if action == 'listfiles':
        replica_monit_file = open(f'{outdir}/monit_{action}_replicas_{now}.txt', 'a+')
    # Loop over the scopes
    for scope, dids in datasets.items():
        print(f"Looking into actioning {len(dids)} datasets in scope {scope}")
        # Loop over the datasets in scope
        for did in dids:
            outds = did if args.noScopeInOut else f'{scope}:{did}'
            # Try to get the size of the dataset, use as proxy to skip datasets
            # found from a task but not existing on Rucio...
            try:
                totalsize_processed += dataset_size(did, scope, didcl)
            except rucio.common.exception.DataIdentifierNotFound:
                print("WARNING:: Dataset not found in Rucio: ", did, "Skipping...")
                continue

            if action == 'replicate':
                    # Loop over the RSEs to replicate to
                    for rse in usable_rses:
                        # Tell the user what we are doing
                        print("INFO:: Replicating rules for dataset: ", did)
                        print("INFO:: Replicating to RSE: ", rse)
                        # Only really add the rule if --submit is used
                        if args.submit:
                            ruleid = add_rule(did, rse, args.lifetime, scope, rulecl)
                        else:
                            ruleid = 'NOT_SUBMITTED'

                        # Write to monitoring scripts
                        dids_monit_file.write(f"{outds}\n")
                        ruleid_monit_file.write(ruleid+'\n')
                        # Keep track of number of rule deletions
                        nprocessed += 1
                        # Keep track of what we replicated exactly
                        action_summary[did][rse]['ruleid'] = ruleid

            elif action == 'delete':
                # Prepare in case a dataset has no rules
                no_valid_rules = True
                # Find the rules to delete and rses to delete them from
                rule_ids_rses_zip = get_ruleids_to_delete(did, usable_rses, rses, scope, didcl)
                # Loop over the rules to delete and rse to delete them from
                for ruleid, rse in rule_ids_rses_zip:
                    # If we are here, at least one rule was found for the dataset
                    no_valid_rules = False

                    # Tell the user what we are doing
                    print("INFO:: Deleting rules for dataset: ", did)
                    print(f"INFO:: Deleting rule ID {ruleid} on RSE {rse}")

                    # Only really delete the rule if --submit is used
                    if args.submit:
                        success = delete_rule(ruleid, rulecl)
                        if not success: continue

                    # Write to monitoring scripts
                    dids_monit_file.write(f"{outds}\n")
                    ruleid_monit_file.write(ruleid+'\n')
                    # Keep track of number of rule deletions
                    nprocessed += 1
                    # Keep track of what we deleted exactly
                    action_summary[did][rse]['ruleid'] = ruleid

                # Tell the user if no rules were found for the dataset
                if no_valid_rules:
                    print("WARNING:: No rules to delete for dataset: ", did)
                    continue
            elif action == 'update':
                # Prepare in case a dataset has no rules
                no_valid_rules = True
                max_time_to_death = args.maxlifeleft
                # Find the rules to update and rses to update them from
                rule_ids_rses_zip = get_ruleids_to_update(did, usable_rses, rses, scope, max_time_to_death, didcl)
                # Loop over the rules to update and rse to update them from
                for ruleid, rse in rule_ids_rses_zip:
                    # If we are here, at least one rule was found for the dataset
                    no_valid_rules = False

                    # Tell the user what we are doing
                    print("INFO:: Updating rules for dataset: ", did)
                    print(f"INFO:: Updating rule ID {ruleid} on RSE {rse}")

                    # Only really update the rule if --submit is used
                    if args.submit:
                        success = update_rule(ruleid, args.lifetime, rulecl)
                        if not success: continue
                    # Write to monitoring scripts
                    dids_monit_file.write(f"{outds}\n")
                    ruleid_monit_file.write(ruleid+'\n')
                    # Keep track of number of rule updates
                    nprocessed += 1
                    # Keep track of what we updated exactly
                    action_summary[did][rse]['ruleid'] = ruleid

                # Tell the user if no rules were found for the dataset
                if no_valid_rules:
                    print("WARNING:: No rules to update for dataset: ", did)
                    continue
            elif action == 'download':
                items = {'did': did, 'base_dir': args.downto}
                if usable_rses is not None:
                    if len(usable_rses) == 1:
                        items = {'did': cont, 'base_dir': args.downto, 'rse': usable_rses[0]}
                    else:
                        if len(usable_rses) != 0:
                            print("WARNING:: More than one RSE specified for download is invalid... not using any RSEs")

                dids_monit_file.write(f"{outds}\n")
                if args.submit:
                    try:
                        downloadcl.download_dids([items])
                        nprocessed += 1
                    except rucio.common.exception.NotAllFilesDownloaded as e:    raise str(e)
            elif action == 'listfiles':
                replicas = list_replicas(did, scope, rses, replicacl)
                json.dump(replicas, replica_monit_file, indent=4, cls=SetEncoder)
                dids_monit_file.write(f"{outds}\n")
                nprocessed += 1

            else:
                dids_monit_file.write(f"{outds}\n")
                nprocessed += 1

    dids_monit_file.close()
    ruleid_monit_file.close()
    if action == 'listfiles':
        replica_monit_file.close()

    # Dump the replication summary to a json file
    with open(f'{outdir}/{action}_summary_{now}.json', 'w') as f:
        json.dump(action_summary, f, indent=4)

    # Summarise to user
    print("INFO:: TOTAL NUMBER OF DATASET PROCESSED: ", nprocessed)
    good_units_size = bytes_to_best_units(totalsize_processed)
    print("INFO:: TOTAL SIZE OF DATASETS PROCESSED: ", good_units_size[0], good_units_size[1])

    # Remove the monit file if --submit is not used
    if not args.submit:
        print(f"INFO:: --submit not used, so not {action}-ing. Deleting ruleid monit file.. ")
        os.unlink(f'{outdir}/monit_{action}_ruleids_{now}.txt')

if __name__ == "__main__":  run()
