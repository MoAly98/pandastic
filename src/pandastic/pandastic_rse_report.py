'''
The idea here is to find how much space a user is using on a given RSE or groups of RSEs and which Datasets are
using up this space
'''
# Required Imports
# System
import os, json, re
import argparse
from datetime import datetime
from collections import defaultdict
# Rucio
from rucio import client as rucio_client
RUCIO_USER = os.environ.get('RUCIO_ACCOUNT')
# Pandastic
from tools import  ( dataset_size, bytes_to_best_units, draw_progress_bar )
from common import (  get_rses_from_regex, has_rule_on_rse)
from dataset_handlers import (DatasetHandler, RucioDatasetHandler, PandaDatasetHandler)

# ===============  Rucio Clients ================
rulecl    = rucio_client.ruleclient.RuleClient()
didcl     = rucio_client.didclient.DIDClient()
rsecl     = rucio_client.rseclient.RSEClient()
replicacl = rucio_client.replicaclient.ReplicaClient()
acccl     = rucio_client.accountclient.AccountClient()
# ===============  ArgParsing  ===================================
# ===============  Arg Parser Help ===============================
_h_regex                  = 'A regex in the panda *taskname* to be used to find the jobs to replicate datasets from'
_h_rses                   =  'The RSEs to process to (create rules there)'
_h_scopes                 = 'Scopes to look for the DIDs in if --usetask is not used'
_h_users                  = 'The grid usernames under for which the jobs should be searched (often your normal name with spaces replaced by +)'
_h_outdir                 = 'Output directory for the output files. Default is the current directory'



_h_rule_on_rse            = 'List of RSEs that the DID must have rule on *any* of them before we process it'
_h_replica_on_rse         = 'List of RSEs that the DID must have replica on *any* of them before we process it'
_h_rule_or_replica_on_rse = 'Use if both replica_on_rse and rule_on_rse are used, to specify that if either are satisfied, replication will happen.\
                             Not using this means both must be satified satisfied'
_h_type                   = 'Type of dataset being processd .. is it the task input or output?'
_h_days                   = 'The number of days in the past to look for jobs in'
_h_life                   = 'How long is should the lifetime of the dataset be on its destination RSE'
_h_did                    = 'Subset of the jobs following the pattern to keep'
_h_submit                 = 'Should the code submit the replication jobs? Default is to run dry'
_h_usetask                = 'Should the regex be used to filter PanDA jobs? Specify task statuses to look for here'
_h_containers             = 'Should the code only process containers? Default is to process individual datasets. DID regex will be used to match the container name.'
_h_matchfiles             = 'Strict requirment when finding datasets from PanDA tasks that input and output number of files match for a given task before we delete rules of containers/datasets associated with it'
_h_fromfiles              = 'Files containing lists of datasets to process \n \
                             If this is used, only --regex, --rses, --submit, --outdir arguments are used as well.\n\
                             The regex will be used to filter the datasets, and rses will be used get datasets to process'
_h_gsummary               = 'Get a general summary of RSE usage for the user'
_h_tags                   = 'To get a breakdown of the space used by different regexes, specify them here'
_h_containers             = 'Should the code only process containers? Default is to process individual datasets. DID regex will be used to match the container name.'


def argparser():
    '''
    Method to parse the arguments for the script.
    '''
    parser = argparse.ArgumentParser("This report usage on RSEs for usrs")
    parser.add_argument('-s', '--regexes', type=str, required=True, nargs='+',  help=_h_regex)
    parser.add_argument('-r', '--rses',    type=str, required=True, nargs='+',  help=_h_rses)
    parser.add_argument('--scopes',        type=str, required=True, nargs='+',  help=_h_scopes)
    parser.add_argument('-u', '--usrs',    nargs='+', default=[RUCIO_USER],     help=_h_users)
    parser.add_argument('--outdir',        type=str,   default='./',            help=_h_outdir)
    parser.add_argument('--gsummary',      action='store_true',                 help=_h_rule_on_rse)
    parser.add_argument('--containers',    action='store_true',                 help=_h_containers)
    parser.add_argument('-t', '--tags',    type=str, nargs='+',                 help=_h_tags)

    return parser.parse_args()

def get_account_limits(usr):
    """
    Method to get the account limits for the user
    """
    disk_type_to_acc_limit = {}
    for disktype, info in acccl.get_global_account_limits(usr).items():
        size, units = bytes_to_best_units(info['limit'])
        disk_type_to_acc_limit[disktype.replace('type=','')] = (size, units)
    return disk_type_to_acc_limit

def run():
    """ Main method to run the script """

    # ===============  ArgParsing  ===================================
    args      = argparser()
    users     = args.usrs
    regexes   = args.regexes
    scopes    = args.scopes
    rses      = args.rses
    tags      = args.tags
    rse_info  = args.gsummary
    only_cont = args.containers
    outdir    = args.outdir
    os.makedirs(outdir, exist_ok=True)

    # Get all available RSEs
    available_rses = rsecl.list_rses()

    # Get the requested RSEs names from the regex
    req_rses = set()
    for rse in rses:
        req_rses |= get_rses_from_regex(rse, rsecl)

    # Does user want global summary?
    if rse_info:
        # loop over users
        for usr in users:
            # Dictionary to store user usage summary
            rse_user_summary = defaultdict(dict)

            print(f"INFO:: Processing usage of user {usr}")

            # Get the account limits
            disk_type_to_acc_limit = get_account_limits(usr)
            print("INFO:: General limits for user: ", usr)

            # Print the limits
            for disktype, (size, units) in disk_type_to_acc_limit.items():
                print(f"INFO:: {disktype} : {size} {units}")

            # Get the usage for each RSE
            total_used, total_limit = 0, 0
            for rse in req_rses:
                print(f"INFO:: Checking RSE {rse}")
                try:
                    usage = next(acccl.get_local_account_usage(usr, rse))
                except:
                    print(f"WARNING:: The RSE {rse} usage is not retrievable")
                    continue

                # Convert to human readable units
                used, used_units = bytes_to_best_units(usage['bytes'])
                limit, limit_units = bytes_to_best_units(usage['bytes_limit'])
                # Print the usage for the RSE
                print(f"INFO:: Usage = {used:.2f} {used_units}")
                print(f"INFO:: Limit = {limit:.2f} {limit_units}")

                # Store the usage in the dictionary
                rse_user_summary[rse] = {'used': f'{used}', 'limit': f'{limit}'}
                # Add to the total
                total_used += usage['bytes']
                total_limit += usage['bytes_limit']

            # Store the total usage too
            total_used, total_used_units = bytes_to_best_units(total_used)
            total_limit, total_limit_units = bytes_to_best_units(total_limit)
            rse_user_summary['total']    = {'used': f'{total_used:.2f} {total_used_units}', 'limit': f'{total_limit:.2f} {total_limit_units}'}
            # Write the summary to a json file
            with open(f'{outdir}/rse_usage_{usr}.json', 'w') as f:
                json.dump(rse_user_summary, f, indent=2)

    # Get the datasets
    dataset_handler = RucioDatasetHandler(regexes = regexes,
                                          rses = rses,
                                          containers = only_cont,
                                          scopes = args.scopes)

    datasets = dataset_handler.GetDatasets()


    # Dictionary mapping scopes to tags to total sizes of datasets matching the tag
    scope_to_tag_to_size = defaultdict(lambda: defaultdict(float))
    # Dictionary mapping RSEs to datasets and their individual sizes
    rse_to_dids_sizes = defaultdict(lambda: defaultdict(list))

    # Loop over scopes
    for scope, dids in datasets.items():
        # Loop over datasets
        for did in dids:

            # Check if the dataset has a rule on any of the requested RSEs
            did_has_rule_on_regex_rse = any(has_rule_on_rse(did, scope, rse, didcl) for rse in rses)
            # Skip if it doesn't
            if not did_has_rule_on_regex_rse: continue

            # Loop over tags
            for tag in tags:
                # Skip if the tag doesn't match the dataset
                if re.match(tag, did) is None: continue
                # Get the size of the dataset
                ds_size = dataset_size(did, scope, didcl)
                # Add it to the dictionary
                scope_to_tag_to_size[scope][tag] += ds_size

            # loop over RSEs
            for rse in rses:
                # Check if the dataset has a rule on the RSE
                if has_rule_on_rse(did, scope, rse, didcl):
                    # Add the dataset and its size to the dictionary
                    rse_to_dids_sizes[rse]['did'].append(f'{scope}:{did}')
                    rse_to_dids_sizes[rse]['size'].append(dataset_size(did, scope, didcl))

    # ======= Write the outputs to files ===========
    # Loop over RSEs and datasets stored on them
    for rse, dids_info in rse_to_dids_sizes.items():

        with open(f'{outdir}/rse_{rse}_dids_sizes.txt', 'w') as f:
            dids  = dids_info['did']
            sizes = dids_info['size']

            # Zip and Sort the datasets and their sizes by size
            zipped = zip(dids, sizes)
            sort   = sorted(zipped, key=lambda x: x[1], reverse=True)

            # Write the datasets and their sizes to a file (1x per line)
            for elem in sort:
                did, size = elem
                size, units = bytes_to_best_units(size)
                f.write(f'{did} {size:.2f} {units} \n')

    # Loop over scopes and tags and write the total sizes to a file
    for scope, tag_to_sizes in scope_to_tag_to_size.items():
        # Loop over the tags and their sizes
        for tag, size in tag_to_sizes.items():
            # Convert to human readable units
            size, units = bytes_to_best_units(size)
            # Update the dictionary
            scope_to_tag_to_size[scope][tag] = f'{size:.2f} {units}'

    # Write the dictionary to a json file
    with open(f'{outdir}/tag_sizes.json', 'w') as f:
        json.dump(scope_to_tag_to_size, f, indent=2)


if __name__ == '__main__':  run()