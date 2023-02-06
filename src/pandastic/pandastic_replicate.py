# Required Imports
# System
import sys, os, re, json
import argparse
from datetime import datetime
# Rucio
from rucio import client as rucio_client
import rucio


_h_regex    = 'A regex in the rucio DID to be used to find datasets to be replicated'
_h_rses     = 'The RSE to replicate to'
_h_rsesfrom = 'List of RSEs that the DID must have replcias on *any* of them before we replicate it (e.g. you only want to replicate from SCRATCH)'
_h_type     = 'Type of dataset being replicated .. is it the task input or output?'
_h_days     = 'The number of days in the past to look for jobs in'
_h_user     = 'The grid username under for which the jobs should be searched if its not your jobs'
_h_life     = 'How long is should the lifetime of the dataset be on its destination RSE'
_h_did      = 'Subset of the jobs following the pattern to keep'
_h_submit   = 'Should the code submit the replication jobs? Default is to run dry'

def argparser():
    parser = argparse.ArgumentParser("This is used to replicate datasets independently of GRID jobs")    
    parser.add_argument('-s', '--regex',       type=str,   required=True,   help=_h_regex)
    parser.add_argument('-r', '--rseto',       type=str,   required=True,   help=_h_rses)
    parser.add_argument('--type',              type=str,   required=True,   choices=['OUT','IN'], help=_h_type)
    parser.add_argument('-d', '--days',        type=int,   default=30,      help=_h_days)
    parser.add_argument('-u', '--grid-user',   type=str,   default=pbook.username,  help=_h_user)
    parser.add_argument('-l', '--life',        type=int,   default = 3600,  help= _h_life)
    parser.add_argument('--did',               type=str,                    help=_h_did)
    parser.add_argument('-f', '--rsefrom',     nargs='+',                   help=_h_rses)
    parser.add_argument('--submit',            action='store_true',         help=_h_submit)
   
    return parser.parse_args()

def find_datasets(data, regex, type, rseto, rsefrom):
    '''
    Method to find the relevant jobs and associated datasets
    that need to be replicated. 
    '''
    if cont_type == 'OUT':  look_for_type = 'output'
    else:   look_for_type = 'input'
    
    # List to hold names of DIDs to replicate
    to_repl = []

    # Loop over the jobs found on the grid
    for datum in data:
        # Skip jobs that don't match the required regex
        taskname = datum.get("taskname")
        if re.match(f"^{regex}$", taskname) is None:    continue
        
        # Datasets to replicate
        DSes = set()
        # Loop over datasets associated with the job
        for ds in datum.get("datasets"):
            # Skip the type of dataset we don't care about
            if(ds.get("type")!=look_for_type):  continue 
            
            # Get the name of the dataset and extract scope from it
            cont = ds.get("containername")
            scope = ".".join(cont.split(".")[:2])

            # Check if name we found matches extra restriction
            if did is not None:
                if re.match(did, cont) is None: continue

            # Find existing rules for the dataset that would be replciated
            existing_rules = didcl.list_did_rules(scope, cont.replace('/',''))

            rule_exits = False
            rule_in_rsefrom = False
            for er in existing_rules:
                # Skip dataset if it has a rule which exists on the requested destination RSE
                if re.match(rseto, er['rse_expression']) is not None:
                    rule_exits = True
                    break
                
                # Skip dataset if is not on an rse that it should be on, if any specified
                if rsefrom is None: continue
                if any(re.match(rse, er['rse_expression']) is not None for rse in rsefrom):
                    rule_in_rsefrom = True
                    break
            
            if (not rule_exits) and rule_in_rsefroms:
                DSes.add(cont)
            else:
                continue
        
        # Skip this job if it doesn't have any relevant datasets   
        if len(DSes) == 0: continue
        # Save datasets to replicate if they pass all requirements
        to_repl.extend(DSes)
        # Tell the user what they are signing up for
        jobstatus = datum.get("status")
        print(f"INFO:: TO REPLICATE: {DSes}, Job Status = {jobstatus}")

    return to_repl

def dataset_size(dses):
    
    # The total size of the files being replciated (useful in dry-run, too late in active ones)
    totalsize = 0
    for cont in DSes:
        scope = ".".join(cont.split(".")[:2])
        files = list(didcl.list_files(scope,cont.replace('/','')))
        totalsize += sum([file['bytes']/1e6 for file in files])

    return totalsize


def add_rules(to_repl, rse_expression, lifetime):
    '''
    Method to add rules 
    '''
        
    # RSE Definition
    rse_attrs = rsecl.list_rse_attributes(rse_expression)
    rse_tier  = rse_attrs['tier']
    rse_cloud = rse_attrs['cloud']
    rse_site  = rse_attrs['site']
    rse_type  = rse_attrs['type']
    rse_bool  = f'tier={rse_tier}&type={rse_type}&cloud={rse_cloud}&site={rse_site}'
    
    # Get replicating
    rule_ids = []
    for ds in to_repl:
        scope = ".".join(ds.split(".")[:2])
        ds = ds.replace('/','')
        # see: http://rucio.cern.ch/documentation/client_api/ruleclient
        try:
            rule = rcl.add_replication_rule([{'scope':scope, 'name': ds}], 1, rse_expression, lifetime = lifetime)
            print(f'INFO:: DS = {ds} \n RuleID: {rule[0]}')
            rule_ids.append(rule[0])
        except rucio.common.exception.DuplicateRule as de:
            print(f"WARNING:: Duplication already done for \n {ds} \n to {rse_expression} ...  skipping!")
            continue 
        except rucio.common.exception.ReplicationRuleCreationTemporaryFailed as tfe:
            print(f"WARNING:: Duplication not currently possible for \n {ds} \n to {rse_expression} ...  skipping, try again later!")
            continue
    
    return rule_ids

def save_rules_ids(rules_ids):
    if len(rule_ids) != 0:
        now = datetime.now()
        now = now.strftime("%Y%m%d_%H%M%S")
        with open(f'monit_replicate_output_{now}.txt', 'w') as monitf:
            for r in rule_ids:
                if r != rule_ids[-1]:   monitf.write(r+'\n')
                else:   monitf.write(r)

def run():

    # Set up Rucio clients
    rcl = rucio_client.ruleclient.RuleClient()
    didcl = rucio_client.didclient.DIDClient()
    rsecl = rucio_client.rseclient.RSEClient()

    args = argparser()    
    user   = args.grid_user
    days   = args.days
    regex  = args.regex.replace("*",".*")
    if args.rsefrom is not None:
        rsefrom = [r.replace("*",".*") for r in args.rsefrom]
    else:  rsefrom = None 
    did    = args.did.replace("*",".*")
    
    # Find all PanDA jobs that are done for the user and period specified
    _, url, data = queryPandaMonUtils.query_tasks( username=user,
                                                   days=days, 
                                                   status='done'
                                                )
    # Tell the user the search URL if they want to look
    print(f"INFO:: PanDAs query URL: {url}")
    # What am I replicating
    to_repl = find_datasets(data, regex, args.type, args.rseto, rsefrom)
    
    totalsize = dataset_size(to_repl)
    print(f"INFO:: Will replicate {len(to_repl)} datasets")
    if totalsize > 1e3 and totalsize < 1e5: 
        totalsize/=1e3
        print(f"INFO:: Total Size = {totalsize:2f} GB")
    elif totalsize > 1e5:
        totalsize/=1e6
        print(f"INFO:: Total Size = {totalsize:.2f} TB")
    else:
        print(f"INFO:: Total Size = {totalsize:.2f} MB")
    
    if  args.submit:
        # Add rules
        rules_ids = add_rules(to_repl, args.rseto, args.life )
        save_rules_ids(rules_ids)

if __name__ == "__main__":  run()
