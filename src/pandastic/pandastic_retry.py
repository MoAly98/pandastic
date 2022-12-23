'''
This module indetifies the task-IDs of jobs that are either exhausted, finished or failed and
retries them using pbook, with the possibility of changing options

'''

# Required Imports
# System
import sys, os, re, json
import argparse
# Panda
from pandaclient import Client
from pandaclient import PBookCore
from pandaclient import queryPandaMonUtils
# Rucio
from rucio import client as rucio_client

# Create an instance of the PBook API
pbook = PBookCore.PBookCore()


_h_regex  = 'A regex/pattern to be used to find the jobs to retry. This can be for example a suffix. This will be used with .*pattern.*'
_h_submit = 'Should the code submit the retry jobs? Default is to run dry'
_h_days   = 'How many days in the past should we look for the jobs?'
_h_user   = 'By default the tasks for the current user are the ones that will be queried. Use this option to query other users.'
_h_newargs= 'New arguments to retry the jobs with must be passed as a dictionary inside a single-quote string'

def argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--regex',       type=str, required=True, help=_h_regex)
    parser.add_argument('-d', '--days',        type=int, default=30,    help=_h_days)
    parser.add_argument('-u', '--grid-user',   type=str, default='',    help=_h_user)    
    parser.add_argument('--submit',            action='store_true',     help=_h_submit)
    parser.add_argument('--newargs',           type=str,                help=_h_newargs)

    return parser.parse_args()

def run():

    args = argparser()
    
    # /cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase/x86_64/PandaClient/1.5.9/lib/python3.6/site-packages/pandaclient/PBookCore.py
    
    user = pbook.username if args.grid_user == '' else args.grid_user
    days = args.days
    suffix = args.regex
    _, url, data = queryPandaMonUtils.query_tasks( username=user,
                                                   days=days, 
                                                   status='ready|pending|exhausted|finished|failed')

    print(f"INFO:: PanDAs query URL: {url}")                                   
    do_retry(data, suffix, args.submit, args.newargs)
    #if duplicate:   add_rule(data, suffix, args.submit, args.rse_exp)
    
def do_retry(data, suffix, submit, newargs):
    
    to_retry = []
    newargs  = json.loads(newargs)
    print(newargs)
    for datum in data:
        taskid = datum.get("jeditaskid")
        taskname = datum.get("taskname")
        status = datum.get("status")
        
        if re.match(f"^.*{suffix}/$", taskname) is None:   continue 
        to_retry.append(taskid)
        print(f"INFO:: TO RETRY: {taskname}, Status = {status}")
    
    if submit:
        for taskid in to_retryal:
            pbook.retry(taskid, newOpts=newargs)

def add_rule(data, suffix, submit, rse_expression, lifetime):
    assert rse_expression != '', 'ERROR:: Must provide valid RSE expression to add replication rule'
    rcl = rucio_client.ruleclient.RuleClient()
    didcl = rucio_client.didclient.DIDClient()

    to_repl = []
    totalsize = 0
    
    for datum in data:
        
        taskname = datum.get("taskname")
        if re.match(f"^.*_{suffix}/$", taskname) is None:   continue 
        
        inDS = set()
        for ds in datum.get("datasets"):
            if(ds.get("type")!='input'):    continue 
            cont = ds.get("containername")
            inDS.add(cont)
        
        for cont in inDS:
            # Log DS size
            scope = ".".join(cont.split(".")[:2])
            files = list(didcl.list_files(scope,cont.replace('/','')))
            totalsize += sum([file['bytes']/1e6 for file in files])

        to_repl.extend(inDS)

        status = datum.get("status")
        print(f"INFO:: TO REPLICATE: {inDS}, Status = {status}")
    
    print(f"INFO:: Will replicate {len(to_repl)} input DSs")
    
    if totalsize > 1e3 and totalsize < 1e5: 
        totalsize/=1e3
        print(f"INFO:: Total Size = {totalsize} GB")
    elif totalsize > 1e5: 
        totalsize/=1e6
        print(f"INFO:: Total Size = {totalsize} TB")
    else:
        print(f"INFO:: Total Size = {totalsize} MB")
    
    if submit:
        
        # RSE Definition
        rse_attrs = rsecl.list_rse_attributes(rse_expression)
        rse_tier  = rse_attrs['tier']
        rse_cloud = rse_attrs['cloud']
        rse_site  = rse_attrs['site']
        rse_type  = rse_attrs['type']
        rse_bool  = f'tier={rse_tier}&type={rse_type}&cloud={rse_cloud}&site={rse_site}'
        
        # Get replicating
        rule_ids = []
        for inds in to_repl:
            scope = ".".join(inds.split(".")[:2])
            ds = inds.replace('/','')
            # see: http://rucio.cern.ch/documentation/client_api/ruleclient
            try:
                rule = rcl.add_replication_rule([{'scope':scope, 'name': ds}], 1, rse_bool, lifetime = lifetime)
                print(f'INFO:: DS = {ds} \n RuleID: {rule[0]}')
                rule_ids.append(rule[0])
            except rucio.common.exception.DuplicateRule as e:
                print(f"WARNING:: Duplication already done for \n {ds} \n to {rse_expression} ...  skipping!")
                continue 
            except rucio.common.exception.ReplicationRuleCreationTemporaryFailed as tfe:
                print(f"WARNING:: Duplication not currently possible for \n {ds} \n to {rse_expression} ...  skipping, try again later!")
                continue
        
        if len(rule_ids) != 0:
            now = datetime.now()
            now = now.strftime("%Y%m%d_%H%M%S")
            with open(f'monit_replicate_inputs_{now}.txt', 'w') as monitf:
                for r in rule_ids:
                    if r != rule_ids[-1]:   monitf.write(r+'\n')
                    else:   monitf.write(r)


if __name__ == "__main__":  run()