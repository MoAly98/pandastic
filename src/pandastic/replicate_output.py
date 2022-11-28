import argparse
from pandaclient import Client
import sys, os, re
from pandaclient import PBookCore
from pandaclient import queryPandaMonUtils
from rucio import client as rucio_client
import rucio
from datetime import datetime

pbook = PBookCore.PBookCore()

def argparser():
    parser = argparse.ArgumentParser()    
    parser.add_argument('-s', '--suffix',      type=str,   required=True)
    parser.add_argument('-r', '--rse-exp',     type=str,   required=True, help="tier=1&type=SCRATCHDISK&cloud=US")
    parser.add_argument('-d', '--days',        type=int,   default=30)
    parser.add_argument('-u', '--grid-user',   type=str,   default='')
    parser.add_argument('-l', '--life',        type=int,   default = 3600, help= 'How long to keep it for, in seconds')
    parser.add_argument('--did',               type=str,)
    parser.add_argument('--submit',            action='store_true')
   
    return parser.parse_args()

def run():
    args = argparser()

    # /cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase/x86_64/PandaClient/1.5.9/lib/python3.6/site-packages/pandaclient/PBookCore.py    
    user   = pbook.username if args.grid_user == '' else args.grid_user
    days   = args.days
    suffix = args.suffix
    did    = args.did
    _, url, data = queryPandaMonUtils.query_tasks( username=user,
                                                   days=days, 
                                                   status='done'
                                                )

    print(f"INFO:: PanDAs query URL: {url}")                                   
    add_rule(data, suffix, args.submit, args.rse_exp, args.life, did)

def add_rule(data, suffix, submit, rse_expression, lifetime, did):

    rcl = rucio_client.ruleclient.RuleClient()
    didcl = rucio_client.didclient.DIDClient()
    rsecl = rucio_client.rseclient.RSEClient()
    to_repl = []
    totalsize = 0
    for datum in data:
        taskname = datum.get("taskname")
        if re.match(f"^.*_{suffix}/$", taskname) is None:   continue

        outDS = set()
        for ds in datum.get("datasets"):
            if(ds.get("type")!='output'):    continue 
            cont = ds.get("containername")
            if did is not None:
                if re.match(did.replace('*','.*'), cont) is None: continue
            outDS.add(cont)
        if len(outDS) == 0: continue
        for cont in outDS:
            # Log DS size
            scope = ".".join(cont.split(".")[:2])
            files = list(didcl.list_files(scope,cont.replace('/','')))
            totalsize += sum([file['bytes']/1e6 for file in files])
    
        to_repl.extend(outDS)
        
        status = datum.get("status")
        print(f"INFO:: TO REPLICATE: {outDS}, Status = {status}")

    print(f"INFO:: Will replicate {len(to_repl)} output DSs")
    if totalsize > 1e3 and totalsize < 1e5: 
        totalsize/=1e3
        print(f"INFO:: Total Size = {totalsize:2f} GB")
    elif totalsize > 1e5: 
        totalsize/=1e6
        print(f"INFO:: Total Size = {totalsize:.2f} TB")
    else:
        print(f"INFO:: Total Size = {totalsize:.2f} MB")

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
        for outds in to_repl:
            scope = ".".join(outds.split(".")[:2])
            ds = outds.replace('/','')
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

        if len(rule_ids) != 0:
            now = datetime.now()
            now = now.strftime("%Y%m%d_%H%M%S")
            with open(f'monit_replicate_output_{now}.txt', 'w') as monitf:
                for r in rule_ids:
                    if r != rule_ids[-1]:   monitf.write(r+'\n')
                    else:   monitf.write(r)

if __name__ == "__main__":  run()