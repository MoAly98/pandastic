'''
The idea here is to find how much space a user is using on a given RSE or groups of RSEs and which Datasets are
using up this space
'''
from rucio import client as rucio_client
import rucio
import re
from collections import defaultdict
from pprint import pprint

username = 'maly'
rse_regex = '*SCRATCHDISK*'
re_rse_regex = rse_regex.replace('*', '.*')
full_dids_regex  = 'panda:*v26*'
scope = full_dids_regex.split(':')[0]
did_regex = full_dids_regex.split(':')[1]
check_tags_size = ['.*v26_out.*','.*v26truth.*','.*v26_log.*','.*v26.log.*']
GET_RSE_INFO = True

rsecl = rucio_client.rseclient.RSEClient()
acccl = rucio_client.accountclient.AccountClient()
didcl = rucio_client.didclient.DIDClient()
replicacl = rucio_client.replicaclient.ReplicaClient()

# Get all RSEs
available_rses = rsecl.list_rses()

if GET_RSE_INFO:
    # Account limits
    disk_type_to_acc_limit = {}
    for disktype, info in acccl.get_global_account_limits(username).items():
        disk_type_to_acc_limit[disktype.replace('type=','')] =  info['limit']/1e12

    print("INFO:: Account limits on various types of disks: ")
    pprint(disk_type_to_acc_limit)


    rses_to_check = []
    rse_user_summary = defaultdict(dict)
    total_used = 0.
    for rse in available_rses:

        rsename = rse['rse']
        
        if re.match(re_rse_regex, rse['rse']) is None: continue 
        rses_to_check.append(rsename)

        usage = next(acccl.get_local_account_usage(username, rsename), None)
        if usage is None:
            print(f"WARNING:: The RSE {rsename} usage is not retrievable")
            continue
        usage = usage['bytes']
        limit = next(acccl.get_local_account_usage(username, rsename), None)['bytes_limit']
        
        total_used += usage/1e12
        
        rse_user_summary[rsename] = {'used': f'{usage*100/limit:.2f} %', 'limit': f'{limit/1e12:.2f} TB'}


    rse_user_summary['Total'] = {'used': total_used}
    print("INFO:: Current usage on regexed RSEs: ")
    pprint(rse_user_summary)
    print(f"INFO:: Total usage on regexed RSEs: {total_used:.2f} TB")


# dids = list(didcl.list_dids(scope, {'name':'*root*'}))

# for did in dids:
#     replicas = replicacl.list_replicas([{'scope': scope, 'name': did}])

#     for i, repl in enumerate(replicas):
#         rses = repl.get('rses')
#         for rse in rses:
#             if rse in rses_to_check:
#                 if 'v26' not in did:
#                     print(did, rse)

# print("DONE CHECKING NON V26 DATA")
# Get DIDs:
dids = list(didcl.list_dids(scope, {'name':did_regex}))
dids_on_regexed_rse = []

size_with_tag = {tag: {'size': 0., 'unit': 'MB'} for tag in check_tags_size}
for did in dids:
    rules = didcl.list_did_rules(scope, did)
    for rule in rules:
        rse_exp = rule['rse_expression']
        if re.match(re_rse_regex, rse_exp) is None: continue
        

        for tag in check_tags_size:
            if re.match(tag, did):
                files = didcl.list_files(scope, did)
                size_with_tag[tag]['size'] += sum([file['bytes']/1e6 for file in files])
                
                dids_on_regexed_rse.append(did)

# for did in dids:
#     rules = didcl.list_did_rules(scope, did)
#     for rule in rules:
#         rse_exp = rule['rse_expression']
#         if re.match(re_rse_regex, rse_exp) is None: continue
#         if did not in dids_on_regexed_rse:
#             print(did)

for tag, size_and_units in size_with_tag.items():
     size = size_and_units['size']
     if size < 1e5 and size > 1e2:
         size_and_units['size'] /= 1e3
         size_and_units['unit'] = 'GB'
     elif size > 1e5:
         size_and_units['size'] /= 1e6
         size_and_units['unit'] = 'TB'
         

pprint(size_with_tag)
#pprint(dids_on_regexed_rse) 
