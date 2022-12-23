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
full_dids_regex  = 'user.maly:*v26_out*.root*'
scope = full_dids_regex.split(':')[0]
did_regex = full_dids_regex.split(':')[1]
DELETE = False

rulecl = rucio_client.ruleclient.RuleClient()
didcl = rucio_client.didclient.DIDClient()

# Get DIDs:
dids = didcl.list_dids(scope, {'name':did_regex})
dids_on_regexed_rse = []
for did in dids:
    rules = didcl.list_did_rules(scope, did)
    for rule in rules:
        rse_exp = rule['rse_expression']
        rule_id = rule['id']
        if re.match(re_rse_regex, rse_exp) is None: continue
        if delete:
            rulecl.delete_replication_rule(rule_id)