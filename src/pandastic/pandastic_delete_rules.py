'''
The idea here is to find how much space a user is using on a given RSE or groups of RSEs and which Datasets are
using up this space
'''

# Required Imports
# System
import sys, os, re, json
import argparse
# Rucio
from rucio import client as rucio_client

_h_regex  = 'A regex/pattern in the taskname to be used to find the jobs to retry. This can be for example a suffix. This will be used with .*pattern.*'
_h_rses   = 'A list of RSEs to delete the DID replicas from'
_h_scopes = 'Scopes to look for the DIDs in'
_h_submit = 'Should the code submit the deletion jobs? Default is to run dry'
_h_paranoid = 'Flag will only delete dataset if it has replicas that exists on >1 RSE'

def argparser():
    parser = argparse.ArgumentParser()    
    parser.add_argument('-s', '--regex',       type=str,    required=True, help = _h_regex)
    parser.add_argument('-r', '--rses',        nargs='+',  required=True,  help = _h_rses)
    parser.add_argument('--scopes',             nargs='+',  required=True,  help= _h_scopes)
    parser.add_argument('--paranoid',          action='store_true',        help= _h_paranoid)
    parser.add_argument('--submit',            action='store_true')
   
    return parser.parse_args()


def run():
    args = argparser()
    did_regex = args.regex
    rses = args.rses
    scopes = args.scopes
    submit = args.submit
    paranoid = args.paranoid
    
    rulecl = rucio_client.ruleclient.RuleClient()
    didcl = rucio_client.didclient.DIDClient()
    ndeleted = 0
    for scope in scopes:
        print(f"INFO:: Looking in the scope {scope} for the pattern {did_regex}")
        dids = list(didcl.list_dids(scope, {'name': did_regex}))
        if len(dids) == 0: 
            print(f"WARNING:: Pattern {did_regex} not found in scope {scope}, moving to next scope")
            continue
        
        for did in dids:
            rules = list(didcl.list_did_rules(scope, did))

            if paranoid:
                if len(rules) < 2:
                    print(f"INFO:: Paranoid flag is on and DID {did} has < 2 rules, not deleting it..")
                    continue

            for rule in rules:
                rse_exp = rule['rse_expression']
                rule_id = rule['id']
                
                if all(re.match(rse_to_clear, rse_exp) is None for rse_to_clear in rses): continue

                print(f"INFO:: I will delete the replica of {did} with rule ID: {rule_id} which lives on RSE: {rse_exp}")

                ndeleted += 1
                if submit:
                    print("Actually deleting...")
                    rulecl.delete_replication_rule(rule_id, purge_replicas=True)
    
    print(f"INFO:: Total Number of Deletions is {ndeleted}")

if __name__ == "__main__":  run()
