# Required Imports
# System
import sys, os, re, json
import argparse
# Rucio
from rucio import client as rucio_client

def argparser():
    parser = argparse.ArgumentParser()    
    parser.add_argument('file', type=str, help='File with Rule IDs (1x per line)')

    return parser.parse_args()

def run():
    
    rcl = rucio_client.ruleclient.RuleClient()
    
    args = argparser()
    ids_file = args.file
    with open(ids_file, 'r') as f:
        ids = f.readlines()
        ids = [x.strip() for x in ids]
    
    for an_id in ids:
        rule = next(rcl.list_replication_rules(filters={'id': an_id}), None)
        if rule is None: print(f"WARNING:: No rule found with ID {an_id}... maybe it is past the rule lifetime? "); continue 
        ok_count    = rule['locks_ok_cnt']
        repl_count  = rule['locks_replicating_cnt']
        stuck_count = rule['locks_stuck_cnt']
        last_update = rule['updated_at'].strftime("On %d/%m/%Y at %H:%M:%S")
        name        = rule['name']
        print(f"==================================================================================")
        print(f"ID = {an_id}: {name}")
        print(f"    OK = {ok_count} ")
        print(f"    REPLICATING = {repl_count}")
        print(f"    STUCK = {stuck_count}")
        print(f"    Last updated: {last_update}")
        print(f"==================================================================================")

if __name__ == "__main__":  run()