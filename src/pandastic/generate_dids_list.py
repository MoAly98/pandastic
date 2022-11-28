# Required Imports
import os
import argparse
from pathlib import Path
from collections import defaultdict
from rucio import client as rucio_client
didcl = rucio_client.didclient.DIDClient()

# HELP Messages
_h_patterns = 'The patterns to look for when extracting containers to skim from Rucio'
_h_outdir   = 'The directory where the list of containers should be dumped'
_h_suffix   = 'The unique suffix which denotes the round of production'
_h_scope    = 'The scope which should be used to look for the containers'
_h_fav      = 'Out of the patterns used to find containers, which one is preferred if DSID+TAG exist in > 1 pattern'
_h_keep     = 'After the containers list is built and favourite pattern is kept in clashes, do you want to only keep a specific pattern?\
               Useful if some DSIDs exist in >1 patterns, and you want to run them from one of the patterns, without runnig other DSIDs\
               from this pattern. In this case you should use patterns, fav and keep options together'

parser = argparse.ArgumentParser(description='Prepare a list of DIDs to process')
parser.add_argument('patterns',       nargs='+',     help=_h_patterns)
parser.add_argument('-o', '--outdir', required=True, help=_h_outdir)
parser.add_argument('-s', '--suffix', required=True, help=_h_suffix)
parser.add_argument('--scope',        required=True, help=_h_scope)
parser.add_argument('--fav',                         help=_h_fav)
parser.add_argument('--keep',         nargs = '+',   help=_h_keep)

args = parser.parse_args()

# Create the output directory in case it doesn't exist
outdir = args.outdir
os.makedirs(outdir, exist_ok=True)
# Make sure the fav and keep options are used correctly
assert args.fav in args.patterns, "Favourite pattern must be one of the patterns to look for"
assert all(k in args.patterns for k in args.keep), "Kept patterns must be one of the patterns to look for"

# Prepare objects to hold the containers for writing
done_cont, done_pat = defaultdict(list), defaultdict(list)

# Go over all patterns specified in args
for pattern in args.patterns:
    # Grab containers that contain the pattern in the provided scope
    containers = list(didcl.list_dids(args.scope, [{"name": f"*{pattern}*.root"}], did_type='container'))        
    # Loop over the containers
    for cont in containers:
        # Get a unique ID of container = DSID+TAG
        dsid = cont.split('.')[3]
        tag = cont.split('.')[6]
        unique = dsid+'_'+tag
        # Append container to list of containers with this unique ID (should be 1 entry per unique)
        done_cont[unique].append(cont)
        # Append pattern to list of patterns with this unique ID (can be more than 1 per unique where -fav- and -keep- use)
        done_pat[unique].append(pattern)

with open(f'{outdir}/list_GRID_{args.suffix}.txt', 'w') as f:
    for unique, patterns in done_pat.items():
        for i, pat in enumerate(patterns):
            if len(patterns) > 1 and pat != args.fav: continue 
            if pat not in args.keep: continue
            cont = done_cont[unique][i]
            f.write(f'{cont}') if (i == len(patterns)-1 and unique == list(done_pat.keys())[-1]) else f.write(f'{cont}\n')