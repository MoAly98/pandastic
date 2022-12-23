# pandastic/pandiculate

## The Wishlist
- Find failed/exhausted/finished jobs and retry them
- Find a readable summary of the error thrown for these jobs
- Be able to copy over input datasets to sites in case there are issues in current site (create rules)
- Be able to copy over output datasets to RSEs for longer shelf life (create rules)
- Montior rule creation in an intuitive way
- Paths on Tier 2

## The Status

- `pandastic_retry.py` to retry jobs (not resubmit) with possibly different settings
- `pandastic_replicate.py` to create rules for both output and input datasets
- `pandastic_whats_on_rse.py` is in dev, but it is working to give a summary of a user usage on RSEs and prints out a list of datasets following a regex which are stroed on specific regexed RSEs. It also summarises the sizes of files which match certain patterns that are subpatterns of the main dataset regex. 
- `pandastic_update_rules.py` is in dev, but it is working to take some datasets regex and a regex of RSEs, finds the rules for the regexed DIDs on the regexed RSEs and changes their options (currently only lifetime supported)
- `pandastic_delete_rules.py` is in dev, it also works like `pandastic_update_rules.py` but to delete a rule. Purging replicas is optional and not implemented.
- `pandastic_findpath.py` is coming up, and will allow user to find paths of files for a given regexed DID on Tier2 using protocol of choice. Example in use by Mo and Zak. 
- `pandastic_download.py` is coming up, and will allow user to downlaod both ntuples and logs from rucio. Currently this is a script borrowed from tH analysis. 