# Pandastic - Connecting Rucio and PanDA

Pandastic is a simple tool which utilises the PanDA and Rucio python clients to allow the user to perform a wide-scale
of operations on PanDA tasks and their associated input and output datasets which live on rucio. User's don't have
to always use this PanDA-Rucio bridge, since the package will allow you to also manipulate Rucio datasets and PanDA tasks
independently in a smooth way.

## What can you do with the package:

The following is a non-exhaustive list of things you can do:

- You can create, extend and delete rules or download datasets using Rucio. You can compile the list of rules using 3 methods:
    - Provide an explicit list od datasets
    - Use a regex on some PanDA tasks and use their associated input or output datasets
    - Use a regex on some rucio datasets to be grabbed from rucio

- You can upload datasets to rucio from local directories
- You can retry, kill, pause or unpause tasks on the grid. You can compile the list of tasks using 2 methods:
    - Regex of PanDA tasks
    - Explicit list of PanDA tasks

- You can build a report of a users usage on RSEs and datasets with rules occupying spaces.
- You can build a table of statuses of jobs with various suffixes/tags.
- You can create a mapping from rucio dataset name to exact location of all its files on RSEs so that
you can access them with `xrootd` or `davs`.

The real power of the package comes in when you want to customise exactly what to look for...

For example, when manipulating dataset rules, you may only want to manipulate datasets
with rules/replicas on certain sites, or no rules/replicas on some other sites. You might
also want to go stricter and only select datasets that has never had rules on a given site.

There is a lot more options and use-cases, which are best explored by looking at the usage
instructions for the various modules as will be described below.


## Getting the Package

Currently the package must be downlaoded from github manually, but soon it will be made available on PyPi.

To get the tool you should clone this repository to your machine.

## Setup

The tool assumes you have CVMFS installed and `setupATLAS` command ready. You will also need a grid certificate
and a VOMS-proxy. The following assumes you are in the repository directory:

You sould source the environment setup:

```
source lx_env.sh
```

To run the modules, you should go in the source files directory:

```
cd src/pandastic/
```

## Usage

### Manipulating datasets
The module responsible for manipulating datasets is `pandastic_data_manager.py`

To use the module, you should run

```
python3 pandastic_data_manager.py <action> <args>
```

The potential `<action>`s are:
- *replicate:* used to create new rules for datasets
- *update:*    used to increase the lifetime for rules
- *delete:*    used to delete rules
- *download:*  used to download datasets

The required `<args>` are:


### Manipulating Tasks


### Reports


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