# Pandastic - Connecting Rucio and PanDA

`Pandastic` is a simple command line tool which utilises the PanDA and Rucio clients to allow the user to perform operations on PanDA tasks and their associated input and output datasets which live on rucio.
The PanDA-Rucio bridge is a main feature of `Pandastic`, but you don't have to always use it.
`Pandastic` allows you to search and manipulate Rucio datasets and PanDA tasks independently.

## Getting the Package

The tool is available with `pip` and can be downloaded with

```
pip install pandastic
```

**Note:: you must be able to generate a VOMS-proxy to manipulate datasets with ATLAS. On `lxplus`, you will be prompted for your Grid certificate passphrase. On a local machine, you need to install VOMS**

You may be prompted to create a rucio configuration.
In this case, you should create a file in the prompted location with the content:
```
[client]
rucio_host = https://rucio-lb-prod.cern.ch
auth_host = https://atlas-rucio-auth.cern.ch
ca_cert = /etc/grid-security/certificates/
account = <rucio_account>
auth_type = x509_proxy
```
or equivalent settings for different authentication method, or non-ATLAS host.
If you are on lxplus, or have access to the ATLAS environment setup, simply running `setupATLAS -q && lsetup rucio` will achieve the same result. 

## What can you do with the package:

### For datasets:

- You can retrieve datasets under some scope with a `regex` pattern
- You can retrieve datasets that are the inputs/outputs of grid tasks (specified by a `regex` pattern, from a given user) with a task status of choice
    - Support is avaiabe for Production tasks with extra flag
- You can retrieve datasets under some scope with a `regex` pattern that
    - specifically have/not have rules or replicas on specific RSEs
    - whose history never involvled a rule on a particular RSE/site
    - whose rules have a particular time left in their lifetime
    - whose containers respect those criteria

Once the list of datasets is is retrieved, you can perform one of the following actions on each task:

- Create, extend or delete rules associated to dataset or its container
- List the files within the dataset (to access them later with `XrootD` or `davs` protocoles)
- Download the datasets to a specified destination

### For tasks:

- You can retrieve tasks with a status of choice for some user with a `regex` pattern, looking back `N` days

Once the list of tasks is is retrieved, you can perform one of the following actions on each task:

- Pause/unpause the task
- Retry the task (optionally with new arguments)
- Kill the task

### Some general comments:

- You don't have to search for the tasks/datasets you would like to manipulate on-the-fly. You can simply provide a list of tasks or files to perform the actions over.

- Regex here is not the linux globbing regex. It is the UNIX-wide regexing syntax, where a wildcard is given by `.*`.


### Features to look forward to

- Ability to montior rule creation in an intuitive way
- Ability to build a nicely presented report of a user's usage on any RSE, including datasets with rules occupying spaces.
- Build a table of statuses of `PanDA` tasks differentiated by a pattern in their name
- Ability to upload datasets to Rucio
- More color and proper logging!
- Download the logs for jobs failed in given task

## Usage

### Manipulating datasets

The actions available for a given dataset are:

- *find*:      used to just dump list of datasets with no action
- *replicate*: used to create new rules for datasets
- *update*:    used to increase the lifetime for rules
- *delete*:    used to delete rules
- *download*:  used to download datasets

You can then use the command line tool to perform one of these actions (`<action>`) with some filter on the dataset specified by the arguments `<args>`:

```
pandastic-data <action> <args>
```

The required and allowed `<args>` are extensively described in their respecitive help messages. You shoud have a look on the availablle arguments with
```
pandastic-data --help
```
*A tabulated summary coming here soon!*

### Manipulating Tasks

The actions available for a given task are:

- *find*:       used to just dump list of tasks with no action
- *pause*:      used to pause tasks
- *unpause*:    used to unpause tasks
- *retry*:      used to retry tasks
- *kill*:       used to kill tasks

You can then use the command line tool to perform one of these actions (`<action>`) with some filter on the dataset specified by the arguments `<args>`:

```
pandastic-tasks <action> <args>
```

The required and allowed `<args>` are extensively described in their respecitive help messages. You shoud have a look on the availablle arguments with
```
pandastic-tasks --help
```
*A tabulated summary coming here soon!*
