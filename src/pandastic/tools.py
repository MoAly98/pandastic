import re
def dataset_size(dses, didclient):

    '''
    Method to compute the size of a dataset in bytes.
    This is done by summing the size of all files in the dataset.
    The files are listed using a DID client.

    Parameters:
    -----------
    dses: list
        List of datasets to compute the size of
    didclient: rucio.client.didclient.DIDClient
        DID client to use to list the files in the dataset

    Returns:
    --------
    totalsize: int
        The total size of the datasets in bytes

    '''

    totalsize = 0
    for ds in dses:
        scope = ".".join(ds.split(".")[:2])
        files = list(didclient.list_files(scope,ds.replace('/','')))
        totalsize += sum([file['bytes'] for file in files])

    return totalsize

def bytes_to_best_units(ds_size):
    '''
    Method to convert bytes to the best units for display.
    This is done by dividing the size by 1e3, 1e6, or 1e9 depending on the size.
    The units are then returned as a string in a tuple along with the size.

    Parameters:
    -----------
    ds_size: int
        The size of the dataset in bytes

    Returns:
    --------
    (ds_size, units): tuple
        The size of the dataset in the best units and the units as a string
    '''
    if ds_size > 1e3 and ds_size < 1e5:
        ds_size/=1e9
        return (ds_size, 'GB')
    elif ds_size > 1e5:
        ds_size/=1e12
        return (ds_size, 'TB')
    else:
        ds_size/=1e6
        return (ds_size, 'MB')

def has_rule_on_rse(did, scope, rse, didcl):
    '''
    Method to check if a dataset has a rule on a given RSE

    Parameters
    ----------
    did: str
        Name of the dataset to check
    scope: str
        Scope of the dataset to check
    rse: str
        Name of the RSE to check

    Returns
    -------
    has_rule: bool
        True if the dataset has a rule on the RSE, False otherwise
    '''

    # Find existing rules for the given did
    rules    = didcl.list_did_rules(scope, did.replace('/',''))

    for rule in rules:
        if re.match(rse, rule.get("rse_expression")) is not None:
            return True

    # If here, no rule found on RSE
    return False

def has_replica_on_rse(did, scope, rse, replicacl):
    '''
    Method to check if a dataset has a replica on a given RSE. This
    is done by checking if all the files in the dataset have a replica
    on the RSE.

    Parameters
    ----------
    did: str
        Name of the dataset to check
    scope: str
        Scope of the dataset to check
    rse: str
        Name of the RSE to check

    Returns
    -------
    has_replica: bool
        True if the dataset has a replica on the RSE, False otherwise
    '''

    file_replicas = list(replicacl.list_replicas([{'scope': scope, 'name': did.replace('/','')}]))

    file_replica_is_on_req_rse = []

    for file_replica in file_replicas:
        fname = file_replica.get("name")
        file_rses = list(file_replica.get("rses").keys())
        if any(re.match(rse, frse) is not None for frse in file_rses):
            file_replica_is_on_req_rse.append(True)
        else:
            file_replica_is_on_req_rse.append(False)

    if all(file_replica_is_on_req_rse): return True
    else: return False

def get_rses_from_regex(rse_regex, rsecl):
    '''
    Method to get a list of RSEs from a list of RSEs that match a regex

    Parameters
    ----------
    rse_regex: str
        Regex to match RSEs against
    rsecl: rucio.client.rseclient.RSEClient
        RSE client to use to get the list of RSEs

    Returns
    -------
    matching_rses: list
        set of RSEs that match the regex
    '''

    matching_rses = set()

    available_rses = rsecl.list_rses()
    for avail_rse in available_rses:
        print(avail_rse.get('rse'), rse_regex)
        if re.match(rse_regex, avail_rse.get('rse')) is not None:
            matching_rses.add(avail_rse.get('rse'))
    return matching_rses