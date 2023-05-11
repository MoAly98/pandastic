from collections import defaultdict
import re
def list_replicas(did, scope, rses, replcl):
    '''
    Get the list of file replicas from a list of rucio datasets

    Parameters
    ----------
    did:
        The dataset to list replicas for
    scope:
        The scope of the did
    rses:
        The regexes for RSEs to look for replicas in
    replcl:
        The Replica Client

    Returns
    -------
    all_files : dict
        Dictionary mapping each file to its available replicas
    '''

    # Mapping to hold the file paths
    all_files = defaultdict(set)

    # ======================================== #
    freplicas = None
    # Retry 3 times to get the list of replicas
    for retry in range(3):
        try:
            #  Get the list of replicas from scope and DID
            freplicas = list(replcl.list_replicas([{'scope': scope, 'name': did.replace('/','')}]))
            break
        except Exception:
            continue

    if freplicas is None:
        print(f"ERROR:: Failed to access replica client")
        exit(1)

    # If no replicas found, skip dataset
    if len(freplicas) == 0:
        print(f" WARNING:: No file replicas found for dataset {did}")
        return {}

    # Prepare a set to hold all file names found to use it later to check if all files
    # available in dataset had their paths saved

    all_fnames = set()
    # Loop over the file replicas
    for replica in freplicas:
        # Name of the file
        fname = replica['name']
        all_fnames.add(fname)
        # Rses where file is stored, and status of replica on those rses
        rse_to_files  = replica['rses']
        rse_to_status = replica['states']
        # Loop over RSEs, and paths to files on those RSEs
        for rse, files in rse_to_files.items():
            # replace davs with root and remove port number
            if any("davs:" not in f and "root:" not in f for f in files):
                print(f"WARNING:: File {name} has a replica which is not available in davs/root protocol.. skipping replica")

            files = [f for f in files if ("davs:" in f or "root:" in f)]
            files = [f.replace("davs://", "root://") for f in files]
            files = [re.sub(r':[0-9]+/', '://', f) for f in files]

            # If RSE is not available, skip it
            if rse_to_status[rse] != 'AVAILABLE': continue
            # If RSE is in the list of RSEs to look for replicas, keep it
            if rses != [] and any(re.match(rgx, rse) for rgx in rses):
                all_files[fname] |= set(files)
            # If no RSEs are specified, save all replicas
            elif rses == []:
                all_files[fname] |= set(files)
            # If RSE is not in the list of RSEs to look for replicas, skip it
            else:   continue

    # Check if all files in dataset had their paths saved
    fileskept  = set(all_files.keys())
    filesavail = all_fnames
    if len(all_files) != len(all_fnames):
        print(f"WARNING:: Files which belong to DID {did} were not used because they are not in the RSEs specified by the user or no davs/root protocol !!")
        print(f"WARNING:: Consider relaxing the RSE regexes or adding the following files to the RSEs: {filesavail - fileskept}")

    return dict(all_files)