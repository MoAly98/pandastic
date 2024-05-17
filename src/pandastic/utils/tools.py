#!python3

import re, json
class SetEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)

def dataset_size(ds, scope, didclient):

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
    files = list(didclient.list_files(scope,ds.replace('/','')))
    totalsize = sum([file['bytes'] for file in files])

    return totalsize

def bytes_to_best_units(ds_size, ensure=None):
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
    if ensure is not None:
        if ensure == 'GB':
            ds_size/=1e9
            return (ds_size, 'GB')
        elif ensure == 'TB':
            ds_size/=1e12
            return (ds_size, 'TB')
        else:
            ds_size/=1e6
            return (ds_size, 'MB')

    if ds_size > 1e3 and ds_size < 1e5:
        ds_size/=1e9
        return (ds_size, 'GB')
    elif ds_size > 1e5:
        ds_size/=1e12
        return (ds_size, 'TB')
    else:
        ds_size/=1e6
        return (ds_size, 'MB')

def merge_dicts(d1, d2):
    '''
    Method to merge two dictionaries. If the same key is present in both
    dictionaries, the value from the first dictionary is used.  If the value
    is a dictionary, the method is called recursively.

    Parameters
    ----------
    d1: dict
        First dictionary to merge
    d2: dict
        Second dictionary to merge

    Returns
    -------
    merged: dict
        Merged dictionary
    '''
    merged = {}

    for key in set(d1.keys()) | set(d2.keys()):
        if key in d1 and key in d2:
            if isinstance(d1[key], dict) and isinstance(d2[key], dict):
                merged[key] = merge_dicts(d1[key], d2[key])
            elif isinstance(d1[key], set) and isinstance(d2[key], set):
                merged[key] = d1[key] | d2[key]
            else:
                merged[key] = d1[key]
        elif key in d1:
            merged[key] = d1[key]
        else:
            merged[key] = d2[key]
    return merged

def sort_dict(d):
    '''
    Method to sort a dictionary by key. If the value is a dictionary, the method
    is called recursively.

    Parameters
    ----------
    d: dict
        Dictionary to sort

    Returns
    -------
    sorted_dict: dict
        Sorted dictionary
    '''
    if type(d) != dict:
        return d
    sorted_dict = {}
    for key in sorted(d.keys()):
        sorted_dict[key] = sort_dict(d[key])

    return sorted_dict

def nested_dict_equal(d1, d2):
    """
    Check if all keys and values of two nested dictionaries are equal.

    Parameters:
        d1: dict
            First nested dictionary.
        d2: dict
            Second nested dictionary.

    Returns:
        bool: True if all keys and values are equal, False otherwise.
    """
    if d1.keys() != d2.keys():
        return False
    for key in d1.keys():
        if type(d1[key]) != type(d2[key]):
            return False
        if isinstance(d1[key], dict):
            if not nested_dict_equal(d1[key], d2[key]):
                return False
        elif d1[key] != d2[key]:
            return False
    return True

def get_camp(string):
    '''
    Method to get the campaign from a dataset name. The method looks
    for r9364, r10201, r10724 in the dataset name and returns the
    corresponding campaign. If none of these are found, 'unknown' is
    returned.

    Parameters
    ----------
    string: str
        Dataset/Job name to get the campaign from

    Returns
    -------
    campaign: str
        Campaign name. Can be 'mc16a', 'mc16d', 'mc16e' or 'unknown'.

    '''

    if   re.findall("r9364",  string) != []: return 'mc16a'
    elif re.findall("r10201", string) != []: return 'mc16d'
    elif re.findall("r10724", string) != []: return 'mc16e'
    else:   return 'unknown'

def get_dsid(string):
    '''
    Method to get the DSID from a dataset name. The method looks
    for a 6 digit number in the dataset name and returns it.

    Parameters
    ----------
    string: str
        Dataset/Job name to get the DSID from

    Returns
    -------
    dsid: str
        DSID of the dataset
    '''
    dsid = re.findall(r"\D(\d{6})\D", string)
    dsid = dsid[0]

    return dsid

def get_tag(string):
    '''
    Method to get the tag from a dataset name. The method looks
    for a string of the form e1234_a/s1234_r1234_p1234 in the dataset
    name and returns it.

    Parameters
    ----------
    string: str
        Dataset/Job name to get the tag from

    Returns
    -------
    tag: str
        Tag of the dataset
    '''
    tag = re.findall(r'e\d+_[as]\d+_r\d+_p\d+', string)
    tag = tag[0]
    return tag

def progress_bar(items, processed_items=0, bar_length=20, msg='Progress'):
    '''
    Displays a progress bar.

    Parameters
    ----------
    items: int
        Total number of items.
    processed_items: int
        Number of processed items (default=0)
    bar_length: int
        Length of the progress bar in characters (default=20).
    '''
    percent = processed_items / items
    hashes = '#' * int(percent * bar_length)
    spaces = ' ' * (bar_length - len(hashes))
    print(f"\r{msg}: [{hashes}{spaces}] {percent*100:.2f}%", end="\n")

def draw_progress_bar(numitems, current_item, msg = 'Progress'):
    '''
    Displays a progress bar.

    Parameters
    ----------
    numitems: int
        Total number of items.
    current_item: int
        Number of processed items (default=0)
    msg: str
        Message to display before the progress bar (default='Progress')
    '''

    if current_item == 0: progress_bar(numitems, current_item, msg=msg)

    if (numitems > 1000):
        if (current_item+1)%1000 == 0: progress_bar(numitems, current_item+1, msg=msg)
    elif (numitems > 100) and numitems <= 1000:
        if (current_item+1)%100 == 0: progress_bar(numitems, current_item+1, msg=msg)
        if (current_item+1)%10 == 0:  progress_bar(numitems, current_item+1, msg=msg)
    else:
        progress_bar(numitems, current_item+1, msg=msg)

def get_lines_from_files(files):
    '''
    Method to get the lines from a list of files in one list

    Parameters
    ----------
    files: list
        list of files containing information (one per line)

    Returns
    -------
    all_lines: list
        list of lines from all files
    '''
    all_lines = []
    for f in files:
        with open(f,'r') as f:  all_lines.extend(f.readlines())
    all_lines = [l.strip() for l in all_lines]
    return all_lines
