import os, json, re, urllib3
import argparse
# Rucio
from rucio import client as rucio_client
import rucio.client.uploadclient as uploadclient
# Pandastic
from utils.tools import ( dataset_size, bytes_to_best_units, draw_progress_bar, get_lines_from_files )
from utils.common import ( get_rses_from_regex )

from actions.upload_actions import ( upload_file )


# ===============  Rucio Clients ================
uploadcl  = uploadclient.UploadClient()
rsecl      = rucio_client.rseclient.RSEClient()

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ===============  ArgParsing  ===================================
# ===============  Arg Parser Help ===============================
_h_dirs      = 'Directories to look in for files to upload.\
                The files are expected to be in stored as <dataset>/<file>'
_h_regex     = 'Regex to match files to upload'
_h_rses      = 'RSEs to upload to'
_h_life      = 'Lifetime that should be given to uploaded file (in seconds)'
_h_scopes    = 'Scope to upload to'
_h_dsmap     = 'A JSON file containing mapping from desired dataset names to \
                complete file paths that should be uploaded into the dataset'
_h_outdir    = 'Output directory for the output files. Default is the current directory'
_h_fromfiles = 'Files containing lists of datasets to process filtered by regex.\n \
                Names are assumed to be <dataset>/<file> unless --nods flag is used'
_h_submit    = 'Should the code submit the upload jobs? Default is to run dry'
_h_filtds    = 'Use the regex to filter dataset names instead of files'
_h_nods      = 'Do not assume that the names of the files are <dataset>/<file>'

def argparser():
    '''
    Method to parse the arguments for the script.
    '''
    parser = argparse.ArgumentParser("This is used to upload files to Rucio RSEs from directories")

    parser.add_argument('-s', '--regexes',    type=str,   required=True,  nargs='+', help=_h_regex)
    parser.add_argument('--scopes',         type=str,   required=True,  nargs='+', help=_h_scopes)
    parser.add_argument('-r', '--rses',     type=str,   required=True,  nargs='+', help=_h_rses)
    parser.add_argument('-d', '--dirs',     type=str,   required=False, nargs='+', help=_h_dirs)
    parser.add_argument('-l', '--lifetime', type=int,   default = 3600,            help= _h_life)
    parser.add_argument('--dsmap',          type=str,   required=False, nargs=1,   help=_h_dsmap)
    parser.add_argument('--outdir',         type=str,   default='./',              help=_h_outdir)
    parser.add_argument('--fromfiles',      type=str,   required=False, nargs='+', help=_h_fromfiles)
    parser.add_argument('--filtds',         action='store_true',                   help=_h_filtds)
    parser.add_argument('--nods',           action='store_true',                   help=_h_nods)
    parser.add_argument('--submit',         action='store_true',                   help=_h_submit)

    return parser.parse_args()

def run():
    """" Main method """
    args = argparser()

    dirs      = args.dirs
    regexes   = args.regexes
    rses_rgx  = args.rses
    rses = set()
    for rse in rses_rgx:
        rses |= get_rses_from_regex(rse, rsecl)
    lifetime  = args.lifetime
    scopes    = args.scopes
    dsmapf    = args.dsmap
    outdir    = args.outdir
    fromfiles = args.fromfiles
    filtds    = args.filtds
    no_ds     = args.nods
    submit    = args.submit

    if fromfiles is not None:
        assert dirs is None, "ERROR:: Cannot use --dirs and --fromfiles together"
    if dsmapf is not None:
        assert no_ds is False, "ERROR:: Cannot use --dsmap and --nods together"

    print("INFO:: ================ Summary of Upload ==================")
    print("INFO:: Regexes to match files to upload: ", regexes)
    if fromfiles is not None:
        print("INFO:: Files to upload are read from files: ", fromfiles)
    else:
        print("INFO:: Directories to look in for files to upload: ", dirs)

    print("INFO:: Regex applied to: ", 'files' if not filtds else 'datasets')
    print("INFO:: RSEs to upload to: ", rses)
    print("INFO:: Lifetime that should be given to uploaded file (in seconds): ", lifetime)
    print("INFO:: Scopes to upload to: ", scopes)

    if dsmapf is not None:
        print("INFO:: Grouping files into datasets according to the mapping in: ", dsmap)
    elif no_ds is False:
        print("INFO:: Grouping files into datasets according to the folder structure: <dataset>/<file>")
    else:
        print("INFO:: Uploading files as individual files not inside datasets")


    # Get the mapping from dataset to files
    dsmap = {}
    if dsmapf is not None:
        with open(dsmapf, 'r') as dsmapfile:
            dsmap = json.load(dsmapfile)

    # Get the list of files to upload
    all_files = []
    if fromfiles is not None:
        all_files = get_lines_from_files(fromfiles)
    else:
        for folder in dirs:
            if no_ds:
                for _, _, files in os.walk(folder):
                    all_files.extend([f'{folder}/{file}' for file in files])
            else:
                for root, datasets, _ in os.walk(folder):
                    for dataset in datasets:
                        for _, _, files in os.walk(f'{root}/{dataset}'):
                            all_files.extend([f'{root}/{dataset}/{file}' for file in files])

    nfilesuploaded = 0
    nuploads       = 0

    for file in all_files:
        dataset = None
        if dsmap != {}:
            for ds, dsfiles in dsmap.items():
                if file in dsfiles:
                    dataset = ds
                    break
        elif no_ds is False:
            dataset = file.split('/')[-2]

        if filtds:
            if not any(re.match(regex, dataset) is not None for regex in regexes): continue
        else:
            if not any(re.match(regex, file) is not None for regex in regexes): continue

        if submit:
            nuploads += upload_file(file, rses, scopes, lifetime, dataset, uploadcl)
        else:
            for scope in scopes:
                for rse in rses:
                    print(f"INFO:: Would upload {file} to {rse} in {scope} with lifetime {lifetime} and dataset {dataset}")
                    nuploads += 1

        nfilesuploaded += 1

    print(f'INFO:: Will upload {nfilesuploaded} files')
    print(f'INFO:: Total number of upload requests: {nuploads} ')

if __name__ == '__main__':  run()

# datasets_folders_in = ['/eos/atlas/atlascerngroupdisk/phys-higgs/HSG8/tH_v34_skimmedNtuples']
# rse = 'UKI-NORTHGRID-MAN-HEP_SCRATCHDISK'
# scope = 'user.maly'
# submit = True
# nfilesuploaded = 0
# for folder in datasets_folders_in:
#     for root, datasets, _ in os.walk(folder):
#         for dataset in datasets[5:6]:
#             for _, _, files in os.walk(f'{root}/{dataset}'):
#                 for file in files[5:6]:
#                     print( f'{root}/{dataset}/{file}')
#                     # Upload file to RUCIO
#                     if submit:
#                         uploadcl.upload([{'path': f'{root}/{dataset}/{file}', 'rse': rse}])
#                     else:
#                         print(f"INFO:: Uploading {root}/{dataset}/{file} to {rse} in {scope}:{dataset}")

#                     nfilesuploaded += 1



# , 'dataset_scope': scope, 'dataset_name': f'{dataset}'