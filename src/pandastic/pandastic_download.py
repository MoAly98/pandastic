import rucio.common.exception as excep
from rucio import client as rucio_client
import rucio.client.downloadclient as downloadclient
import os, sys, re
import argparse
from pandaclient import queryPandaMonUtils
from pandaclient import PBookCore
import certifi
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def argparser():
    parser = argparse.ArgumentParser()

    #parser.add_argument('dids', help='File with input container names')
    parser.add_argument('-s', '--suffix',      type=str, required=True)
    parser.add_argument('-o', '--outdir',      type=str, required=True)
    parser.add_argument('--ds',                type=str)
    parser.add_argument('--submit',            action=   'store_true')
    parser.add_argument('-d', '--days',        type=int, default=30)
    parser.add_argument('-u', '--grid-user',   type=str, default='')

    return parser.parse_args()

def run():
    args = argparser()
    # /cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase/x86_64/PandaClient/1.5.9/lib/python3.6/site-packages/pandaclient/PBookCore.py
    pbook = PBookCore.PBookCore()
    outdir = args.outdir
    user = pbook.username if args.grid_user == '' else args.grid_user
    days = args.days
    suffix = args.suffix
    _, url, data = queryPandaMonUtils.query_tasks( username=user,
                                                   days=days, 
                                                   status='done')
    to_download = []
    for datum in data:
        outDS = set()
        for ds in datum.get("datasets"):
            if(ds.get("type") != 'output'):    continue 
            cont = ds.get("containername")
            if args.ds is not None:
                ds = args.ds.replace("*",".*")
                if re.match(ds, cont) is None:   continue
            outDS.add(cont)
        taskname = datum.get("taskname")
        status = datum.get("status")
        
        if re.match(f"^.*_{suffix}/$", taskname) is None:   continue

        to_download.extend(list(outDS))
        if outDS != set():
            print(f"INFO:: TO DOWNLOAD: {outDS}, Status = {status}")
    
    if to_download == []:   print(f"ERROR:: NO CONTAINERS TO DOWNLOAD, BYE!"); sys.exit(1)
    print(f"INFO:: Prepare to download {len(to_download)} containers..")
    if args.submit: 
        didcl = rucio_client.didclient.DIDClient()
        downloadcl = downloadclient.DownloadClient()
        for cont in to_download:
            print(f"INFO:: Downloading: {cont}")
            
            if ("JetLepton" in cont or "423300" in cont):
                idx_to_get_for_log = [5,8,9] # ?? this is LEGACY
            elif("data" in cont):
                idx_to_get_for_log = [2,7]   # DataXX_NTeV_filter (e.g. data17_13TeV_lj)
            else:
                idx_to_get_for_log = [3,6]   # dsid_tag

            logfile = open(outdir+'/'+"_".join([cont.split(".")[i] for i in idx_to_get_for_log])+".log", "w")
            print(f"Logfile: {logfile.name}")
            
            scope = ".".join(cont.split(".")[:2])
            files = list(didcl.list_files(scope,cont.replace('/','')))
            print(f"INFO:: Num Files = {len(list(files))}")
            totalsize = sum([file['bytes']/1e6 for file in files])
            print(f"INFO:: Total Size = {totalsize} MB")
            
            sys.stdout = logfile
            sys.stderr = logfile

            try:    downloadcl.download_dids([{'did': cont, 'base_dir': outdir}], )
            except excep.NotAllFilesDownloaded as e:    raise str(e)
            sys.stdout = sys.__stdout__
            print("Hi")
            logfile.close()

if __name__ == "__main__":  run()
