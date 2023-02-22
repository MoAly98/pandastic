
def upload_file(file, rses, scopes, lifetime, ds, uploadcl):

    nuploads = 0
    print("INFO:: Preparing uploads with lifetime ", lifetime, " seconds")
    for scope in scopes:
        for rse in rses:
            print(f"INFO:: Uploading the file {file} to RSE {rse} in scope {scope}")
            options = {'path': file, 'rse': rse, 'scope': scope, 'lifetime': lifetime}
            if ds is not None:
                print("INFO:: File will be saved in dataset: ", ds)
                options['dataset_name'] = ds
                options['dataset_scope'] = scope
            try:
                uploadcl.upload([options])
                nuploads += 1
            except Exception as e:
                print(f"ERROR:: Failed to upload file {file} to RSE {rse} in scope {scope}")
                print(str(e))
                continue

    return nuploads
