'''
@author: 200001934
'''

import requests
import uuid
import os

def download_url(url, baseDir):
    '''
        download a URL contents to baseDir
        attempt to use filename from headers, else generate a guid
        append _0 _1 etc to base filename to avoid duplicates
    '''
    
    # perform download into r
    r = requests.get(url)
    
    # get filename from headers or generate guid
    if 'Content-Disposition' in r.headers:
        filename =  r.headers['Content-Disposition'].split('"')[1]
    else:
        filename = str(uuid.uuid4())
    
    # error on bad baseDir
    if not os.path.exists(baseDir) or not os.path.isdir(baseDir):
        raise Exception("baseDir is not an existing directory: " + baseDir)
    
    # add number to filename if it doesn't exist
    orig_path = os.path.join(baseDir, filename)
    path = orig_path
    number = 0
    while os.path.exists(path):
        base, ext = os.path.splitext(orig_path)
        path = base + "_" + str(number) + ext
        number += 1
        
    
    # write the file
    open(path, 'wb').write(r.content)
    
    return path