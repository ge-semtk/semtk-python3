import json
import requests
import sys

# 
# SETUP NOTES
#    - Inside GE, will fail with Captcha if Windows has HTTP_PROXY and maybe HTTPS_PROXY environment variables set
#           unless NO_PROXY is set up on your endpoint
#
class RestException(Exception):
    "Exception for errors from rest endpoints"
    pass;

class RestClient(object):
    
    def __init__(self, baseURL, service=None):
        '''
            baseURL string - http://machine:8000, http://machine:8000/,  http://machine:8000/service, or http://machine:8000/service/
            service string - appended to baseURL if it isn't already there
        '''
        
        # self.baseURL
        # make sure it ends with "/"
        self.baseURL = baseURL
        
        if not self.baseURL.endswith("/"): self.baseURL += "/"
        
        # make sure baseURL ends with service
        if (not service is None):
            suffix = service + "/"
            if not self.baseURL.endswith(suffix): self.baseURL += suffix
        
        # lastURL
        self.lastURL = ""
        # lastContent
        self.lastContent = ""
        
        # self.headers
        self.headers = {
            'content-type': "application/json",
            'cache-control': "no-cache"
        }

    def to_json_array(self, to_jsonable_list):
        ret = "[" + to_jsonable_list[0].to_json()
        for i in range(1, len(to_jsonable_list)):
            ret += "," + to_jsonable_list[i].to_json()
        ret += "]"
        return ret
        
    def raise_exception(self, msg):
        # make readable shortened version of last full content returned
        c = str(self.lastContent[1:-1]).replace('","', '"\n"').replace('\\\\n', '\n')
        if (len(c) > 10000):
            c = c[:10000] + "..."
            
        # raise exception, adding URL and last content
        raise RestException(self.lastURL + ": " + msg + "\nDetails:\n" + c)
        
    def post(self, endpoint, dataObj={}, files=None):
        ''' basic POST 
               endpoint - string
               dataObj - dict will be converted to json for the post  (default {})
                
               returns string - response content
               raises RestException if response is not OK
        '''
        
        if (files):
            data = dataObj 
            headers = None
        else :
            data = json.dumps(dataObj)
            headers = self.headers
        
        self.lastURL = self.baseURL + endpoint

        sys.stderr.write("Posting to " + self.lastURL + "...\n")
        response = requests.request("POST", self.lastURL, data=data, headers=headers, files=files)

        if response.ok:
            self.lastContent = response.content
            return response.content
        else:
            self.raise_exception("failed with reason: " + response.text)
            
