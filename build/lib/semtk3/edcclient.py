import json
from . import semtkasyncclient
from . import semtktable

class EdcClient(semtkasyncclient.SemTkAsyncClient):
    
    def __init__(self, baseURL, service=None, status_client=None, results_client=None):
        ''' Client for a semtk service that uses status and results services
          
            baseURL string - http://machine:8000, http://machine:8000/,  http://machine:8000/service, or http://machine:8000/service/
            service string - appended to baseURL if it isn't already there
            status_client - SemtkClient or None to use self as pass-through
            results_client - SemtkClient or None to use self as pass-through
        '''
        
        super(EdcClient, self).__init__(baseURL, service)
        
        self.status_client = status_client
        self.results_client = results_client

    ######## exec functions are direct calls to the service #######
    
    #
    # EDC clients may be sync or async.  
    # No way to tell until we make the call and see the response.
    # 
    # So these three methods are identical/interchangeable
    #
    def post_async_to_table(self, endpoint, dataObj={}):
        return self.post_edc_to_table(endpoint, dataObj)
    
    def post_to_table(self, endpoint, dataObj={}):
        return self.post_edc_to_table(endpoint, dataObj)
    
    def post_edc_to_table(self, endpoint, dataObj={}):
        res = self.post(endpoint, dataObj)
        content = json.loads(res)
        self._check_status(content)
        
        if "table" in content and "@table" in content["table"]:
            return semtktable.SemtkTable(content["table"]["@table"])
                                                
        elif "table" in content and "jobId" in content["table"]:
            jobid = content["table"]["jobId"]
            self.poll_until_success(jobid)
            json_blob = self.results_client.exec_get_json_blob_results(jobid)
            return semtktable.SemtkTable(json_blob)
        
        else:
            self.raise_exception("Can't find table.@table or simpleResults.job in edc service response")
  
        
        

        
    