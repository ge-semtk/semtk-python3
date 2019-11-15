import json
from . import semtkclient
from . import util


class ResultsClient(semtkclient.SemTkClient): 
   
    def __init__(self, serverURL):
        ''' servierURL string - e.g. http://machine:8099
        '''
        
        #SemTkClient.__init__(self, serverURL, "results")
        super(ResultsClient, self).__init__(serverURL, "results")
    
    def exec_get_table_results(self, jobId):
        ''' 
            returns SemtkTable
        '''

        payload = {}
        payload["jobId"] = jobId
        
        table = self.post_to_table("getTableResultsJson", payload)
        
        return table
    
    def exec_get_json_blob_results(self, jobId):
        ''' 
            returns SemtkTable
        '''

        payload = {}
        payload["jobId"] = jobId
        
        content_str = self.post("getJsonBlobResults", payload)
        
        return json.loads(content_str)
    
    def exec_get_binary_file(self, fileId, baseDir):
        '''
            Download file into baseDir
            return the path
        '''
        url = self.baseURL + "/getBinaryFile/" + fileId
        return util.download_url(url, baseDir)
    
            
        
        
    
    