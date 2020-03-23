#
# Copyright 2019-20 General Electric Company
#
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
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
    
            
        
        
    
    