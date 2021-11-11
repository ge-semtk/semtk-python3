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
import sys
import logging
from . import restclient
from . import semtktable

# 
# SETUP NOTES
#    - Inside GE, will fail with Captcha if Windows has HTTP_PROXY and maybe HTTPS_PROXY environment variables set
#           unless NO_PROXY is set up on your endpoint
#


semtk3_logger = logging.getLogger("semtk3")


class SemTkClient(restclient.RestClient):
    JOB_ID_KEY = "JobId"
    RESULT_TYPE_KEY = "resultType"
    
    def _check_status(self, content):
        ''' check content is a dict, has status=="success"
        '''
        if not isinstance(content, dict):
            self.raise_exception("Can't process content from rest service")
        
        if "status" not in content.keys():
            self.raise_exception("Can't find status in content from rest service")
        
        if content["status"] != "success":
            self.raise_exception("Rest service call did not succeed ")

    def _check_simple(self, content):
        ''' perform all checks on content through checking for simpleresults 
        '''
        self._check_status(content)
        
        if "simpleresults" not in content.keys():
            self.raise_exception("Rest service did not return simpleresults")
            
    def _check_table(self, content):
        ''' perform all checks on content through checking for table 
        '''
        self._check_status(content)
        
        if  "table" not in content.keys():
            self.raise_exception("Rest service did not return table")
        
        if "@table" not in content["table"].keys():
            self.raise_exception("Rest service table does not contain @table")
    
    def _check_record_process(self, content):
        ''' checking for recordProcess table 
        '''

        if  "recordProcessResults" not in content.keys():
            self._check_status(content)
            
            self.raise_exception("Rest service did not record process results")
    
    
    def get_simple_field(self, simple_res, field):
        ''' get a simple field with REST error handling
        '''
        if field not in simple_res.keys():
            self.raise_exception("Rest service did not return simple result " + field)
        
        return simple_res[field]
    
    def get_simple_field_int(self, simple_res, field):
        ''' get integer simple results field
            returns int
            raises RestException on type or missing field
        '''
        try:
            f = self.get_simple_field(simple_res, field)
            return int(f)
        
        except ValueError:
            self.raise_exception("Simple results field " + field + " expecting integer, found " + f)
            
    def get_simple_field_str(self, simple_res, field):
        ''' get string from simple result
            returns string
            raises RestException on type or missing field
        '''
        f = self.get_simple_field(simple_res, field)
        return str(f)
    
    def ping(self):
        '''
            logger.INFO(success)  or  logger.ERROR(error)
            returns True (success) or False (failure)
        '''
        try:
            res = self.post_to_simple("serviceInfo/ping")
            semtk3_logger.info(self.baseURL + self.get_simple_field_str(res, "available"))
            return True
        except Exception as e:
            semtk3_logger.error(str(e).replace("\n", ""))
            return False
        
    def post_to_status(self, endpoint, dataObj={}, files=None):
        ''' 
            returns dict - the simple results
                           which can be used as a regular dict, or with error-handling get_simple_field*() methods
        '''
        content = self.post(endpoint, dataObj=dataObj, files=files)
        content = json.loads(content.decode("utf-8", errors='ignore'))
        self._check_status(content)
        return content
        
    def post_to_simple(self, endpoint, dataObj={}, files=None):
        ''' 
            returns dict - the simple results
                           which can be used as a regular dict, or with error-handling get_simple_field*() methods
        '''
        content = self.post(endpoint, dataObj=dataObj, files=files)
        content = json.loads(content.decode("utf-8", errors='ignore'))
        self._check_simple(content)
        return content["simpleresults"]
    
    def post_to_table(self, endpoint, dataObj={}):
        ''' 
            returns dict - the table 
            raises RestException
        '''
        content = self.post(endpoint, dataObj)
        content = json.loads(content.decode("utf-8", errors='ignore'))

        self._check_table(content)
        
        table = semtktable.SemtkTable(content["table"]["@table"])
        return table
    
    def post_to_record_process(self, endpoint, dataObj={}, files=None):
        ''' 
            returns records processed successfully
            raises RestException unless failuresEncountered = 0
        '''
        content = self.post(endpoint, dataObj=dataObj, files=files)
        content = json.loads(content.decode("utf-8", errors='ignore'))

        # throw exception if no recordProcessResults
        self._check_record_process(content)
        
        record_process = content["recordProcessResults"]
        if  "failuresEncountered" not in record_process.keys():
            raise Exception("Results did not contain recordProcessResults.failuresEncountered: \n" + content)
        
        if record_process["failuresEncountered"] != 0:
            if "errorTable" in record_process:
                t = semtktable.SemtkTable(record_process["errorTable"])
                raise Exception("Encountered failures: \n" + t.get_csv_string())
            else:
                raise Exception("Encountered failures but no table given: \n" + content)
            
        if not ("recordsProcessed" in record_process):
            raise Exception("Results did not contain recordProcessResults.recordsProcessed: \n" + content)

        return record_process["recordsProcessed"]
    
    def post_to_jobid(self, endpoint, dataObj={}):
        ''' 
            returns string jobid
            raises errors otherwise
        '''
        simple_res = self.post_to_simple(endpoint, dataObj)
        return self.get_simple_field_str(simple_res, SemTkClient.JOB_ID_KEY)
