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
from . import semtkclient
import logging
import sys


semtk3_logger = logging.getLogger("semtk3")

RESULT_TYPE_TABLE = "TABLE"
RESULT_TYPE_GRAPH_JSONLD = "GRAPH_JSONLD"
RESULT_TYPE_CONFIRM = "CONFIRM"
RESULT_TYPE_RDF = "RDF"
RESULT_TYPE_HTML = "HTML"
RESULT_TYPE_N_TRIPLES = "N_TRIPLES"
    
class SemTkAsyncClient(semtkclient.SemTkClient):
    PRINT_DOTS = False
    WAIT_MSEC = 5000
    PERCENT_INCREMENT = 20
    
    
    def __init__(self, baseURL, service=None, status_client=None, results_client=None):
        ''' Client for a semtk service that uses status and results services
          
            baseURL string - http://machine:8000, http://machine:8000/,  http://machine:8000/service, or http://machine:8000/service/
            service string - appended to baseURL if it isn't already there
            status_client - SemtkClient or None to use self as pass-through
            results_client - SemtkClient or None to use self as pass-through
        '''
        
        super(SemTkAsyncClient, self).__init__(baseURL, service)
        
        self.status_client = status_client
        self.results_client = results_client

    ######## exec functions are direct calls to the service #######
    
    def exec_get_job_completion_percentage(self, jobid):
        ''' 
            returns int
        '''

        payload = {}
        payload["jobID"] = jobid
        
        simple = self.post_to_simple("getJobCompletionPercentage", payload)
        
        return self.get_simple_field_int(simple, "percent")
    
    def exec_job_status_boolean(self, jobid):
        ''' 
            returns boolean 
        '''

        payload = {}
        payload["jobID"] = jobid
        
        simple = self.post_to_simple("jobStatus", payload)
        
        return (self.get_simple_field_str(simple, "status") == "Success")
    
    def exec_job_status_message(self, jobid):
        ''' 
            returns string 
        '''

        payload = {}
        payload["jobID"] = jobid
        
        simple = self.post_to_simple("jobStatusMessage", payload)
        
        return self.get_simple_field_str(simple, "message") 
    
    def exec_get_results_table(self, jobid):
        ''' 
            returns SemtkTable
        '''

        payload = {}
        payload["jobID"] = jobid
        
        table = self.post_to_table("getResultsTable", payload)
        
        return table
    
    def exec_wait_for_percent_or_msec(self, jobid, percent_complete, max_wait_msec):
        '''
            returns integer percent complete
        '''
        payload = {}
        payload["jobID"] = jobid
        payload["percentComplete"] = percent_complete
        payload["maxWaitMsec"] = max_wait_msec

        simple = self.post_to_simple("waitForPercentOrMsec", payload)
        
        return self.get_simple_field_int(simple, "percentComplete")

    ######## the main externally used methods #######
    
    #
    # Use async jobID and results/status to get table
    #
    def post_async_to_table(self, endpoint, dataObj={}, log_status_info=False):
        ''' 
            returns SemTkTable
            raises errors otherwise
        '''
        jobid = self.post_to_jobid(endpoint, dataObj)
        semtk3_logger.debug("jobid:  " + jobid)
        self.poll_until_success(jobid, log_status_info)
        table = self.post_get_table_results(jobid)
        return table
    
    def post_async_to_json_blob(self, endpoint, dataObj={}):
        ''' 
            returns json
            raises errors otherwise
        '''
        jobid = self.post_to_jobid(endpoint, dataObj)
        semtk3_logger.debug("jobid:  " + jobid)
        self.poll_until_success(jobid)
        ret = self.post_get_json_blob_results(jobid)
        return ret
    
    def post_async_to_record_process(self, endpoint, dataObj={}):
        ''' 
            returns success message, which may include warnings
            raises errors including error table
        '''
        jobid, warnings = self.post_to_jobid_warnings(endpoint, dataObj)
        semtk3_logger.debug("jobid:  " + jobid)
        warningText = ""
        if warnings:
            warningText = "Ingestion Warnings:\n - " + "\n - ".join(warnings) + "\n"
                
        try:
            self.poll_until_success(jobid)
            return self.post_get_status_message(jobid), warningText
        except Exception as e1:
            # failure occurred in ingestion:  tack on the error table
            ingest_err_msg = repr(e1)
            
            table = None
            try:
                # try to get table of errors
                table = self.post_get_table_results(jobid)
            except:
                # if it was really bad, getting table caused more problems.
                # so, just give the original
                raise Exception(ingest_err_msg)
            
            # if getting an error table worked, then raise that  
            raise Exception(ingest_err_msg + "\n" + warningText + "Failures encountered:\n" + table.get_csv_string()) from None
            
               
         
    def post_async_to_status(self, endpoint, dataObj={}, log_status_info=False):
        ''' 
        
            returns success message
            raises errors including status != success
        '''
        jobid = self.post_to_jobid(endpoint, dataObj)
        semtk3_logger.debug("jobid:  " + jobid)
        
        self.poll_until_success(jobid, log_status_info)
        return self.post_get_status_message(jobid)
         
      
    def poll_until_success(self, jobid, log_status_info=False):
        ''' poll for percent complete and return if SUCCESS
            raises RestException including if status="failure"
            
            returns void
        '''
        
        # get initial percent
        percent_complete = self.post_wait_for_percent_or_msec(jobid, 0, 0)
        semtk3_logger.info("Percent complete:  " + str(percent_complete) + "%")

        # loop on percent complete calls
        while percent_complete < 100:
            
            # poll
            percent_complete = self.post_wait_for_percent_or_msec(jobid, percent_complete + 1, SemTkAsyncClient.WAIT_MSEC)
            msg = self.post_get_status_message(jobid) if log_status_info else None
            
            # print
            if msg != None:
                print(str(percent_complete) + "%  : " + msg, flush=True, file=sys.stderr)
            else:
                semtk3_logger.info("Percent complete:  " + str(percent_complete) + "%")
                
            if SemTkAsyncClient.PRINT_DOTS:
                print('.', end='')
                
        if SemTkAsyncClient.PRINT_DOTS:
                print(' ')
                    
        # 100% : check for success/failure
        if not self.post_get_status_boolean(jobid):
            msg = self.post_get_status_message(jobid)
            raise Exception("Job " + jobid + " failed: " + msg)
        else:
            semtk3_logger.debug("SUCCESS")
            
        # otherwise return quietly
        return   
            
    ######## these choose results/status clients if they exist   #######
    ######## otherwise use the version attached to self          #######
    
    def post_get_table_results(self, jobid):
        ''' get table results using results, otherwise using self
        '''
        if (self.results_client):
            return self.results_client.exec_get_table_results(jobid)
        else:
            return self.exec_get_results_table(jobid)
        
    def post_get_json_blob_results(self, jobid):
        ''' get table results using results, otherwise using self
        '''
        
        return self.results_client.exec_get_json_blob_results(jobid)

    
    def post_get_percent_complete(self, jobid):
        ''' get percent complete using status, otherwise using self
        '''
        if (self.status_client):
            return self.status_client.exec_get_percent_complete(jobid)
        else:
            return self.exec_get_job_completion_percentage(jobid)
    
    def post_get_status_boolean(self, jobid):
        ''' get status  using status client, otherwise using self
        '''
        if (self.status_client):
            return self.status_client.exec_get_status_boolean(jobid)
        else:
            return self.exec_job_status_boolean(jobid)
        
    def post_get_status_message(self, jobid):
        ''' get status message using status client, otherwise using self
        '''
        if (self.status_client):
            return self.status_client.exec_get_status_message(jobid)
        else:
            return self.exec_job_status_message(jobid)
        
    def post_wait_for_percent_or_msec(self, jobid, percent_complete, max_wait_msec):
        ''' 
        '''
        ''' get status message using status client, otherwise using self
        '''
        if (self.status_client):
            return self.status_client.exec_wait_for_percent_or_msec(jobid, percent_complete, max_wait_msec)
        else:
            return self.exec_wait_for_percent_or_msec(jobid, percent_complete, max_wait_msec)
        