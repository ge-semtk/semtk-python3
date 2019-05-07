from . import semtkclient
import time
import sys

class SemTkAsyncClient(semtkclient.SemTkClient):
    
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
    
    def post_async_to_table(self, endpoint, dataObj={}):
        ''' 
            returns SemTkTable
            raises errors otherwise
        '''
        jobid = self.post_to_jobid(endpoint, dataObj)
        sys.stderr.write("jobid:  " + jobid + "\n")
        self.poll_until_success(jobid)
        table = self.post_get_table_results(jobid)
        return table
        
    def poll_until_success(self, jobid):
        ''' poll for percent complete and return if SUCCESS
            raises RestException including if status="failure"
            
            returns void
        '''
        MSEC = 5000
        INCREMENT = 20
        
        percent_complete = self.post_wait_for_percent_or_msec(jobid, INCREMENT, MSEC)
        sys.stderr.write("Percent complete:  " + str(percent_complete) + "%\n")

        # loop on percent complete calls
        while percent_complete < 100:
            percent_complete += INCREMENT
            if percent_complete > 100:
                percent_complete = 100
            
            percent_complete = self.post_wait_for_percent_or_msec(jobid, percent_complete, MSEC)
            sys.stderr.write("Percent complete:  " + str(percent_complete) + "%\n")
            
        sys.stderr.write("\n")
        
        # throw exception on job failure
        if not self.post_get_status_boolean(jobid):
            msg = self.post_get_status_message(jobid)
            self.raise_exception("Job " + jobid + " failed: " + msg)
            sys.stderr.write("FAILURE\n")
        else:
            sys.stderr.write("SUCCESS\n")
            
        # otherwise return quietly
        return   
            
    ######## these choose results/status clients if they exist   #######
    ######## otherwise use the version attached to self          #######
            
    def post_to_jobid(self, endpoint, dataObj={}):
        ''' 
            returns string jobid
            raises errors otherwise
        '''
        simple_res = self.post_to_simple(endpoint, dataObj)
        return self.get_simple_field_str(simple_res, "JobId")
    
    def post_get_table_results(self, jobid):
        ''' get table results using results, otherwise using self
        '''
        if (self.results_client):
            return self.results_client.exec_get_table_results(jobid)
        else:
            return self.exec_get_results_table(jobid)
    
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
        