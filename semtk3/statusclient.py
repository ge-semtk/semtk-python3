
from . import semtkclient

class StatusClient(semtkclient.SemTkClient):
   
    def __init__(self, serverURL):
        ''' servierURL string - e.g. http://machine:8099
        '''
        
        semtkclient.SemTkClient.__init__(self, serverURL, "status")
    
    def exec_get_percent_complete(self, jobId):
        ''' 
            returns int
        '''

        payload = {}
        payload["jobId"] = jobId
        
        simple = self.post_to_simple("getPercentComplete", payload)
        
        return self.get_simple_field_int(simple, "percentComplete")
    
    def exec_get_status_boolean(self, jobId):
        ''' 
            returns boolean 
        '''

        payload = {}
        payload["jobId"] = jobId
        
        simple = self.post_to_simple("getStatus", payload)
        
        return (self.get_simple_field_str(simple, "status") == "Success")
    
    def exec_get_status_message(self, jobid):
        ''' 
            returns string 
        '''

        payload = {}
        payload["jobId"] = jobid
        
        simple = self.post_to_simple("getStatusMessage", payload)
        
        return self.get_simple_field_str(simple, "statusMessage") 
    
    def exec_wait_for_percent_or_msec(self, jobid, percent_complete, max_wait_msec):
        '''
            returns integer percent complete
        '''
        payload = {}
        payload["jobId"] = jobid
        payload["percentComplete"] = percent_complete
        payload["maxWaitMsec"] = max_wait_msec

        simple = self.post_to_simple("waitForPercentOrMsec", payload)
        
        return self.get_simple_field_int(simple, "percentComplete")