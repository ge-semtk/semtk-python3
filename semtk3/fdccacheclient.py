from . import semtkasyncclient


class FdcCacheClient(semtkasyncclient.SemTkAsyncClient):
    
    
    def __init__(self, serverURL, status_client=None, results_client=None):
        ''' servierURL string - e.g. http://machine:8099
            status_client & results_client - usually None. set only if different from the nodeGroupExecutionService
        '''
        super(FdcCacheClient, self).__init__(serverURL, "fdcCache", status_client, results_client)
    #
    # Upload owl.
    # Default to model[0] graph in the connection
    #
    def exec_cache_using_table_bootstrap(self, conn_json_str, spec_id, bootstrap_table, max_age_sec):
        
        payload = {
            "conn": conn_json_str,
            "specId": spec_id,
            "bootstrapTableJsonStr": bootstrap_table.to_json_str(),
            "maxAgeSeconds": max_age_sec
        }

        job_id = self.post_to_jobid("cacheUsingTableBootstrap", payload)
        self.poll_until_success(job_id)
        return