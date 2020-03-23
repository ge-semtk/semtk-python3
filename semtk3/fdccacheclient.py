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
    def exec_cache_using_table_bootstrap(self, conn_json_str, spec_id, bootstrap_table, recache_after_sec):
        
        payload = {
            "conn": conn_json_str,
            "specId": spec_id,
            "bootstrapTableJsonStr": bootstrap_table.to_json_str(),
            "recacheAfterSec": recache_after_sec
        }

        job_id = self.post_to_jobid("cacheUsingTableBootstrap", payload)
        self.poll_until_success(job_id)
        return