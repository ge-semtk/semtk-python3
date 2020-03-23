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
from . import edcclient
#
# A client to HiveService
#
class HiveClient(edcclient.EdcClient):
    
    def __init__(self, serverURL, hiveserver_host, hiveserver_port, hiveserver_database, status_client=None, results_client=None):
        ''' serverURL string - e.g. http://machine:12055
        '''
        super(HiveClient, self).__init__(serverURL, "hiveService", status_client, results_client)
        self.hiveserverhost = hiveserver_host
        self.hiveserverport = hiveserver_port
        self.hiveserverdatabase = hiveserver_database
 
 
    #
    # Execute query.
    #
    def exec_query_hive(self, query):    
        payload = {
            "host": self.hiveserverhost,
            "port": self.hiveserverport,
            "database": self.hiveserverdatabase,
            "query": query
        }
        table = self.post_to_table("queryHive", payload)
        return table
    