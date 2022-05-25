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

class NodegroupClient(semtkasyncclient.SemTkAsyncClient):
    USE_NODEGROUP_CONN = "{\"name\": \"%NODEGROUP%\",\"domain\": \"%NODEGROUP%\",\"model\": [],\"data\": []}"
    
    def __init__(self, serverURL, status_client, results_client):
        ''' serverURL string - e.g. http://machine:8099
            status_client 
            results_client 
        '''
        super(NodegroupClient, self).__init__(serverURL, "nodeGroup", status_client, results_client)
    
    def exec_create_nodegroup(self, conn_json_str, class_uri, sparql_id=None):
        ''' execute a create_nodegroup
            throws: exception otherwise
        '''
        payload = {}
        payload["conn"] = conn_json_str
        payload["uri"] = class_uri
        if (sparql_id):
            payload["sparqlID"] = sparql_id
     
        res = self.post_to_simple("createNodeGroup", payload)
        
        return res
