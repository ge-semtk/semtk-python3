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

class NodegroupStoreClient(semtkclient.SemTkClient):
   
    def __init__(self, serverURL):
        ''' servierURL string - e.g. http://machine:8099
        '''
        
        semtkclient.SemTkClient.__init__(self, serverURL, "nodeGroupStore")
    
    def exec_get_nodegroup_metadata(self):

        payload = {}
        
        return self.post_to_table("getNodeGroupMetadata", payload)
    
    
    def exec_get_nodegroup_by_id(self, nodegroup_id):

        payload = {}
        payload["id"] = nodegroup_id;
        
        return self.post_to_table("getNodeGroupById", payload)
            
    