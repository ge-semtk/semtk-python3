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
from . import semtk

class NodegroupStoreClient(semtkclient.SemTkClient):
   
    def __init__(self, serverURL):
        ''' servierURL string - e.g. http://machine:8099
        '''
        
        semtkclient.SemTkClient.__init__(self, serverURL, "nodeGroupStore")
    
    def exec_get_stored_items_metadata(self, item_type):

        payload = {}
        payload["itemType"] = item_type
        
        return self.post_to_table("getStoredItemsMetadata", payload)
    
    
    def exec_get_stored_item_by_id(self, item_id, item_type):

        payload = {}
        payload["id"] = item_id
        payload["itemType"] = item_type
        
        return self.post_to_table("getStoredItemById", payload)
    
    def exec_delete_stored_item(self, item_id, item_type):

        payload = {}
        payload["id"] = item_id
        payload["itemType"] = item_type
        
        self.post_to_status("deleteStoredItem", payload)
        return
    
    def exec_store_item(self, item_id, comments, creator, item_json_str, item_type, overwrite_flag=False):

        payload = {}
        payload["name"] = item_id
        payload["comments"] = comments
        payload["creator"] = creator
        payload["item"] = item_json_str
        payload["itemType"] = item_type
        payload["overwriteFlag"] = overwrite_flag
        
        self.post_to_status("storeItem", payload)
        return
            
    