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

#
# Encapsulate the JSON format of StitchingStep objects
#
import json

class StitchingStep:
    
    def __init__(self, nodegroup_id, key_columns=[]):
        self.nodegroup_id = nodegroup_id
        self.key_columns = key_columns
        
    # build a json string that the REST calls like
    def to_json_str(self):
        json_dict = {"nodegroupId": self.nodegroup_id}
        if self.key_columns and self.key_columns != []:
            json_dict["keyColumns"] = self.key_columns
        return json.dumps(json_dict)
    
            
        
    