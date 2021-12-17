#
# Copyright 2021 General Electric Company
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
import json

#
# Encapsulate the JSON format of a nodegroup
#
class Report:
        
    def __init__(self, jsonval):
        if isinstance(jsonval, dict):
            self.json = jsonval
        elif (isinstance(jsonval, str)):
            self.json = json.loads(jsonval)
        else:
            raise Exception("bad jsonval param is neither json dict nor string" + jsonval)  
   
    def get_nodegroup_ids(self):
        return self.__recurse_get_nodegroups(self.json)
    
    def __recurse_get_nodegroups(self, jobj):
        '''
        recursively check jobj for "nodegroup" fields and build a list of the values
        '''
        ret = []
        # loop through all the keys
        for key in jobj:
            add_list = []
            if key == "nodegroup":
                add_list.append(jobj[key])
            
            elif isinstance(jobj[key], dict):
                add_list = self.__recurse_get_nodegroups(jobj[key])
                
            # recursively process lists
            elif isinstance(jobj[key], list):
                for item in jobj[key]:
                    add_list = add_list + self.__recurse_get_nodegroups(item)
            
            # now only add unique ids
            for add in add_list:
                if not (add in ret):
                    ret.append(add)
        return ret