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
# Encapsulate the JSON format of SparqlConnection objects
#
class OntologyInfo:
    
    def __init__(self, oinfo_json):
        self.oinfo_json = oinfo_json
        
    def unprefix(self, raw):
        fields = raw.split(":")
        if len(fields) == 2 and fields[0] in self.oinfo_json["prefixes"]:
            return self.oinfo_json["prefixes"][fields[0]] + "#" + fields[1]
        else:
            return raw
        
    def get_class_list(self):
        ret = {}
        for c in self.oinfo_json["topLevelClassList"]:
            ret[self.unprefix(c)] = 1
        
        for pair in self.oinfo_json["subClassSuperClassList"]:
            ret[self.unprefix(pair[0])] = 1
            ret[self.unprefix(pair[1])] = 1;
            
        return list(ret.keys())
            
    
    
            
        
    