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
import json

class RuntimeConstraint():
    OP_MATCHES = "MATCHES"
    OP_NOTMATCHES = "NOTMATCHES"
    OP_REGEX = "REGEX"
    OP_GREATERTHAN = "GREATERTHAN"
    OP_GREATERTHANOREQUALS = "GREATERTHANOREQUALS"
    OP_LESSTHAN = "LESSTHAN"
    OP_LESSTHANOREQUALS = "LESSTHANOREQUALS"
    OP_VALUEBETWEEN = "VALUEBETWEEN"
    OP_VALUEBETWEENUNINCLUSIVE = "VALUEBETWEENUNINCLUSIVE"
    
    def __init__(self, sparqlId, operator, operand_list):
        ''' 
            Create a SemtkTable from the @table results from a Semtk REST service
        '''
        self.sparqlId = sparqlId
        self.operator = operator
        self.operand_list = operand_list
    
    def to_json(self):
        
        jdict = {}
        jdict["SparqlID"] = self.sparqlId
        jdict["Operator"] = self.operator
        jdict["Operands"] = self.operand_list
        
        return json.dumps(jdict)