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
from . import sparqlconnection

class NodegroupExecClient(semtkasyncclient.SemTkAsyncClient):
    USE_NODEGROUP_CONN = "{\"name\": \"%NODEGROUP%\",\"domain\": \"%NODEGROUP%\",\"model\": [],\"data\": []}"
    
    def __init__(self, serverURL, status_client=None, results_client=None):
        ''' servierURL string - e.g. http://machine:8099
            status_client & results_client - usually None. set only if different from the nodeGroupExecutionService
        '''
        super(NodegroupExecClient, self).__init__(serverURL, "nodeGroupExecution", status_client, results_client)
    
    def exec_async_dispatch_filter_by_id(self, nodegroup_id, target_obj_sparql_id, override_conn_json=None, limit_override=None, offset_override=None, runtime_constraints=None, edc_constraints=None, flags=None ):
        ''' execute a select by nodegroup id
            returns: the table
            thorws: exception otherwise
        '''
        payload = {}
        payload["externalDataConnectionConstraints"] = ""
        payload["flags"] = ""
        if (limit_override):  payload["limitOverride"] = limit_override
        if (offset_override): payload["offsetOverride"] = offset_override
        payload["nodeGroupId"] = nodegroup_id
        payload["runtimeConstraints"] = ""
        payload["sparqlConnection"] = override_conn_json if override_conn_json else self.USE_NODEGROUP_CONN
        payload["targetObjectSparqlId"] = target_obj_sparql_id
        if (flags):  payload["flags"] = flags
        if (runtime_constraints): payload["runtimeConstraints"] = self.to_json_array(runtime_constraints)
        if (edc_constraints): payload["externalDataConnectionConstraints"] = edc_constraints
        

        table = self.post_async_to_table("dispatchFilterById", payload)
        
        return table
    
    def exec_async_dispatch_select_by_id(self, nodegroup_id, override_conn_json=None, limit_override=None, offset_override=None, runtime_constraints=None, edc_constraints=None, flags=None ):
        ''' execute a select by nodegroup id
            returns: the table
            thorws: exception otherwise
        '''
        payload = {}
        payload["externalDataConnectionConstraints"] = ""
        payload["flags"] = ""
        if (limit_override):  payload["limitOverride"] = limit_override
        if (offset_override): payload["offsetOverride"] = offset_override
        payload["nodeGroupId"] = nodegroup_id
        payload["runtimeConstraints"] = ""
        payload["sparqlConnection"] = override_conn_json if override_conn_json else self.USE_NODEGROUP_CONN
        if (flags):  payload["flags"] = flags
        if (runtime_constraints): payload["runtimeConstraints"] = self.to_json_array(runtime_constraints)
        if (edc_constraints): payload["externalDataConnectionConstraints"] = edc_constraints
        

        table = self.post_async_to_table("dispatchSelectById", payload)
        
        return table
    
    def exec_async_dispatch_count_by_id(self, nodegroup_id, override_conn_json=None, limit_override=None, offset_override=None, runtime_constraints=None, edc_constraints=None, flags=None ):
        ''' execute a count by nodegroup id
            returns: the table
            thorws: exception otherwise
        '''
        payload = {}
        payload["externalDataConnectionConstraints"] = ""
        payload["flags"] = ""
        if (limit_override):  payload["limitOverride"] = limit_override
        if (offset_override): payload["offsetOverride"] = offset_override
        payload["nodeGroupId"] = nodegroup_id
        payload["runtimeConstraints"] = ""
        payload["sparqlConnection"] = override_conn_json if override_conn_json else self.USE_NODEGROUP_CONN
        if (flags):  payload["flags"] = flags
        if (runtime_constraints): payload["runtimeConstraints"] = self.to_json_array(runtime_constraints)
        if (edc_constraints): payload["externalDataConnectionConstraints"] = edc_constraints
        

        table = self.post_async_to_table("dispatchCountById", payload)
        
        return table
    def exec_async_dispatch_raw_sparql(self, sparql, override_conn_json=None):
        ''' execute a select by nodegroup id
            returns: the table
            thorws: exception otherwise
        '''
        payload = {}
        payload["sparql"] = sparql
        payload["sparqlConnection"] = override_conn_json if override_conn_json else self.USE_NODEGROUP_CONN
        
        table = self.post_async_to_table("dispatchRawSparql", payload)
        
        return table
    
    def exec_get_runtime_constraints_by_id(self, nodegroup_id):
        ''' execute a select by nodegroup id
            returns:  valueId,      itemType,     valueType
                      ?sparqlId    PROPERTYITEM    INT|STRING|FLOAT etc.
                      ?sparqlId2   NODE            NODE_URI
            throws: exception otherwise
        '''
        payload = {}
        payload["nodegroupId"] = nodegroup_id

        return self.post_to_table("getRuntimeConstraintsByNodeGroupID", payload)     
    
    def exec_dispatch_clear_graph(self, conn, model_or_data, index):
        ''' execute clear graph
            returns:  message,      
                      some text   
            throws: exception otherwise
        '''
        payload = {}
        payload["serverType"] = conn.get_server_type(model_or_data, index)
        payload["serverAndPort"] = conn.get_server_and_port(model_or_data, index)
        payload["graph"] = conn.get_graph(model_or_data, index)

        return self.post_async_to_table("dispatchClearGraph", payload)   
    
    def exec_async_ingest_from_csv(self, nodegroup_id, csv_str, override_conn_json=None):
        ''' nodegroup_id - from nodegroup store
            csv_str - data, e.g. from:  open('data.csv', 'r').read()
            override_conn_json - string with json of a different connection
        '''
        payload = {}
        payload["templateId"] = nodegroup_id
        payload["csvContent"] = csv_str
        payload["sparqlConnection"] = override_conn_json if override_conn_json else self.USE_NODEGROUP_CONN
        '''payload["sparqlConnection"] = ""
        '''
        res = self.post_to_record_process("ingestFromCsvStringsById", payload)
        
        return res
