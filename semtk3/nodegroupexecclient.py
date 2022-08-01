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
    
    def exec_async_dispatch_filter_by_id(self, nodegroup_id, target_obj_sparql_id, override_conn_json_str=None, limit_override=None, offset_override=None, runtime_constraints=None, edc_constraints=None, flags=None ):
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
        payload["sparqlConnection"] = override_conn_json_str if override_conn_json_str else self.USE_NODEGROUP_CONN
        payload["targetObjectSparqlId"] = target_obj_sparql_id
        if (flags):  payload["flags"] = flags
        if (runtime_constraints): payload["runtimeConstraints"] = self.to_json_array(runtime_constraints)
        if (edc_constraints): payload["externalDataConnectionConstraints"] = edc_constraints
        

        table = self.post_async_to_table("dispatchFilterById", payload)
        
        return table
    
    def exec_async_dispatch_select_by_id(self, nodegroup_id, override_conn_json_str=None, limit_override=None, offset_override=None, runtime_constraints=None, edc_constraints=None, flags=None ):
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
        payload["sparqlConnection"] = override_conn_json_str if override_conn_json_str else self.USE_NODEGROUP_CONN
        if (flags):  payload["flags"] = flags
        if (runtime_constraints): payload["runtimeConstraints"] = self.to_json_array(runtime_constraints)
        if (edc_constraints): payload["externalDataConnectionConstraints"] = edc_constraints
        

        table = self.post_async_to_table("dispatchSelectById", payload)
        
        return table
    
    def exec_async_dispatch_query_by_id(self, nodegroup_id, override_conn_json_str=None, limit_override=None, offset_override=None, runtime_constraints=None, edc_constraints=None, flags=None, query_type=None, result_type=None ):
        ''' execute default query type nodegroup id
            returns: One of: table, json, integer
            thorws: exception otherwise
        '''
        payload = {}
        if (limit_override):  payload["limitOverride"] = limit_override
        if (offset_override): payload["offsetOverride"] = offset_override
        payload["nodeGroupId"] = nodegroup_id
        payload["runtimeConstraints"] = ""
        payload["sparqlConnection"] = override_conn_json_str if override_conn_json_str else self.USE_NODEGROUP_CONN
        if (flags):  payload["flags"] = flags
        if (runtime_constraints): payload["runtimeConstraints"] = self.to_json_array(runtime_constraints)
        if (edc_constraints): payload["externalDataConnectionConstraints"] = edc_constraints
        if (query_type): payload["queryType"] = query_type
        if (result_type): payload["resultType"] = result_type
        
    
        simple_res = self.post_to_simple("dispatchQueryById", payload)
        result_type = self.get_simple_field(simple_res, self.RESULT_TYPE_KEY)
        jobid = self.get_simple_field(simple_res, self.JOB_ID_KEY)
        self.poll_until_success(jobid)
        
        return self.__get_result_type_based_result(simple_res, jobid)
        
    def exec_async_dispatch_query_from_nodegroup(self, nodegroup_str, override_conn_json_str=None, runtime_constraints=None, edc_constraints=None, flags=None, query_type=None, result_type=None ):
        ''' execute default query type nodegroup id
            returns: One of: table, json, integer
            thorws: exception otherwise
        '''
        payload = {}
        payload["jsonRenderedNodeGroup"] = nodegroup_str
        payload["sparqlConnection"] = override_conn_json_str if override_conn_json_str else self.USE_NODEGROUP_CONN
        
        if (runtime_constraints): payload["runtimeConstraints"] = self.to_json_array(runtime_constraints)
        if (edc_constraints): payload["externalDataConnectionConstraintsJson"] = edc_constraints
        if (flags):  payload["flags"] = flags
        if (query_type): payload["queryType"] = query_type
        if (result_type): payload["resultType"] = result_type
        
        simple_res = self.post_to_simple("dispatchQueryFromNodegroup", payload)
        result_type = self.get_simple_field(simple_res, self.RESULT_TYPE_KEY)
        jobid = self.get_simple_field(simple_res, self.JOB_ID_KEY)
        self.poll_until_success(jobid)
        
        return self.__get_result_type_based_result(simple_res, jobid)
        
    
    def __get_result_type_based_result(self, simple_res, jobid):
        result_type = self.get_simple_field(simple_res, self.RESULT_TYPE_KEY)
        
        if (result_type == "GRAPH_JSONLD"):
            return self.post_get_json_blob_results(jobid);
        else:
            return self.post_get_table_results(jobid);
    
    def exec_async_dispatch_count_by_id(self, nodegroup_id, override_conn_json_str=None, limit_override=None, offset_override=None, runtime_constraints=None, edc_constraints=None, flags=None ):
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
        payload["sparqlConnection"] = override_conn_json_str if override_conn_json_str else self.USE_NODEGROUP_CONN
        if (flags):  payload["flags"] = flags
        if (runtime_constraints): payload["runtimeConstraints"] = self.to_json_array(runtime_constraints)
        if (edc_constraints): payload["externalDataConnectionConstraints"] = edc_constraints
        

        table = self.post_async_to_table("dispatchCountById", payload)
        
        return table
    def exec_async_dispatch_raw_sparql(self, sparql, override_conn_json_str=None):
        ''' execute a select by nodegroup id
            returns: the table
            thorws: exception otherwise
        '''
        payload = {}
        payload["sparql"] = sparql
        payload["sparqlConnection"] = override_conn_json_str if override_conn_json_str else self.USE_NODEGROUP_CONN
        
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
    
    def exec_async_ingest_from_csv(self, nodegroup_id, csv_str, override_conn_json_str=None):
        ''' nodegroup_id - from nodegroup store
            csv_str - data, e.g. from:  open('data.csv', 'r').read()
            override_conn_json_str - string with json of a different connection
            
            returns status,warnings  where status is always a string and warnings might be ""
        '''
        payload = {}
        payload["nodegroupId"] = nodegroup_id
        payload["csvContent"] = csv_str
        payload["sparqlConnection"] = override_conn_json_str if override_conn_json_str else self.USE_NODEGROUP_CONN

        # return is:  status, warnings      
        statusMsg, warnMsg = self.post_async_to_record_process("ingestFromCsvStringsByIdAsync", payload)
        return statusMsg, warnMsg

    def exec_dispatch_combine_entities(self, target_uri, duplicate_uri, delete_predicates_from_target, delete_predicates_from_duplicate, conn_json_str):
        payload = {}
        payload["conn"] = conn_json_str
        payload["targetUri"] = target_uri
        payload["duplicateUri"] = duplicate_uri
        
        
        if delete_predicates_from_target:
            payload["deletePredicatesFromTarget"] = delete_predicates_from_target
        if delete_predicates_from_duplicate:
            payload["deletePredicatesFromDuplicate"] = delete_predicates_from_duplicate
        
        # error unless res is success info
        status = self.post_async_to_status("dispatchCombineEntities", payload)
    
        return status