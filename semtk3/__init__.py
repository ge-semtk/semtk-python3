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
# pip notes
# ---------
# pip install requests
# pip install python-dateutil
#

from . import util
from . import fdccacheclient
from . import nodegroupclient
from . import nodegroupexecclient
from . import nodegroupstoreclient
from . import oinfoclient
from . import queryclient
from . import restclient
from . import ingestionclient
from . import ontologyinfo
from . import resultsclient
from . import semtk
from . import statusclient
from . import utilityclient
from . import report
from . import runtimeconstraint
from . import sparqlconnection
from . import semtkasyncclient
from . import semtktable
from . import sparqlgraphjson
from . import predicatestats

import csv
import json
import os.path
import re
import sys
import logging
import plotly
import requests
import urllib

from semtk3.oinfoclient import OInfoClient
from semtk3.semtkasyncclient import SemTkAsyncClient
from semtk3.semtktable import SemtkTable

# pip install requests

SEMTK3_CONN_OVERRIDE=None

SEMTK3_CONN_MODEL = sparqlconnection.SparqlConnection.MODEL
SEMTK3_CONN_DATA = sparqlconnection.SparqlConnection.DATA

QUERY_TYPE_SELECT_DISTINCT = "SELECT_DISTINCT"
QUERY_TYPE_FILTER_CONSTRAINT = "FILTER_CONSTRAINT"
QUERY_TYPE_COUNT = "COUNT"
QUERY_TYPE_CONSTRUCT = "CONSTRUCT"
QUERY_TYPE_ASK = "ASK"
QUERY_TYPE_DELETE = "DELETE"

RESULT_TYPE_TABLE = "TABLE"           
RESULT_TYPE_GRAPH_JSONLD = "GRAPH_JSONLD"     
RESULT_TYPE_CONFIRM = "CONFIRM"  
RESULT_TYPE_RDF = "RDF"  
RESULT_TYPE_HTML = "HTML"

STORE_ITEM_TYPE_NODEGROUP = "PrefabNodeGroup"
STORE_ITEM_TYPE_REPORT = "Report"
STORE_ITEM_TYPE_ALL = "StoredItem"

QUERY_PORT = "12050"
STATUS_PORT = "12051"
RESULTS_PORT = "12052"
NODEGROUP_STORE_PORT = "12056"
OINFO_PORT = "12057"
NODEGROUP_EXEC_PORT = "12058"
NODEGROUP_PORT = "12059"
UTILITY_PORT = "12060"
FDCCACHE_PORT = "12068"
INGESTION_PORT="12091"

QUERY_HOST = "http://localhost"
STATUS_HOST = "http://localhost"
RESULTS_HOST = "http://localhost"
NODEGROUP_STORE_HOST = "http://localhost"
OINFO_HOST = "http://localhost"
NODEGROUP_EXEC_HOST = "http://localhost"
NODEGROUP_HOST = "http://localhost"
UTILITY_HOST = "http://localhost"
FDCCACHE_HOST = "http://localhost"
INGESTION_HOST = "http://localhost"

OP_MATCHES = runtimeconstraint.RuntimeConstraint.OP_MATCHES
OP_REGEX = runtimeconstraint.RuntimeConstraint.OP_REGEX
OP_GREATERTHAN = runtimeconstraint.RuntimeConstraint.OP_GREATERTHAN
OP_GREATERTHANOREQUALS = runtimeconstraint.RuntimeConstraint.OP_GREATERTHANOREQUALS
OP_LESSTHAN = runtimeconstraint.RuntimeConstraint.OP_LESSTHAN
OP_LESSTHANOREQUALS = runtimeconstraint.RuntimeConstraint.OP_LESSTHANOREQUALS
OP_VALUEBETWEEN = runtimeconstraint.RuntimeConstraint.OP_VALUEBETWEEN
OP_VALUEBETWEENUNINCLUSIVE = runtimeconstraint.RuntimeConstraint.OP_VALUEBETWEENUNINCLUSIVE

DEFAULT_GRAPH = "uri://DefaultGraph"

#   This is the main setup for semtk3
#   
#   All "normal" calls can be made through this object.
#
#
#
#

#
# Give extra error about missing dependencies
#
missing = []
for depend in ['requests']:
    if not (depend in sys.modules):
        missing.append(depend)
    if len(missing) > 0:
        raise Exception("Use pip.exe or others to install missing modules: " + ", ".join(missing))

###########################

def set_host(hostUrl):
    global QUERY_HOST, STATUS_HOST, RESULTS_HOST, NODEGROUP_STORE_HOST, OINFO_HOST, NODEGROUP_EXEC_HOST, NODEGROUP_HOST, UTILITY_HOST, FDCCACHE_HOST, INGESTION_HOST
        
    QUERY_HOST = hostUrl
    STATUS_HOST = hostUrl
    RESULTS_HOST = hostUrl
    NODEGROUP_STORE_HOST = hostUrl
    OINFO_HOST = hostUrl
    NODEGROUP_EXEC_HOST = hostUrl
    NODEGROUP_HOST = hostUrl
    UTILITY_HOST = hostUrl
    FDCCACHE_HOST = hostUrl
    INGESTION_HOST = hostUrl

#
# can't understand why this is needed
#

def set_headers(headers):
    restclient.RestClient.set_headers(headers);
    
def print_wait_dots(seconds):
    semtkasyncclient.SemTkAsyncClient.WAIT_MSEC = seconds * 1000
    semtkasyncclient.SemTkAsyncClient.PERCENT_INCREMENT = 100
    semtkasyncclient.SemTkAsyncClient.PRINT_DOTS = True
    
def get_logger():
    return logging.getLogger("semtk3")

def set_connection_override(conn_str):
    ''' 
    Set a connection string to be used in all nodegroups
    :param conn_str - a SemTK connection json string
    '''
    global SEMTK3_CONN_OVERRIDE
    SEMTK3_CONN_OVERRIDE = conn_str
    
def check_connection_up(conn_str):
    ''' 
    Throw exception if connection triplestore(s) don't respond OK to http GET
    :param conn_str - a SemTK connection json string
    '''
    conn = sparqlconnection.SparqlConnection(conn_str)
    for url in conn.get_all_triplestore_urls():
        response = requests.request("GET", url)
        if not response.ok:
            raise Exception("Problem connecting to triplestore url: " + url + '\n' + str(response.content))

def build_connection_str(name:str, triple_store_type:str, triple_store_url:str, model_graphs:[str], data_graph:str, extra_data_graphs:[str]=[]):
    '''
    build a connection
    @param name : name is for display only
    @param triple_store_type : "fuseki" "neptune" "virtuoso", etc.
    @param triple_store_url : the URL e.g. "http://localhost:3030/DATASET"
    @model_graphs : list of model graphs e.g. ["uri://my_graph", "http://my/other#graph"]
    @data_graph : default ingestion data graph e.g. "uri://my_graph"
    @extra_data_graphs : list of data graphs with  ["uri://my_graph", "http://my/other#graph"]
    '''
    
    # single non-list data_graph is a little confusing so quietly fix it
    if type(data_graph) == type(["list"]) and len(data_graph) == 1:
        dg = data_graph[0]
    else:
        dg = data_graph
        
    conn = sparqlconnection.SparqlConnection()
    conn.build(name, triple_store_type, triple_store_url, model_graphs, dg, extra_data_graphs)
    return conn.to_conn_str()

def build_default_connection_str(name, triple_store_type, triple_store_url):
    '''
    build a connection to the default graph only
    @param name : name is for display only
    @param triple_store_type : "fuseki" "neptune" "virtuoso", etc.
    @param triple_store_url : the URL e.g. "http://localhost:3030/DATASET"
    '''
    return build_connection_str(name, triple_store_type, triple_store_url, [DEFAULT_GRAPH], DEFAULT_GRAPH)

def clear_graph(conn_json_str, model_or_data, index):
    '''
    Clear a graph
    :param conn_json_str: connection json as a string
    :param model_or_data: string "model" or "data"
    :param index: integer specifying which model or data graph to use
    :return: message
    :rtype: string
    '''
    sparql_conn = sparqlconnection.SparqlConnection(conn_json_str)
    nge_client = __get_nge_client()
   
    table = nge_client.exec_dispatch_clear_graph(sparql_conn, model_or_data, index)
    return table.get_cell(0,0)

def copy_graph(from_graph:str, to_graph:str, from_server:str=None, from_server_type:str=None, to_server:str=None, to_server_type:str=None, user_name="noone", password="nopass"):
    '''
    Copy one graph to another (merging into the destination)
    So clear the "to" graph as a separate step if desired.
    :param from_graph - merge from this graph
    :param to_graph - merge to this graph
    :param from_server - merge from this server.     if None: get from SEMTK_CONN_OVERRIDE.data[0]
    :param from_server_type - type of 'from' server. if None: get from SEMTK_CONN_OVERRIDE.data[0]
    :param to_server - merge to this server.         if None: get from SEMTK_CONN_OVERRIDE.data[0]
    :param to_server_type - type of 'to' server.     if None: get from SEMTK_CONN_OVERRIDE.data[0]
    :param user_name - if security needed on 'to' server
    :param password - if security needed on 'to' server
    :return status message string like "successfully copied uri://from into uri://to
    :throws exception on any error
    '''
    if (SEMTK3_CONN_OVERRIDE):
        conn = sparqlconnection.SparqlConnection(SEMTK3_CONN_OVERRIDE)
        if not from_server:
            from_server = conn.get_server_and_port("data",0) 
        if not from_server_type:
            from_server_type = conn.get_server_type("data",0)
        if not to_server:
            to_server = conn.get_server_and_port("data",0) 
        if not to_server_type:
            to_server_type = conn.get_server_type("data",0)
    elif not from_server or not from_server_type or not to_server or not to_server_type:
        raise Exception("empty params are invalid when there is no SEMTK3 connection override")
    
    nge_client = __get_nge_client()
    status = nge_client.exec_copy_graph(from_server, from_server_type, from_graph, to_server, to_server_type, to_graph, user_name, password)
    return status

def check_services():
    '''
    logs success or failure of each service
    :return: did all pings succeed
    :rtype: boolean
    '''
    # these are not used right now
    b1 = True  # __get_fdc_cache_client().ping()
    b2 = True  #__get_hive_client("server", "host", "db").ping()
    
    b3 = __get_nge_client().ping() 
    b4 = __get_nodegroup_client().ping()
    b5 = __get_oinfo_client("{}").ping()
    b6 = __get_query_client("{}").ping()
    b7 = __get_status_client().ping()
    b8 = __get_results_client().ping()
    b9 = __get_ingestion_client().ping()
    
    return b1 and b2 and b3 and b4 and b5 and b6 and b7 and b8 and b9

def query_by_id(nodegroup_id, limit_override=0, offset_override=0, runtime_constraints=None, edc_constraints=None, flags=None, query_type=None, result_type=None):
    '''
    Execute the default query type for a given nodegroup id
    
    Check results for type(result) is 
        dict - json ld results
        semtk3.semtktable.SemtkTable
            A count query will be a SemtkTable with colum nname "count"
            A confirm query will be a SemtkTable with column name "@message"
    
    :param nodegroup_id: id of nodegroup in the store
    :param limit_override: optional override of LIMIT clause
    :param offset_override: optional override of OFFSET clause
    :param runtime_constraints: optional runtime constraints built by build_constraint()
    :param edc_constraints: optional edc constraints
    :param flags: optional query flags
    :return: results  : dict or semtk3.semtktable.SemtkTable
    :rtype: semtktable or JSON
    '''
    nge_client = __get_nge_client()
   
    res = nge_client.exec_async_dispatch_query_by_id(nodegroup_id, SEMTK3_CONN_OVERRIDE, limit_override, offset_override, runtime_constraints, edc_constraints, flags, query_type, result_type)
    return res

def query_by_nodegroup(nodegroup_str, runtime_constraints=None, edc_constraints=None, flags=None, query_type=None, result_type=None):
    '''
    Execute the default query type for a given nodegroup id
    
    Check results for type(result) is 
        dict - json ld results
        semtk3.semtktable.SemtkTable
            A count query will be a SemtkTable with colum nname "count"
            A confirm query will be a SemtkTable with column name "@message"
    
    :param nodegroup_str: nodegroup 
    :param runtime_constraints: optional runtime constraints built by build_constraint()
    :param edc_constraints: optional edc constraints
    :param flags: optional query flags
    :return: results  : dict or semtk3.semtktable.SemtkTable
    :rtype: semtktable or JSON
    '''
    nge_client = __get_nge_client()
   
    res = nge_client.exec_async_dispatch_query_from_nodegroup(nodegroup_str, SEMTK3_CONN_OVERRIDE, runtime_constraints, edc_constraints, flags, query_type, result_type)
    return res
    
def select_by_id(nodegroup_id, limit_override=0, offset_override=0, runtime_constraints=None, edc_constraints=None, flags=None ):
    '''
    Execute a select query for a given nodegroup id
    :param nodegroup_id: id of nodegroup in the store
    :param limit_override: optional override of LIMIT clause
    :param offset_override: optional override of OFFSET clause
    :param runtime_constraints: optional runtime constraints built by build_constraint()
    :param edc_constraints: optional edc constraints
    :param flags: optional query flags
    :return: results
    :rtype: semtktable
    '''
    nge_client = __get_nge_client()
   
    table = nge_client.exec_async_dispatch_select_by_id(nodegroup_id, SEMTK3_CONN_OVERRIDE, limit_override, offset_override, runtime_constraints, edc_constraints, flags)
    return table

def combine_entities(target_uri, duplicate_uri, delete_predicates_from_target=None, delete_predicates_from_duplicate=None, conn=None):
    '''
    Combine two entities.  Exception on failure.
    :param target_uri: target instance to be combined INTO
    :param duplicate_uri: duplicate instance to be combined then removed
    :param delete_predicates_from_target: list of predicate URIs to be deleted from target
    :param delete_predicates_from_duplicate: list of predicate URIs to be deleted from duplicate
    :param conn: connection (can also be set with set_connection_override())
    '''
    
    my_conn = conn or SEMTK3_CONN_OVERRIDE
    if not my_conn:
        raise Exception("No connection specified.  Use conn param or set_connection_override() function")
    
    nge_client = __get_nge_client()
   
    nge_client.exec_dispatch_combine_entities(target_uri, duplicate_uri, delete_predicates_from_target, delete_predicates_from_duplicate, my_conn)

def combine_entities_table(csv_str, target_col_prop_dict, duplicate_col_prop_dict, delete_predicates_from_target=[], delete_predicates_from_duplicate=[], conn=None):
    '''
    Combine entities described by rows in a table.
    Each row has col(s) to lookup the target and duplicate
    Any properties outgoing from duplicate are ignored if they exist in the target
    All incoming properties are combined
    delete_predicates_from_* parameters occur before combining using the above rules
    
    "#type" may be used as a property shorthand
    
    :param csv_str: csv table of entities to combine
    :param target_col_prop_dict: dictionary describing how to look up target dict[col_name]=prop_uri
    :param duplicate_col_prop_dict: dictionary describing how to look up duplicate dict[col_name]=prop_uri
    :param delete_predicates_from_target: list of propertyURI's to remove from target before combining
    :param delete_predicatges_from_duplicate: list of propertyURI's to remove from duplicate before combining
    :param conn : connection
    :return status string
    :throws exception with table of errors
    '''
    my_conn = conn or SEMTK3_CONN_OVERRIDE
    if not my_conn:
        raise Exception("No connection specified.  Use conn param or set_connection_override() function")
    
    nge_client = __get_nge_client()
   
    status = nge_client.exec_dispatch_combine_entities_table(csv_str, target_col_prop_dict, duplicate_col_prop_dict, delete_predicates_from_target, delete_predicates_from_duplicate, my_conn)
    return status

def get_plot_spec_names_by_id(nodegroup_id):
    '''
    Get available plot names for a given nodegroup id
    '''
    nodegroupStr = get_nodegroup_by_id(nodegroup_id)
    sg_json = sparqlgraphjson.SparqlGraphJson(json.loads(nodegroupStr))       
    return sg_json.get_plot_specs().get_plot_spec_names()

def get_sparqlgraph_url(host_url:str, nodegroup_id:str = None,  report_id:str = None, runtime_constraints:[runtimeconstraint.RuntimeConstraint]=None, run_flag:str = None, conn_json_str:str = None):
    '''
    Get a URL for sparqlgraph with params to launch a connection, nodegroup, query
    :param host_url: str - base url e.g. http://localhost:8080
    :param nodegroup_id : str - id of nodegroup in the store to launch.  By default, run the query.
    :param report_id : str - id of report in the store to launch.  By default, run the report.
    :param runtime_constraints: [RuntimeConstraint] - constraints to apply to query if nodegroup_id is specified
    :param run_flag: str - "True" or "False", default "True"
    :param conn_json_str: str - connection to load.  Will override nodegroup_id's.
    :return: url
    :rtype: string
    '''
    params = []
    if nodegroup_id:
        params.append("nodegroupId=" + urllib.parse.quote(nodegroup_id))
        
    if report_id:
        params.append("reportId=" + urllib.parse.quote(report_id))
        
    if runtime_constraints:
        constraints_str = "[" + ",".join(x.to_json() for x in runtime_constraints) + "]"
        params.append("constraints=" + urllib.parse.quote(constraints_str))
    
    if run_flag:
        params.append("runFlag=" + urllib.parse.quote(run_flag))
        
    if conn_json_str:
        params.append("conn=" + urllib.parse.quote(conn_json_str))
        
    ret = host_url + "/sparqlGraph/main-oss/sparqlGraph.html"
    if len(params) > 0:
        ret += "?" + "&".join(params)
    
    return ret
    

def select_plot_by_id(nodegroup_id, plot_name):
    '''
    Create a plot for a given nodegroup id
    '''
    table = select_by_id(nodegroup_id)                                       # get results table
    nodegroupStr = get_nodegroup_by_id(nodegroup_id)                         # get nodegroup string
    sg_json = sparqlgraphjson.SparqlGraphJson(json.loads(nodegroupStr))       
    plot_spec = sg_json.get_plot_specs().get_plot_spec_by_name(plot_name)                    # get plot spec
    plot_spec_processed = __get_utility_client().exec_process_plot_spec(plot_spec, table)    # populate plot spec with data
    return plotly.graph_objects.Figure(plot_spec_processed.get_spec())

def count_by_id(nodegroup_id, limit_override=0, offset_override=0, runtime_constraints=None, edc_constraints=None, flags=None ):
    '''
    Execute a count query for a given nodegroup id
    :param nodegroup_id: id of nodegroup in the store
    :param limit_override: optional override of LIMIT clause
    :param offset_override: optional override of OFFSET clause
    :param runtime_constraints: optional runtime constraints built by build_constraint()
    :param edc_constraints: optional edc constraints
    :param flags: optional query flags
    :return: results
    :rtype: semtktable
    '''
    nge_client = __get_nge_client()
   
    table = nge_client.exec_async_dispatch_count_by_id(nodegroup_id, SEMTK3_CONN_OVERRIDE, limit_override, offset_override, runtime_constraints, edc_constraints, flags)
    return table

def get_constraints_by_id(nodegroup_id):
    '''
    Get runtime constraints for a stored nodegroup
    :param nodegroup_id: the id
    :return: columns valueId, itemType and valueType
    :rtype: semtktable
    '''
    nge_client = __get_nge_client()
   
    table = nge_client.exec_get_runtime_constraints_by_id(nodegroup_id)
    return table

def get_filter_values_by_id(nodegroup_id, target_obj_sparql_id, override_conn_json_str=None, limit_override=None, offset_override=None, runtime_constraints=None, edc_constraints=None, flags=None ):
    '''
    Run a filter values query, which returns all the existing values for a given variable in the nodegroup
    :param nodegroup_id: the id
    :param target_obj_sparql_id: the variable to be interrogated
    :param override_conn_json_str: optional override connection json string
    :param limit_override: optional override of LIMIT clause
    :param offset_override: optional override of OFFSET clause
    :param runtime_constraints: optional runtime constraints built by build_constraint()
    :param edc_constraints: optional edc constraints
    :param flags: optional query flags
    :return: results
    :rtype semtktable
    '''
    nge_client = __get_nge_client()
   
    table = nge_client.exec_async_dispatch_filter_by_id(nodegroup_id, target_obj_sparql_id, override_conn_json_str, limit_override, offset_override, runtime_constraints, edc_constraints, flags)
    return table

def build_constraint(sparql_id, operator, operand_list):
    '''
    Build a contraint to be used as a query parameter
    :param sparql_id: the variable name
    :param operator: operator {MATCHES,REGEX,GREATERTHAN,GREATERTHANOREQUALS,LESSTHAN,LESSTHANOREQUALS,VALUEBETWEEN,VALUEBETWEENUNINCLUSIVE}
    :param operand_list: list of values
    :return: the constraint
    :rettype: RuntimeConstraint
    '''
    ret = runtimeconstraint.RuntimeConstraint(sparql_id, operator, operand_list)
    return ret
    
def ingest_by_id(nodegroup_id, csv_str, override_conn_json_str=None):
    '''
    Perform data ingestion, throwing exception on failure
    :param nodegroup_id: nodegroup with ingestion template
    :param csv_str: string csv data
    :param override_conn_json_str: optional override connection
    :return: (statusMsg, warnMsg)   where warnMsg is often ''
    :rettype: string tuple
    '''
    nge_client = __get_nge_client()
   
    (statusMsg, warnMsg) = nge_client.exec_async_ingest_from_csv(nodegroup_id, csv_str, override_conn_json_str)
    return (statusMsg, warnMsg)

def ingest_using_class_template(class_uri, csv_str, conn_json_str=None, id_regex="identifier"):
    '''
    Ingest using class template, throwing exception on failure
    :param class_uri : the class whose template should be used for ingestion
    :param csv_str: string csv data
    :param id_regex: regex matching properties that should be used for lookups
    :conn_json_str: connection
    :return: (statusMsg, warnMsg)   where warnMsg is often ''
    :rettype: string tuple
    '''
    ingest_client = __get_ingestion_client()
    (statusMsg, warnMsg) = ingest_client.exec_from_csv_using_class_template(class_uri, csv_str, conn_json_str if conn_json_str else SEMTK3_CONN_OVERRIDE, id_regex)
    return (statusMsg, warnMsg)

def get_class_template_csv(class_uri, conn_json_str=None, id_regex="identifier"):
    '''
    Get sample CSV that will work with  class template
    :param class_uri : the class whose template should be used for ingestion
    :param conn_json_str : optional conenction json string defaults to override
    :param id_regex : optional regex to identify the key data properties of classes which are the object of object properties
    :returns "colname1, colname2, colname3"
    '''
    ingest_client = __get_ingestion_client()
    (ng_json_str, csv_header, csv_types) =  ingest_client.exec_get_class_template_and_csv(class_uri, conn_json_str if conn_json_str else SEMTK3_CONN_OVERRIDE, id_regex)
    return csv_header

def get_class_template(class_uri, conn_json_str=None, id_regex="identifier"):
    '''
    Get class template nodegroup
    :param class_uri : the class whose template should be used for ingestion
    :param conn_json_str : optional conenction json string defaults to override
    :param id_regex : optional regex to identify the key data properties of classes which are the object of object properties
    :returns nodegroup json string
    '''
    ingest_client = __get_ingestion_client()
    (ng_json_str, csv_header, csv_types) =  ingest_client.exec_get_class_template_and_csv(class_uri, conn_json_str if conn_json_str else SEMTK3_CONN_OVERRIDE, id_regex)
    return ng_json_str

def get_class_template_and_csv(class_uri, conn_json_str=None, id_regex="identifier"):
    '''
    Get class template nodegroup
    :param class_uri : the class whose template should be used for ingestion
    :param conn_json_str : optional conenction json string defaults to override
    :param id_regex : optional regex to identify the key data properties of classes which are the object of object properties
    :returns (ng_json_str, "col1, col2, col3\n", "string, int, dateTime\n")  note that types can be space-separated complex property types
    '''
    ingest_client = __get_ingestion_client()
    (ng_json_str, csv_header, csv_types) =  ingest_client.exec_get_class_template_and_csv(class_uri, conn_json_str if conn_json_str else SEMTK3_CONN_OVERRIDE, id_regex)
    return (ng_json_str, csv_header, csv_types)

def get_class_names(conn_json_str=None):    
    '''
    Get a list of class names in the ontology
    :param conn_json_str : optional conenction json string defaults to override
    :returns list of full class URI's
    ''' 
    oinfo = get_oinfo(conn_json_str)  
    class_list = oinfo.get_class_list()
    return class_list
 

def upload_owl(owl_file_path, conn_json_str, user_name="noone", password="nopass", model_or_data=SEMTK3_CONN_MODEL, conn_index=0):
    '''
    Upload an owl file to a given graph
    :param owl_file_path: path to the file
    :param conn_json_str: connection json string
    :param user_name: optional user name
    :param password: optional password
    :param model_or_data: optional "model" or "data" specifying which graph in the sparql connection, defaults to "model"
    :param conn_index: index specifying which of the model or data graphs in the sparql connection, defaults to 0
    :return: message
    :rettype: string
    '''
    query_client = __get_query_client(conn_json_str, user_name, password)
    return query_client.exec_upload_owl(owl_file_path, model_or_data, conn_index)

def download_owl(owl_file_path, conn_json_str, user_name="noone", password="nopass", model_or_data=SEMTK3_CONN_MODEL, conn_index=0):
    '''
    Download a graph as an OWL file
    :param owl_file_path: path to the file
    :param conn_json_str: connection json string (defaults to the first MODEL graph in the connection)
    :param user_name: optional user name
    :param password: optional password
    :param model_or_data: optional "model" or "data" specifying which endpoint in the sparql connection, defaults to "data"
    :param conn_index: index specifying which of the model or data endpoints in the sparql connection, defaults to 0
    :return: None - raises exception on error
    '''
    query_client = __get_query_client(conn_json_str, user_name, password)
    query_client.exec_download_owl(owl_file_path, model_or_data, conn_index)

def upload_turtle(ttl_file_path, conn_json_str, user_name, password, model_or_data=SEMTK3_CONN_MODEL, conn_index=0):
    '''
    Upload an turtle file
    :param ttl_file_path: path to the file
    :param conn_json_str: connection json string
    :param user_name: optional user name
    :param password: optional password
    :param model_or_data: optional "model" or "data" specifying which endpoint in the sparql connection, defaults to "model"
    :param conn_index: index specifying which of the model or data endpoints in the sparql connection, defaults to 0
    :return: message
    :rettype: string
    '''
    query_client = __get_query_client(conn_json_str, user_name, password)
    return query_client.exec_upload_turtle(ttl_file_path, model_or_data, conn_index)

def query(query, conn_json_str, model_or_data=SEMTK3_CONN_DATA, conn_index=0):
    '''
    Run a raw SPARQL query
    :param query: SPARQL
    :param conn_json_str: connection json string
    :param model_or_data: optional "model" or "data" specifying which endpoint in the sparql connection, defaults to "data"
    :param conn_index: index specifying which of the model or data endpoints in the sparql connection, defaults to 0
    :return: results
    :rettype: semtktable
    
    '''
    query_client = __get_query_client(conn_json_str)
    return query_client.exec_query(query, model_or_data, conn_index)

def get_graph_names(conn_json_str):
    '''
    Get graph names present in the triple store
    :param conn_json_str: connection json string
    :return: a list of graph names
    :rettype: semtktable
    '''
    query_client = __get_query_client(conn_json_str)
    return query_client.exec_select_graph_names()

def get_nodegroup_by_id(nodegroup_id):
    '''
    Retrieve a nodegroup from the store
    :param nodegroup_id: the id
    :return: a nodegroup
    :rettype: json string
    '''
    return get_store_item(nodegroup_id, STORE_ITEM_TYPE_NODEGROUP)

def get_store_item(item_id, item_type):
    
    store_client = __get_nodegroup_store_client()
    table = store_client.exec_get_stored_item_by_id(item_id, item_type)
    if table.get_num_rows() < 1:
        raise Exception("Could not find store item with id: " + item_id + " and type: " + item_type)
    
    return table.get_cell(0, table.get_column_index("item"))
 
def get_nodegroup_store_data():
    '''
    Get list of nodegroups in the nodegroup store
    :return: SemtkTable with columns 'ID', 'comments', 'creationDate', 'creator', 'itemType'
    :rettype: semtktable
    '''
    return get_store_table(STORE_ITEM_TYPE_NODEGROUP)

def get_store_table(item_type=STORE_ITEM_TYPE_ALL):
    '''
    Get list of everything in the store
    :param item_type: one of the STORE_ITEM_TYPE constants
    :return: SemtkTable with columns 'ID', 'comments', 'creationDate', 'creator', 'itemType'
    :rettype: semtktable
    '''
    store_client = __get_nodegroup_store_client()
    return store_client.exec_get_stored_items_metadata(item_type)

def delete_nodegroup_from_store(nodegroup_id):
    '''
    Delete nodegroup_id from the store error if it doesn't exist.
    :param nodegroup_id: the id
    '''
    return delete_item_from_store(nodegroup_id, STORE_ITEM_TYPE_NODEGROUP)

def delete_item_from_store(item_id, item_type):
    '''
    Delete item from the store, error if it doesn't exist.
    :param item_id: the id
    :param item_type: one of STORE_ITEM_TYPE_
    '''
    store_client = __get_nodegroup_store_client()
    store_client.exec_delete_stored_item(item_id, item_type)
    return

def store_nodegroup(nodegroup_id, comments, creator, nodegroup_json_str, overwrite_flag=False):
    '''
    Saves a single nodegroup to the store, fails if nodegroup_id already exists unless overwrite_flag
    :param nodegroup_id: the id
    :param comments: comment string
    :param creator: creator string
    :param nodegroup_json_str: nodegroup in json string form
    :return: status
    :rettype: string
    '''
    return store_item(nodegroup_id, comments, creator, nodegroup_json_str, STORE_ITEM_TYPE_NODEGROUP, overwrite_flag)

def store_item(item_id, comments, creator, item_json_str, item_type, overwrite_flag=False):
    '''
    Saves a single item to the store, fails if nodegroup_id already exists unless overwrite_flag
    :param item_id: the id
    :param comments: comment string
    :param creator: creator string
    :param item_json_str: json string of NODEGROUP or REPORT, etc.
    :param item_type: one of the STORE_ITEM_TYPE constants
    :param overwrite_flag: if true then silently overwrite existing item with the same name
    :return: status
    :rettype: string
    '''
    if overwrite_flag:
        try:
            delete_item_from_store(item_id, item_type)
        except Exception as e: 
            if "No stored item exists with id" not in str(e):
                raise e
        
    store_client = __get_nodegroup_store_client()
    return store_client.exec_store_item(item_id, comments, creator, item_json_str, item_type)


def store_nodegroups(folder_path):
    store_folder(folder_path)

def store_folder(folder_path):
    '''
    Reads a file of the standard "store_data.csv" format
        ID,comments,creator,jsonfile, optional: type
        id27,Test comments,200001111,file.json
    
    ...and saves the specified nodegroups to the store, overwriting existing if needed
    
    :param folder_path: target folder
    '''
    
    # get current store data: id and type columns
    table = get_store_table(STORE_ITEM_TYPE_ALL)
    id_list = table.get_column("ID")
    type_list = table.get_column("itemType")
        
    filename = os.path.join(folder_path, "store_data.csv")
    with open(filename, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            item_id = row["ID"]
            if "itemType" in row.keys():
                item_type = row["itemType"]
                if "nodegroup" in item_type.lower():
                    item_type = STORE_ITEM_TYPE_NODEGROUP 
            else:
                item_type = STORE_ITEM_TYPE_NODEGROUP
            
            # delete if already exists of the same type
            if item_id in id_list:
                i = id_list.index(item_id)

                if (type_list[i].endswith(item_type)):
                    delete_item_from_store(item_id, item_type)

            # read the json and store the nodegroup       
            json_path = os.path.join(folder_path, row["jsonFile"])
            with open(json_path,'r') as json_file:   
                nodegroup_json_str = json_file.read()
                # store item.  
                # Overwriting is already handled more effciently above, so overwrite_flag=False
                store_item(item_id, row["comments"], row["creator"], nodegroup_json_str, item_type, False)
            
def retrieve_from_store(regex_str, folder_path):
    print("retrieve_from_store() is deprecated.  Use retrieve_nodegroups_from_store() or retrieve_items_from_store()")
    return retrieve_nodegroups_from_store(regex_str, folder_path)

def retrieve_nodegroups_from_store(regex_str, folder_path): 
    return retrieve_items_from_store(regex_str, folder_path, STORE_ITEM_TYPE_NODEGROUP)

def retrieve_items_from_store(regex_str, folder_path, item_type=STORE_ITEM_TYPE_ALL): 
    '''
    Retrieve all items matching a pattern, create store_data.csv
    :param regex_str: pattern to match on nodegroup id's
    :param folder_path: target folder
    '''
    
    # open the output and write the header
    with open(os.path.join(folder_path, "store_data.csv"), "w") as store_data:
        store_writer = _init_store_data_csv(store_data)
        
        # get store data
        regex = re.compile(regex_str)
        store_table = get_store_table(item_type)
        
        for i in range(store_table.get_num_rows()):
            
            # for rows matching regex
            if (regex.search(store_table.get_cell(i,"ID"))):
                item_id = store_table.get_cell(i, "ID")
                comments = store_table.get_cell(i, "comments")
                creator = store_table.get_cell(i, "creator")
                if (store_table.has_column("itemType")):
                    item_type = store_table.get_cell(i, "itemType").split("#")[1]
                else:
                    item_type = STORE_ITEM_TYPE_NODEGROUP
               
                _write_item_file(item_id, item_type, get_store_item(item_id, item_type), folder_path)
                _add_to_store_data_csv(store_writer, item_id, comments, creator, item_type)


def retrieve_reports_from_store(regex_str, folder_path): 
    '''
    Retrieve all items matching a pattern, create store_data.csv
    Retrieves reports and any nodegroups they use
    :param regex_str: pattern to match on nodegroup id's
    :param folder_path: target folder
    '''
    written_ng = []
    
    # open the output and write the header
    with open(os.path.join(folder_path, "store_data.csv"), "w") as store_data:
        store_writer = _init_store_data_csv(store_data)
        
        # get nodegroup data
        ng_table = get_store_table(STORE_ITEM_TYPE_NODEGROUP)
        
        # get report data
        report_table = get_store_table(STORE_ITEM_TYPE_REPORT)
        report_row_nums = report_table.get_matching_row_nums("ID", regex_str)
        
        for i in report_row_nums:
            report_id = report_table.get_cell(i, "ID")
            comments = report_table.get_cell(i, "comments")
            creator = report_table.get_cell(i, "creator")
            item_type = STORE_ITEM_TYPE_REPORT
            
            report_json_str = get_store_item(report_id, item_type)
            report_obj = report.Report(report_json_str)
            _write_item_file(report_id, item_type, report_json_str, folder_path)
            _add_to_store_data_csv(store_writer, report_id, comments, creator, item_type)
            
            nodegroup_id_list = report_obj.get_nodegroup_ids()
            
            for ng_id in nodegroup_id_list:
                if not (ng_id in written_ng):
                    rows = ng_table.get_matching_row_nums("ID", "^"+ng_id+"$")
                    if len(rows) > 1:
                        raise Exception("Found more than one nodegroup with id: " + ng_id)
                    elif len(rows) == 0:
                        raise Exception("Could not find nodegroup with id: " + ng_id + " for report: " + report_id)
                    else:
                        ng_id = ng_table.get_cell(rows[0], "ID")
                        comments = ng_table.get_cell(rows[0], "comments")
                        creator = ng_table.get_cell(rows[0], "creator")
                        item_type = STORE_ITEM_TYPE_NODEGROUP
                        
                        ng_json_str = get_store_item(ng_id, item_type)
                        _write_item_file(ng_id, item_type, ng_json_str, folder_path)
                        _add_to_store_data_csv(store_writer, ng_id, comments, creator, item_type)

def _init_store_data_csv(f):
    store_writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL,lineterminator='\n')
    headers = ['ID', 'comments', 'creator', 'jsonFile', 'itemType']
    store_writer.writerow(headers)
    return store_writer

def _add_to_store_data_csv(store_writer, item_id, comments, creator, item_type):
    filename = item_id +".json"
    store_writer.writerow([item_id, comments, creator, filename, item_type])     
        
def _write_item_file(item_id, item_type, json_str, folder_path):
    # get nodegroup and write it
    filename = item_id +".json"
    filepath = os.path.join(folder_path, filename)
    
    # format the json nicely
    j = json.loads(json_str)
    json_str = json.dumps(j,  indent=4, sort_keys=True)
    
    with open(filepath, "w") as f:
        f.write(json_str)
                 
def delete_nodegroups_from_store(regex_str):
    delete_items_from_store(regex_str, STORE_ITEM_TYPE_NODEGROUP)
    
def delete_items_from_store(regex_str, item_type=STORE_ITEM_TYPE_ALL):
    '''
    Delete matching nodegroups from store
    :param regex_str: pattern to search() on nodegroup id's (any match in id)
    :param item_type: only delete items of this type

    '''
    store_table = get_store_table(item_type)
    regex = re.compile(regex_str)

    for i in range(store_table.get_num_rows()):
        item_id = store_table.get_cell(i, "ID")
        if (regex.search(item_id)):  
            delete_item_from_store(item_id, store_table.get_cell(i, "itemType").split("#")[1])
   
    
    
def get_oinfo_uri_label_table(conn_json_str=None):
    '''
    Get a table describing the ontology model
    :param conn_json_str: connection string of graph(s) holding the model
    :rettype: semtktable
    '''
    oinfo_client = __get_oinfo_client(conn_json_str if conn_json_str else SEMTK3_CONN_OVERRIDE)
    return oinfo_client.exec_get_uri_label_table()

def get_oinfo_predicate_stats(conn_json_str=None):
    '''
    Get a table describing the ontology model
    :param conn_json_str: connection string of graph(s) holding the model
    :rettype: semtktable
    '''
    oinfo_client = __get_oinfo_client(conn_json_str if conn_json_str else SEMTK3_CONN_OVERRIDE)
    j = oinfo_client.exec_get_predicate_stats()
    return predicatestats.PredicateStats(j)

def get_oinfo(conn_json_str=None):
    '''
    Get a table describing the ontology model
    :param conn_json_str: connection string of graph(s) holding the model
    :rettype: semtktable
    '''
    oinfo_client = __get_oinfo_client(conn_json_str if conn_json_str else SEMTK3_CONN_OVERRIDE)
    oinfo_json = oinfo_client.exec_get_ontology_info()
    return ontologyinfo.OntologyInfo(oinfo_json)

def get_instance_dictionary(max_words: int = 2, specificity_limit: int = 1, conn_json_str: str = None) -> SemtkTable:
    '''
    Get a table describing the uris and their labels.  Columns: 
     *   instance_uri - the URI
     *   class_uris - instance belongs to one or more classes
     *   label - label (or name) associated with the instance.  NOT UNIQUE: see label_specificity
     *   label_specificity - how many uris have this label
     *   property - what prop was used to associate label with instance_uri
    :param max_words: the maximum number of words a string may have and be considered a label
    :param specificity_limit: the maximum number of URI's one-hop from the label for it to be returned
    :param conn_json_str: connection string of graph(s) holding the model
    :rettype: semtktable
    '''
    oinfo_client = __get_oinfo_client(conn_json_str if conn_json_str else SEMTK3_CONN_OVERRIDE)
    return oinfo_client.exec_get_instance_dictionary(max_words, specificity_limit)
    
def get_table(jobid):
    '''
    Get a table from an async job
    :param jobid: the job id
    :rettype: semtktable
    '''
    async_client = semtkasyncclient.SemTkAsyncClient("http://nothing", status_client=__get_status_client(), results_client=__get_results_client());
    async_client.poll_until_success(jobid);
    return async_client.post_get_table_results(jobid);


def fdc_cache_bootstrap_table(conn_json_str, spec_id, bootstrap_table, recache_after_sec):
    '''
    Run an fdc cache spec
    :param conn_json_str: connection containing model and data graphs
    :param spec_id: the fdc cache spec identifier
    :param bootstrap_table: semtktable to kick off the cache
    :param recache_after_sec: maximum age of cache
    '''
    cache_client = __get_fdc_cache_client()
    cache_client.exec_cache_using_table_bootstrap(conn_json_str, spec_id, bootstrap_table, recache_after_sec)


def create_nodegroup(conn_json_str, class_uri, sparql_id=None):
    '''
    Create a nodegroup containing a single uri
    :param conn_json_str: connection json string
    :param class_uri: class to add
    :param sparql_id: optional sparqlID if different from ?ClassName
    :return: nodegroup
    :rettype: nodegroup json string
    '''
    ng_client = __get_nodegroup_client()
    ret = ng_client.exec_create_nodegroup(conn_json_str, class_uri, sparql_id)
    return ret 

def override_ports(query_port=None, status_port=None, results_port=None, hive_port=None, oinfo_port=None, nodegroup_exec_port=None, nodegroup_port=None, utility_port=None, fdcache_port=None, ingestion_port=None):
    '''
    Override the default port(s) for Semtk service(s).  
    Ports may be numbers (port will be appended with colon), e.g. 80 or "80"
    or context string (port will simply be appended)         e.g. "/query"
    :param query_port: optional
    :param status_port: optional
    :param results_port: optional
    :param hive_port: deprecated
    :param oinfo_port: optional
    :param nodegroup_exec_port: optional
    :param nodegroup_port: optional
    :param fdcache_port: optional
    :param ingestion_port: optional
    '''
    global QUERY_PORT, STATUS_PORT, RESULTS_PORT, HIVE_PORT, OINFO_PORT, NODEGROUP_EXEC_PORT, NODEGROUP_PORT, UTILITY_PORT, FDCCACHE_PORT
    if query_port: QUERY_PORT = query_port
    if status_port: STATUS_PORT = status_port
    if results_port: RESULTS_PORT = results_port
    if oinfo_port: OINFO_PORT = oinfo_port
    if nodegroup_exec_port: NODEGROUP_EXEC_PORT = nodegroup_exec_port
    if nodegroup_port: NODEGROUP_PORT = nodegroup_port
    if utility_port: UTILITY_PORT = utility_port
    if fdcache_port: FDCCACHE_PORT = fdcache_port 
    if ingestion_port: INGESTION_PORT = ingestion_port

def override_hosts(query_host=None, status_host=None, results_host=None, hive_host=None, oinfo_host=None, nodegroup_exec_host=None, nodegroup_host=None, utility_host=None, fdcache_host=None, ingestion_host=None):
    '''
    Override the default host(s) for Semtk service(s).  
    
    :param query_host: optional
    :param status_host: optional
    :param results_host: optional
    :param hive_host: deprecated
    :param oinfo_host: optional
    :param nodegroup_exec_host: optional
    :param nodegroup_host: optional
    :param fdcache_host: optional
    :param ingestion_host: optional
    '''
    global QUERY_HOST, STATUS_HOST, RESULTS_HOST, HIVE_HOST, NODEGROUP_STORE_HOST, OINFO_HOST, NODEGROUP_EXEC_HOST, NODEGROUP_HOST, UTILITY_HOST, FDCCACHE_HOST
    if query_host: QUERY_HOST = query_host
    if status_host: STATUS_HOST = status_host
    if results_host: RESULTS_HOST = results_host
    if oinfo_host: OINFO_HOST = oinfo_host
    if nodegroup_exec_host: NODEGROUP_EXEC_HOST = nodegroup_exec_host
    if nodegroup_host: NODEGROUP_HOST = nodegroup_host
    if utility_host: UTILITY_HOST = utility_host
    if fdcache_host: FDCCACHE_HOST = fdcache_host 
    if ingestion_host: INGESTION_HOST = ingestion_host

def query_hive(hiveserver_host, hiveserver_port, hiveserver_database, query):
    raise Exception("Hive is no longer supported")

##############################
def __get_fdc_cache_client():
    status_client = statusclient.StatusClient(__build_client_url(STATUS_HOST, STATUS_PORT))
    results_client = resultsclient.ResultsClient(__build_client_url(RESULTS_HOST, RESULTS_PORT))
    return fdccacheclient.FdcCacheClient(__build_client_url(FDCCACHE_HOST, FDCCACHE_PORT), status_client, results_client)

def __get_nge_client():
    status_client = statusclient.StatusClient(__build_client_url(STATUS_HOST, STATUS_PORT))
    results_client = resultsclient.ResultsClient(__build_client_url(RESULTS_HOST, RESULTS_PORT))
    return nodegroupexecclient.NodegroupExecClient(__build_client_url(NODEGROUP_EXEC_HOST, NODEGROUP_EXEC_PORT), status_client, results_client)

def __get_query_client(conn_json_str="{}", user_name=None, password=None):
    status_client = statusclient.StatusClient(__build_client_url(STATUS_HOST, STATUS_PORT))
    results_client = resultsclient.ResultsClient(__build_client_url(RESULTS_HOST, RESULTS_PORT))
    conn = sparqlconnection.SparqlConnection(conn_json_str, user_name, password)
    return queryclient.QueryClient( __build_client_url(QUERY_HOST, QUERY_PORT), conn)

def __get_status_client():
    return statusclient.StatusClient(__build_client_url(STATUS_HOST, STATUS_PORT))

def __get_results_client():
    return resultsclient.ResultsClient(__build_client_url(RESULTS_HOST, RESULTS_PORT))

def __get_oinfo_client(conn_json_str):
    status_client = statusclient.StatusClient(__build_client_url(STATUS_HOST, STATUS_PORT))
    results_client = resultsclient.ResultsClient(__build_client_url(RESULTS_HOST, RESULTS_PORT))
    return oinfoclient.OInfoClient( __build_client_url(OINFO_HOST, OINFO_PORT), conn_json_str, status_client, results_client)

def __get_nodegroup_store_client():
    return nodegroupstoreclient.NodegroupStoreClient( __build_client_url(NODEGROUP_STORE_HOST, NODEGROUP_STORE_PORT))

def __get_nodegroup_client():
    status_client = statusclient.StatusClient(__build_client_url(STATUS_HOST, STATUS_PORT))
    results_client = resultsclient.ResultsClient(__build_client_url(RESULTS_HOST, RESULTS_PORT))
    return nodegroupclient.NodegroupClient(__build_client_url(NODEGROUP_HOST, NODEGROUP_PORT), status_client, results_client)

def __get_utility_client():
    return utilityclient.UtilityClient( __build_client_url(UTILITY_HOST, UTILITY_PORT))

def __get_ingestion_client():
    status_client = statusclient.StatusClient(__build_client_url(STATUS_HOST, STATUS_PORT))
    results_client = resultsclient.ResultsClient(__build_client_url(RESULTS_HOST, RESULTS_PORT))
    return ingestionclient.IngestionClient( __build_client_url(INGESTION_HOST, INGESTION_PORT), status_client, results_client)

# build a url using a ":" if the port is a number, otherwise just appending it
def __build_client_url(base_url, port):
    sep = ""
    try:
        int(port)
        sep = ":"
    except:
        sep = ""
    return base_url + sep + port

def main():
    semtk.main()

