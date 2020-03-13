from . import util
from . import fdccacheclient
from . import nodegroupclient
from . import nodegroupexecclient
from . import oinfoclient
from . import queryclient
from . import hiveclient
from . import resultsclient
from . import statusclient
from . import runtimeconstraint
from . import sparqlconnection
from . import semtkasyncclient
from . import semtktable

import sys
from semtk3.oinfoclient import OInfoClient

# pip install requests

SEMTK3_HOST = "<unset>"
SEMTK3_CONN_OVERRIDE=None

SEMTK3_CONN_MODEL = sparqlconnection.SparqlConnection.MODEL
SEMTK3_CONN_DATA = sparqlconnection.SparqlConnection.DATA

QUERY_PORT = "12050"
STATUS_PORT = "12051"
RESULTS_PORT = "12052"
HIVE_PORT = "12055"
OINFO_PORT = "12057"
NODEGROUP_EXEC_PORT = "12058"
NODEGROUP_PORT = "12059"
FDCCACHE_PORT = "12068"

OP_MATCHES = runtimeconstraint.RuntimeConstraint.OP_MATCHES

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
    global SEMTK3_HOST
    SEMTK3_HOST = hostUrl
    
def select_by_id(nodegroup_id, limit_override=0, offset_override=0, runtime_constraints=None, edc_constraints=None, flags=None ):
    nge_client = __get_nge_client()
   
    table = nge_client.exec_async_dispatch_select_by_id(nodegroup_id, SEMTK3_CONN_OVERRIDE, limit_override, offset_override, runtime_constraints, edc_constraints, flags)
    return table

def get_constraints_by_id(nodegroup_id):
    nge_client = __get_nge_client()
   
    table = nge_client.exec_get_runtime_constraints_by_id(nodegroup_id)
    return table

def get_filter_values_by_id(nodegroup_id, target_obj_sparql_id, override_conn_json_str=None, limit_override=None, offset_override=None, runtime_constraints=None, edc_constraints=None, flags=None ):
    nge_client = __get_nge_client()
   
    table = nge_client.exec_async_dispatch_filter_by_id(nodegroup_id, target_obj_sparql_id, override_conn_json_str, limit_override, offset_override, runtime_constraints, edc_constraints, flags)
    return table

def build_constraint(sparql_id, operator, operand_list):
    ret = runtimeconstraint.RuntimeConstraint(sparql_id, operator, operand_list)
    return ret

def ingest_by_id(nodegroup_id, csv_str, override_conn_json_str=None):
    nge_client = __get_nge_client()
   
    table = nge_client.exec_async_ingest_from_csv(nodegroup_id, csv_str, override_conn_json_str)
    return table

def upload_owl(owl_file_path, conn_json_str, user_name, password, model_or_data=SEMTK3_CONN_MODEL, conn_index=0):
    query_client = __get_query_client(conn_json_str, user_name, password)
    return query_client.exec_upload_owl(owl_file_path, model_or_data, conn_index)

def query(query, conn_json_str, model_or_data=SEMTK3_CONN_DATA, conn_index=0):
    query_client = __get_query_client(conn_json_str)
    return query_client.exec_query(query, model_or_data, conn_index)

def get_oinfo_uri_label_table(conn_json_str):
    oinfo_client = __get_oinfo_client(conn_json_str)
    return oinfo_client.exec_get_uri_label_table()

def get_table(jobid):
    async_client = semtkasyncclient.SemTkAsyncClient("http://nothing");
    async_client.poll_until_success(jobid);
    return async_client.post_get_table_results(jobid);

def fdc_cache_bootstrap_table(conn_json_str, spec_id, bootstrap_table, recache_after_sec):
    cache_client = __get_fdc_cache_client()
    cache_client.exec_cache_using_table_bootstrap(conn_json_str, spec_id, bootstrap_table, recache_after_sec)
    
def get_status_client():
    return statusclient.StatusClient(SEMTK3_HOST+ ":" + STATUS_PORT)

def get_results_client():
    return resultsclient.ResultsClient(SEMTK3_HOST+ ":" + RESULTS_PORT)


#
# params:
#    class_uri - class of node to add
#    sparql_id - optional sparql id
#
# ret["sparqlID"] will be sparqlID of new node
# ret["nodegroup"] will be nodegroup json
#
def create_nodegroup(conn_json_str, class_uri, sparql_id=None):
    ng_client = __get_nodegroup_client()
    ret = ng_client.exec_create_nodegroup(conn_json_str, class_uri, sparql_id)
    return ret 

def override_ports(hive_port=None):
    global HIVE_PORT
    if hive_port: HIVE_PORT = hive_port

def query_hive(hiveserver_host, hiveserver_port, hiveserver_database, query):
    hive_client = __get_hive_client(hiveserver_host, hiveserver_port, hiveserver_database)
    return hive_client.exec_query_hive(query)

##############################
def __get_fdc_cache_client():
    status_client = statusclient.StatusClient(SEMTK3_HOST+ ":" + STATUS_PORT)
    results_client = resultsclient.ResultsClient(SEMTK3_HOST+ ":" + RESULTS_PORT)
    return fdccacheclient.FdcCacheClient(SEMTK3_HOST + ":" + FDCCACHE_PORT, status_client, results_client)

def __get_nge_client():
    status_client = statusclient.StatusClient(SEMTK3_HOST+ ":" + STATUS_PORT)
    results_client = resultsclient.ResultsClient(SEMTK3_HOST+ ":" + RESULTS_PORT)
    return nodegroupexecclient.NodegroupExecClient(SEMTK3_HOST + ":" + NODEGROUP_EXEC_PORT, status_client, results_client)

def __get_query_client(conn_json_str, user_name=None, password=None):
    conn = sparqlconnection.SparqlConnection(conn_json_str, user_name, password)
    return queryclient.QueryClient( (SEMTK3_HOST+ ":" + QUERY_PORT), conn)

def __get_oinfo_client(conn_json_str):
    return oinfoclient.OInfoClient( (SEMTK3_HOST+ ":" + OINFO_PORT), conn_json_str)

def __get_nodegroup_client():
    status_client = statusclient.StatusClient(SEMTK3_HOST+ ":" + STATUS_PORT)
    results_client = resultsclient.ResultsClient(SEMTK3_HOST+ ":" + RESULTS_PORT)
    return nodegroupclient.NodegroupClient(SEMTK3_HOST + ":" + NODEGROUP_PORT, status_client, results_client)

def __get_hive_client(hiveserver_host, hiveserver_port, hiveserver_database):
    status_client = statusclient.StatusClient(SEMTK3_HOST+ ":" + STATUS_PORT)
    results_client = resultsclient.ResultsClient(SEMTK3_HOST+ ":" + RESULTS_PORT)
    return hiveclient.HiveClient( (SEMTK3_HOST+ ":" + HIVE_PORT), hiveserver_host, hiveserver_port, hiveserver_database, status_client, results_client)    