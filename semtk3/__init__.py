from . import util
from . import nodegroupexecclient
from . import resultsclient
from . import statusclient
from . import runtimeconstraint

import sys

# pip install requests

SEMTK3_HOST = "<unset>"
SEMTK3_CONN_OVERRIDE=None

STATUS_PORT = "12051"
RESULTS_PORT = "12052"
NODEGROUP_EXEC_PORT = "12058"

OP_MATCHES = runtimeconstraint.RuntimeConstraint.OP_MATCHES

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

def get_filter_values_by_id(nodegroup_id, target_obj_sparql_id, override_conn_json=None, limit_override=None, offset_override=None, runtime_constraints=None, edc_constraints=None, flags=None ):
    nge_client = __get_nge_client()
   
    table = nge_client.exec_async_dispatch_filter_by_id(nodegroup_id, target_obj_sparql_id, override_conn_json, limit_override, offset_override, runtime_constraints, edc_constraints, flags)
    return table

def build_constraint(sparql_id, operator, operand_list):
    ret = runtimeconstraint.RuntimeConstraint(sparql_id, operator, operand_list)
    return ret

def ingest_by_id(nodegroup_id, csv_str, override_conn_json=None):
    nge_client = __get_nge_client()
   
    table = nge_client.exec_async_ingest_from_csv(nodegroup_id, csv_str, override_conn_json)
    return table
    
def __get_nge_client():
    status_client = statusclient.StatusClient(SEMTK3_HOST+ ":" + STATUS_PORT)
    results_client = resultsclient.ResultsClient(SEMTK3_HOST+ ":" + RESULTS_PORT)
    return nodegroupexecclient.NodegroupExecClient(SEMTK3_HOST + ":" + NODEGROUP_EXEC_PORT, status_client, results_client)
    