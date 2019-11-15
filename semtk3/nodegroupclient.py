from . import semtkasyncclient
from . import util

class NodegroupClient(semtkasyncclient.SemTkAsyncClient):
    USE_NODEGROUP_CONN = "{\"name\": \"%NODEGROUP%\",\"domain\": \"%NODEGROUP%\",\"model\": [],\"data\": []}"
    
    def __init__(self, serverURL, status_client, results_client):
        ''' servierURL string - e.g. http://machine:8099
            status_client 
            results_client 
        '''
        super(NodegroupClient, self).__init__(serverURL, "nodeGroup", status_client, results_client)
    
    def exec_create_nodegroup(self, conn_json_str, class_uri, sparql_id=None):
        ''' execute a create_nodegroup
            thorws: exception otherwise
        '''
        payload = {}
        payload["conn"] = conn_json_str
        payload["uri"] = class_uri
        if (sparql_id):
            payload["sparqlID"] = sparql_id
     
        res = self.post_to_simple("createNodeGroup", payload)
        
        return res