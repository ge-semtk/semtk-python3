from . import semtkclient


class OInfoClient(semtkclient.SemTkClient):
    
    def __init__(self, serverURL, conn_json_str):
        ''' servierURL string - e.g. http://machine:12050
        '''
        super(OInfoClient, self).__init__(serverURL, "ontologyinfo")
        self.conn_json_str = conn_json_str
    
    #
    # Upload owl.
    # Default to model[0] graph in the connection
    #
    def exec_get_uri_label_table(self):
        
        payload = {
            "jsonRenderedSparqlConnection": self.conn_json_str
        }

        res = self.post_to_table("getUriLabelTable", payload)
        return res
    