from . import semtkasyncclient
from . import sparqlconnection

import ntpath

class QueryClient(semtkasyncclient.SemTkAsyncClient):
    
    def __init__(self, serverURL, conn_obj):
        ''' servierURL string - e.g. http://machine:12050
        '''
        super(QueryClient, self).__init__(serverURL, "sparqlQueryService")
        self.conn = conn_obj
    
    #
    # Upload owl.
    # Default to model[0] graph in the connection
    #
    def exec_upload_owl(self, owl_file_path, model_or_data=sparqlconnection.SparqlConnection.MODEL, index=0):
        
        payload = {
            "serverAndPort": self.conn.get_server_and_port(model_or_data, index),
            "serverType":    self.conn.get_server_type(model_or_data, index),
            "dataset":       self.conn.get_graph(model_or_data, index),
            "user":          self.conn.get_user_name(),
            "password":      self.conn.get_password()
        }
        
        files = {
            "owlFile": (ntpath.basename(owl_file_path), open(owl_file_path, 'rb'))
        }

        res = self.post_to_status("uploadOwl", payload, files)
        return res
    
     #
    # Upload owl.
    # Default to model[0] graph in the connection
    #
    def exec_upload_turtle(self, owl_file_path, model_or_data=sparqlconnection.SparqlConnection.MODEL, index=0):
        
        payload = {
            "serverAndPort": self.conn.get_server_and_port(model_or_data, index),
            "serverType":    self.conn.get_server_type(model_or_data, index),
            "dataset":       self.conn.get_graph(model_or_data, index),
            "user":          self.conn.get_user_name(),
            "password":      self.conn.get_password()
        }
        
        files = {
            "owlFile": (ntpath.basename(owl_file_path), open(owl_file_path, 'rb'))
        }

        res = self.post_to_status("uploadTurtle", payload, files)
        return res
    
    #
    # Execute query
    #
    def exec_query(self, query, model_or_data=sparqlconnection.SparqlConnection.DATA, index=0):
        
        payload = {
            "serverAndPort": self.conn.get_server_and_port(model_or_data, index),
            "serverType":    self.conn.get_server_type(model_or_data, index),
            "graph":         self.conn.get_graph(model_or_data, index),
            "resultType":    "TABLE",
            "query":         query
        }
        
      
        res = self.post_to_table("query", payload)
        return res