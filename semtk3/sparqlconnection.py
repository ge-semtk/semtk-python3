import json

#
# Encapsulate the JSON format of SparqlConnection objects
#
class SparqlConnection:
    MODEL = "model"
    DATA = "data"
    
    def __init__(self, conn_json_str, user_name = None, password = None):
        self.conn_json = json.loads(conn_json_str)
        self.user_name = user_name
        self.password = password
        
    def get_server_and_port(self, model_or_data, index):
        return self.conn_json[model_or_data][index]["url"]
    
    def get_server_type(self, model_or_data, index):
        return self.conn_json[model_or_data][index]["type"]
    
    def get_graph(self, model_or_data, index):
        return self.conn_json[model_or_data][index]["graph"]
    
    def get_user_name(self):
        return self.user_name
    
    def get_password(self):
        return self.password
        
    