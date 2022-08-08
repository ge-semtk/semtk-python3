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

#
# Encapsulate the JSON format of SparqlConnection objects
#
import json

class SparqlConnection:
    MODEL = "model"
    DATA = "data"
    
    def __init__(self, conn_json_str="{}", user_name = None, password = None):
        """ build a SparqlConnection either empty, or from a json string """
        
        if (isinstance(conn_json_str, dict)):
            # backwards compatible
            self.conn_dict = conn_json_str
        else:
            # normal
            self.conn_dict = json.loads(conn_json_str)
            
        self.user_name = user_name
        self.password = password
        
    def build(self, name, triple_store_type, triple_store, model_graphs, data_graph, extra_data_graphs=[]):
        '''
        build a connection
        @param name : name
        @param triple_store_type : "fuseki" "neptune" "virtuoso", etc.
        @param triple_store : the URL e.g. "http://localhost:3030/DATASET"
        @model_graphs : list of model graphs e.g. ["uri://my_graph", "http://my/other#graph"]
        @data_graph : default ingestion data graph e.g. "uri://my_graph"
        @extra_data_graphs : list of data graphs with  ["uri://my_graph", "http://my/other#graph"]
        '''
        self.conn_dict = {
            "name": name,
            "model": [],
            "data": []
            }
        for graph in model_graphs:
            self.conn_dict["model"].append({"type": triple_store_type, "url": triple_store, "graph": graph})
        for graph in [data_graph] + extra_data_graphs:
            self.conn_dict["data"].append({"type": triple_store_type, "url": triple_store, "graph": graph})
    
    def to_conn_str(self):
        return json.dumps(self.conn_dict)
    
    def get_server_and_port(self, model_or_data, index):
        return self.conn_dict[model_or_data][index]["url"]
    
    def get_server_type(self, model_or_data, index):
        return self.conn_dict[model_or_data][index]["type"]
    
    def get_graph(self, model_or_data, index):
        return self.conn_dict[model_or_data][index]["graph"]
    
    def get_user_name(self):
        return self.user_name
    
    def get_password(self):
        return self.password
    
    def get_all_triplestore_urls(self):
        ret = []
        for sei in (self.conn_dict[self.MODEL] + self.conn_dict[self.DATA]):
            if not (sei["url"] in ret):
                ret.append(sei["url"])
        return ret
            
        
    