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
class SparqlConnection:
    MODEL = "model"
    DATA = "data"
    
    def __init__(self, conn_json, user_name = None, password = None):
        self.conn_json = conn_json
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
    
    def get_all_triplestore_urls(self):
        ret = []
        for sei in (self.conn_json[self.MODEL] + self.conn_json[self.DATA]):
            if not (sei["url"] in ret):
                ret.append(sei["url"])
        return ret
            
        
    