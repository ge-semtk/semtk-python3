#
# Copyright 2021 General Electric Company
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
from . import plotspecs
from . import sparqlconnection
import json

#
# Encapsulate the JSON format of a nodegroup
#
class SparqlGraphJson:
        
    def __init__(self, json_str):
        if isinstance(json_str, str):
            self.json = json.loads(json_str)
        elif isinstance(json_str, dict):
            # backwards compatibility
            self.json = json_str 
   
    def get_plot_specs(self):
        if "plotSpecs" in self.json:
            self.plot_specs = plotspecs.PlotSpecs(self.json["plotSpecs"])
        else:
            self.plot_specs = plotspecs.PlotSpecs([])     
    
    def get_conn(self):
        if "sparqlConn" in self.json:
            return sparqlconnection.SparqlConnection(json.dumps(self.json["sparqlConn"]))
        else:
            return sparqlconnection.SparqlConnection()