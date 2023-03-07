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
from . import semtkclient
from . import plotspecs
import os.path

class UtilityClient(semtkclient.SemTkClient):
   
    def __init__(self, serverURL):
        ''' serverURL string - e.g. http://machine:8099
        '''
        semtkclient.SemTkClient.__init__(self, serverURL, "utility")

    
    def exec_process_plot_spec(self, plotSpec, table):

        payload = {}
        payload["plotSpecJson"] = plotSpec.to_json_str();
        payload["tableJson"] = table.to_json_str();
        
        simple = self.post_to_simple("processPlotSpec", payload)
        plotJson = self.get_simple_field(simple, "plot")
        return plotspecs.PlotSpec(plotJson)
        

    def exec_load_ingestion_package(self, triple_store_url:str, triple_store_type:str, ingestion_package_path, clear:bool, default_model_graph:str, default_data_graph:str):
        '''
        Load an ingestion package
        :param triple_store_url: the URL e.g. "http://localhost:3030/DATASET"
        :param triple_store_type: "fuseki" "neptune" "virtuoso", etc.
        :param ingestion_package_path: path to an ingestion package (zip file) containing manifest and contents to load
        :param clear: if true, clears the footprint graphs (before loading)
        :param default_model_graph: model graph to use if not otherwise specified
        :param default_data_graph: data graph to use if not otherwise specified
        '''
        payload = {
            "serverAndPort":        triple_store_url,
            "serverType":           triple_store_type,
            "clear":                clear,
            "defaultModelGraph":    default_model_graph,
            "defaultDataGraph":     default_data_graph
        }
        files = {
            "file": (os.path.basename(ingestion_package_path), open(ingestion_package_path, 'rb'))
        }

        return self.post_to_stream("loadIngestionPackage", payload, files=files)