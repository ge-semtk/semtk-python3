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
import json

#
# Encapsulate a JSON array containing a set of plot specifications
#
class PlotsHandler:
    
    def __init__(self, json_arr):
        self.json_arr = json_arr
        
    def get_num_plots(self):
        return len(self.json_arr)

    def get_plot(self, index):
        return PlotSpecHandler(self.json_arr[index])
    
    def get_plot_names(self):
        names = []
        for o in self.json_arr:
            names.append(PlotSpecHandler(o).get_name())
        return names
        
#
# Encapsulate a JSON object containing a plot spec (name, type, spec)
#
class PlotSpecHandler:
    
    def __init__(self, json):
        self.json = json
    
    def to_json_str(self):
        return json.dumps(self.json)   
        
    def get_name(self):
        return self.json["name"]    
        
    def get_type(self):
        return self.json["type"]
        
    def get_spec(self):
        return self.json["spec"]
        
    