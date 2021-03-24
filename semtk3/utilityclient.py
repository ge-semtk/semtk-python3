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

class UtilityClient(semtkclient.SemTkClient):
   
    def __init__(self, serverURL):
        ''' serverURL string - e.g. http://machine:8099
        '''
        semtkclient.SemTkClient.__init__(self, serverURL, "utility")

    
    def exec_process_plot_spec(self, plotSpecJson, tableJson):

        payload = {}
        payload["plotSpecJson"] = plotSpecJson;
        payload["tableJson"] = tableJson;
        
        return self.post_to_simple("processPlotSpec", payload)
        
            
    