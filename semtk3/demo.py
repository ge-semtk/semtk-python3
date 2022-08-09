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
import semtk3
import logging

#
# This file is deprecated in favor of the test*.py files in the sub-package "test"
#
# It still has some older useful examples, so it remains.
#

if __name__ == '__main__':
    semtk3.set_host("http://localhost")
    
    # res = semtk3.query_by_id("test_select")
    # type(res) is semtk3.semtktable.SemtkTable
    #
    # res = semtk3.query_by_id("test_construct")
    # type(res) is dict
    #
    # res = semtk3.query_by_id("test_count")
    # res is table with one col "count" and one cell
    #
    # res = semtk3.query_by_id("tes_delete")
    # res is table with one col "@message" and one cell
    
    #semtk3.print_wait_dots(3)
    table = semtk3.select_by_id("DeleteMe")
    
    # Sample pandas code (without adding the dependency)
    #
    # import pandas as pd
    # df = pd.DataFrame(table.get_pandas_data())
    
    print(table.get_rows())
    
    #
    # Column manipulation
    #
    col = "identifier"
    if table.has_column(col):
        c = table.get_column_index(col)
        col_data = table.get_column(c)
        print(col+": ")
        print(col_data)
    else:
        print(col + " doesn't exist in this table")

    #
    # Plotting data (if plotly package is available)
    #

    try:
        import plotly
        nodegroup_id = "DeleteMe"
        plot_name = "hist"
        table = select_by_id(nodegroup_id) # get results table
        nodegroupStr = get_nodegroup_by_id(nodegroup_id) # get nodegroup string
        sg_json = sparqlgraphjson.SparqlGraphJson(json.loads(nodegroupStr))
        plot_spec = sg_json.get_plot_specs().get_plot_spec_by_name(plot_name)
        # populate plot spec with data
        plot_spec_processed = __get_utility_client().exec_process_plot_spec(plot_spec, table)
        # generate and show plotted data
        import plotly
        plot = plotly.graph_objects.Figure(plot_spec_processed.get_spec())
        plot.show()
    except ImportError:
        print ('Install plotly to view the data plotting demo')
     
    #
    # configure logging:   http://docs.python.org/3/howto/logging.html
    # using:               https://docs.python.org/3/howto/logging-cookbook.html
    #
  
    ### LOGGING ####
    # set up logging globally
    logging.basicConfig(filename=None, level=logging.ERROR);
  
    # set semtk3 logging to DEBUG   (below we switch it to the more typical logging.INFO)
    semtk3.get_logger().setLevel(level=logging.INFO)
  
    semtk3.set_headers({ "extra" : "idk"})
    if not semtk3.check_services():
        print("A service failed.  Check logs")
  
    # don't want to demo mucking with nodegroup store.  Also need valid local folder path.
    #semtk3.retrieve_from_store("regex", "folder path")
    #semtk3.delete_nodegroup_from_store("nodegroup_id")
    #semtk3.store_nodegroups("folder path")
  
    #
    # get a list of nodegroups
    #
    table = semtk3.get_nodegroup_store_data()
    print(table.get_rows())
  
    #
    # get a table of constraints for a nodegroup_id
    #
    nodegroup_id="Lighting Weekly Units Sold"
    table = semtk3.get_constraints_by_id(nodegroup_id)
    print(table.get_rows())
  
    #
    # logging
    # show only high-level logging (e.g. percent complete)
    #
    semtk3.get_logger().setLevel(level=logging.INFO)
  
    #
    # get all existing values for the constraint "?pc"
    #
    table = semtk3.get_filter_values_by_id(nodegroup_id, "?pc")
    print(table.get_rows())
  
    #
    # build a constraint of the first two legal values and run it
    #
    rows = table.get_rows()
    list2 = [rows[0][0], rows[1][0]]
    constraint = semtk3.build_constraint("?pc", semtk3.OP_MATCHES, list2)
    table = semtk3.select_by_id(nodegroup_id, runtime_constraints=[constraint])
    print(table.get_rows())
      
    #
    # build a constraint of the first two legal values and run it
    #
    constraint = semtk3.build_constraint("?pc", semtk3.OP_GREATERTHAN, [1000])
    table = semtk3.select_by_id(nodegroup_id, runtime_constraints=[constraint])
    print(table.get_rows())
  
    #
    # run the whole query
    #
    table = semtk3.select_by_id(nodegroup_id)
    print(table.get_rows())
  
    #
    # ingest async with override
    #
    TEST_CONN_STR = '''{
        "name":"daDemo v-test",
        "domain":"",
        "enableOwlImports":true,
        "model":[{
            "type":"virtuoso",
            "url":"http://vesuvius-test.crd.ge.com:2420",
            "graph":"http://paultest/model"
        }],
        "data":[{
            "type":"virtuoso",
            "url":"http://vesuvius-test.crd.ge.com:2420",
            "graph":"http://paultest/data"
        }]
    }'''
  
    # this type of import cleans up code further:
    from semtk3.sparqlconnection import SparqlConnection
  
    semtk3.clear_graph(TEST_CONN_STR, SparqlConnection.MODEL, 0)
  
    # upload owl
    #OWL_PATH = r'C:\Users\200001934\workspace-kepler\Lighting\OwlModels\Lighting.owl'
    #semtk3.upload_owl(OWL_PATH, TEST_CONN_STR, "dba", "dba")
  
    # sample get oinfo table
    table = semtk3.get_oinfo_uri_label_table(TEST_CONN_STR)
    print(table.get_csv_string())
  
  
    #
    # Execute query against SemTK HiveService
    #
    # import semtk3
    # import numpy as np
    #
    # semtk3.set_host("http://semtk-service-host")
    #
    # hive_query=""
    # table=semtk3.query_hive("hive-server-host.crd.ge.com", "10000", "databasename", hive_query)
    #
    # convert to numpy array
    # npa = np.array(table.get_rows())
