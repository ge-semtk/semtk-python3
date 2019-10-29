import semtk3
from semtk3 import SEMTK3_CONN_MODEL

#
# Not done yet
#   - override connections
#   - date runtime constraints
#   - range runtime constraints
#   - ingest
#

if __name__ == '__main__':
    semtk3.set_host("http://localhost")
    
    nodegroup_id="Lighting Weekly Units Sold"
    
    # 
    # get a table of constraints for a nodegroup_id
    #
    table = semtk3.get_constraints_by_id(nodegroup_id)
    print(table.get_rows())
    
    #
    # get all existings values for the constraint "?pc"
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
    
    # upload owl
    #OWL_PATH = r'C:\Users\200001934\workspace-kepler\Lighting\OwlModels\Lighting.owl'
    #semtk3.upload_owl(OWL_PATH, TEST_CONN_STR, "dba", "dba")
    
    # sample get oinfo table
    table = semtk3.get_oinfo_uri_label_table(TEST_CONN_STR)
    print(table.get_csv_string())
    
