import semtk3

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
    # ingest
    #
    
    # pec here
    # large async
    #semtk3.ingest_by_id(nodegroup_id, csv_str, override_conn_json)