import unittest
import importlib.resources
import os 
import shutil

import semtk3
import json
from semtk3 import STORE_ITEM_TYPE_REPORT, STORE_ITEM_TYPE_ALL, STORE_ITEM_TYPE_NODEGROUP
from semtk3 import sparqlconnection

class TestSemtk3(unittest.TestCase):

    PACKAGE = "semtk3.test"
    
    CONSTRUCT_ID = "semtk_test_animalCountByType"
    NO_RT_ID = "semtk_test_animalSubPropsCats"
    REPORT_ID = "semtk_test_animalTestReport"
    
    @classmethod
    def setUpClass(cls):
        '''
        Run once for class
        Exceptions here will stop the class' tests from being run
        '''
                
        # set up the default connection
        TestSemtk3.conn_str = importlib.resources.read_text(TestSemtk3.PACKAGE, "conn.json")
        TestSemtk3.conn2_str = importlib.resources.read_text(TestSemtk3.PACKAGE, "conn2.json")
        semtk3.set_connection_override(TestSemtk3.conn_str)
        
        # check semtk services
        all_ok = semtk3.check_services();
        if not all_ok: 
            raise Exception("Semtk services are not properly running on localhost")
        
        # check triplestore
        semtk3.check_connection_up(TestSemtk3.conn_str)
        
        
        
    def clear_graph(self): 
        # clear graph
        semtk3.clear_graph(TestSemtk3.conn_str, 'model', 0)
        semtk3.clear_graph(TestSemtk3.conn_str, 'data', 0)
        

    def load_cats_and_dogs(self):
        # load some owl
        with importlib.resources.path(TestSemtk3.PACKAGE, "AnimalSubProps.owl") as owl_path:
            semtk3.upload_owl(owl_path, TestSemtk3.conn_str)
            
        # store a nodegroups
        semtk3.delete_nodegroups_from_store("^semtk_test_")
        
        nodegroup_json_str =  importlib.resources.read_text(TestSemtk3.PACKAGE, "animalSubPropsCats.json")
        semtk3.store_nodegroup("semtk_test_animalSubPropsCats", "comments", "semtk python test", nodegroup_json_str)
        
        nodegroup_json_str =  importlib.resources.read_text(TestSemtk3.PACKAGE, "animalSubPropsDogs.json")
        semtk3.store_nodegroup("semtk_test_animalSubPropsDogs", "comments", "semtk python test", nodegroup_json_str)
        
        # ingest data
        csv_str =  importlib.resources.read_text(TestSemtk3.PACKAGE, "animalSubPropsCats.csv")
        semtk3.ingest_by_id("semtk_test_animalSubPropsCats", csv_str, TestSemtk3.conn_str)
        
        csv_str =  importlib.resources.read_text(TestSemtk3.PACKAGE, "animalSubPropsDogs.csv")
        semtk3.ingest_by_id("semtk_test_animalSubPropsDogs", csv_str, TestSemtk3.conn_str)
       
    def test_ingest_warnings(self):
        self.clear_graph()
         # load some owl
        with importlib.resources.path(TestSemtk3.PACKAGE, "AnimalSubProps.owl") as owl_path:
            semtk3.upload_owl(owl_path, TestSemtk3.conn_str)
            
        # load nodegroups
        nodegroup_json_str =  importlib.resources.read_text(TestSemtk3.PACKAGE, "animalSubPropsDogs.json")
        semtk3.store_nodegroup("semtk_test_animalSubPropsDogs", "comments", "semtk python test", nodegroup_json_str, True)
        nodegroup_json_str =  importlib.resources.read_text(TestSemtk3.PACKAGE, "animalSubPropsCats.json")
        semtk3.store_nodegroup("semtk_test_animalSubPropsCats", "comments", "semtk python test", nodegroup_json_str, True)
        
        # load data with no warnings
        csv_str =  importlib.resources.read_text(TestSemtk3.PACKAGE, "animalSubPropsCats.csv")
        statusMsg, warnMsg = semtk3.ingest_by_id("semtk_test_animalSubPropsCats", csv_str, TestSemtk3.conn_str)
        self.assertTrue(warnMsg == '', "Unexpected ingestion warning was generated\n" + warnMsg)
        
        # load data with extra columns
        csv_str =  importlib.resources.read_text(TestSemtk3.PACKAGE, "animalSubPropsDogs.csv")
        statusMsg, warnMsg = semtk3.ingest_by_id("semtk_test_animalSubPropsDogs", csv_str, TestSemtk3.conn_str)
        self.assertTrue(warnMsg != '', "No ingestion warning was generated for extra column\n" + warnMsg)
        
        # try template
        statusMsg, warnMsg = semtk3.ingest_using_class_template("http://AnimalSubProps#Cat", "name,Kitties_name,demons_name,child_name\nkitty,,,\nmom,kitty,,,\n", TestSemtk3.conn_str, "name")
        self.assertTrue(warnMsg == '', "Unexpected ingestion warning was generated:\n" + warnMsg)
        
        statusMsg, warnMsg = semtk3.ingest_using_class_template("http://AnimalSubProps#Cat", "name,Kitties_YYYY\nkitty,null\nmom,kitty\n", TestSemtk3.conn_str, "name")
        self.assertTrue(warnMsg != '', "No ingestion warning was generated for misspelled column\n" + warnMsg)
        self.assertTrue("kitties_yyyy" in warnMsg, "Warning did not contain name of extra column Kitties_YYYY:\n" + warnMsg)
        self.assertTrue("kitties_name" in warnMsg, "Warning did not contain name of missing column Kitties_name:\n" + warnMsg)
        
        
    def test_query_by_id(self):
        self.clear_graph()
        self.load_cats_and_dogs()
        
        # untyped query : should be table
        res = semtk3.query_by_id("semtk_test_animalSubPropsDogs")
        self.assertEqual(type(res), semtk3.semtktable.SemtkTable, "query_by_id() did not return a Table")
        self.assertEqual(res.get_num_rows(), 2, "query_by_id() returned wrong number of table rows")
        
        for t in ["SELECT_DISTINCT", "COUNT", "DELETE","CONSTRUCT"] :
            nodegroup_json_str =  importlib.resources.read_text(TestSemtk3.PACKAGE, "animalSubPropsDogsQueryType.json").replace("%QUERY_TYPE%", t)
            semtk3.store_nodegroup("semtk_test_animalSubPropsDogs_" + t, "comments", "semtk python test", nodegroup_json_str)
            
        # SELECT_DISTINCT : should be table
        res = semtk3.query_by_id("semtk_test_animalSubPropsDogs_SELECT_DISTINCT")
        self.assertEqual(type(res), semtk3.semtktable.SemtkTable, "query_by_id(semtk_test_animalSubPropsDogs_SELECT_DISTINCT) did not return a Table")
        self.assertEqual(res.get_num_rows(), 2, "query_by_id(semtk_test_animalSubPropsDogs_SELECT_DISTINCT) returned wrong number of table rows")
        
        # COUNT : should be table
        res = semtk3.query_by_id("semtk_test_animalSubPropsDogs_COUNT")
        self.assertEqual(type(res), semtk3.semtktable.SemtkTable, "query_by_id(semtk_test_animalSubPropsDogs_COUNT) did not return a Table")
        self.assertEqual(res.get_cell_as_int(0,0), 2, "query_by_id(semtk_test_animalSubPropsDogs_COUNT) returned incorrect count")
        
        # CONSTRUCT : should be json
        res = semtk3.query_by_id("semtk_test_animalSubPropsDogs_CONSTRUCT")
        self.assertEqual(type(res), dict, "query_by_id(semtk_test_animalSubPropsDogs_CONSTRUCT) did not return a dict")
        self.assertEqual(len(res['@graph']), 3, "query_by_id(semtk_test_animalSubPropsDogs_CONSTRUCT) returned incorrect dict")
        
        # Override CONSTRUCT with table
        res = semtk3.query_by_id("semtk_test_animalSubPropsDogs_CONSTRUCT", query_type=semtk3.QUERY_TYPE_SELECT_DISTINCT, result_type=semtk3.RESULT_TYPE_TABLE);
        self.assertEqual(type(res), semtk3.semtktable.SemtkTable, "query_by_id(semtk_test_animalSubPropsDogs_SELECT_DISTINCT) did not return a Table")
        self.assertEqual(res.get_num_rows(), 2, "query_by_id(semtk_test_animalSubPropsDogs_SELECT_DISTINCT) returned wrong number of table rows")
        
        ##### Repeat with empty to make sure we get empty results, not errors #####
        semtk3.clear_graph(TestSemtk3.conn_str, 'data', 0)
        
        # SELECT_DISTINCT : should be table
        res = semtk3.query_by_id("semtk_test_animalSubPropsDogs_SELECT_DISTINCT")
        self.assertEqual(type(res), semtk3.semtktable.SemtkTable, "query_by_id(semtk_test_animalSubPropsDogs_SELECT_DISTINCT) did not return a Table")
        self.assertEqual(res.get_num_rows(), 0, "query_by_id(semtk_test_animalSubPropsDogs_SELECT_DISTINCT) with no data did not return 0 rows")
        
        # COUNT : should be table
        res = semtk3.query_by_id("semtk_test_animalSubPropsDogs_COUNT")
        self.assertEqual(type(res), semtk3.semtktable.SemtkTable, "query_by_id(semtk_test_animalSubPropsDogs_COUNT) did not return a Table")
        self.assertEqual(res.get_cell_as_int(0,0), 0, "query_by_id(semtk_test_animalSubPropsDogs_COUNT) with no data did not return 0 count")
        
        # CONSTRUCT : should be json
        res = semtk3.query_by_id("semtk_test_animalSubPropsDogs_CONSTRUCT")
        self.assertEqual(type(res), dict, "query_by_id(semtk_test_animalSubPropsDogs_CONSTRUCT) did not return a dict")
        self.assertTrue(not '@graph' in res or len(res['@graph']) == 0, "query_by_id(semtk_test_animalSubPropsDogs_CONSTRUCT) with no data did not return empty dict")
        
        # Override CONSTRUCT with table
        res = semtk3.query_by_id("semtk_test_animalSubPropsDogs_CONSTRUCT", query_type=semtk3.QUERY_TYPE_SELECT_DISTINCT, result_type=semtk3.RESULT_TYPE_TABLE);
        self.assertEqual(type(res), semtk3.semtktable.SemtkTable, "query_by_id(semtk_test_animalSubPropsDogs_SELECT_DISTINCT) did not return a Table")
        self.assertEqual(res.get_num_rows(), 0, "query_by_id(semtk_test_animalSubPropsDogs, SELECT_DISTINCT) with no data did no return 0 rows")
        
        # TODO: all manor of combinations of query_type and result_type overrides could be tested here
        # TODO: DELETE query could be tested here
    
        
    def test_class_templates(self):
        
        # load AnimalSubProps owl
        self.clear_graph()
        with importlib.resources.path(TestSemtk3.PACKAGE, "AnimalSubProps.owl") as owl_path:
            semtk3.upload_owl(owl_path, TestSemtk3.conn_str)
          
        # retrieve the csv
        csv = semtk3.get_class_template_csv("http://AnimalSubProps#Cat", TestSemtk3.conn_str, "name"); 
        self.assertTrue("name" in csv)
        self.assertTrue("Kitties_name" in csv)
        self.assertTrue("Child_name" in csv)
        self.assertTrue("Demons_name" in csv)
        
        # retrieve the nodegroup
        ng = semtk3.get_class_template("http://AnimalSubProps#Cat", TestSemtk3.conn_str, "name");  
        
        # ingest
        (msg,warn) = semtk3.ingest_using_class_template("http://AnimalSubProps#Cat", "name,Kitties_name\nkitty,null\nmom,kitty\n", TestSemtk3.conn_str, "name")
        self.assertTrue(msg.find("2"))  # inserted 2 records
        
        # run the nodegroup as SELECT
        tab = semtk3.query_by_nodegroup(ng, query_type=semtk3.QUERY_TYPE_SELECT_DISTINCT, result_type=semtk3.RESULT_TYPE_TABLE)
        self.assertEquals(2, tab.get_num_rows(), "Incorrect number of rows")
        self.assertEquals(4, tab.get_num_columns(), "Incorrect number of columns")
        
        # ingest with a URI Lookup error.   Should throw an exception with error table.
        try:
            semtk3.ingest_using_class_template("http://AnimalSubProps#Cat", "name,Kitties_name\nkitty,lookupFailure\n", TestSemtk3.conn_str, "name")
            self.assertTrue(False, "Missing expected URILookup exception on 'lookupFailure'")
        except Exception as e: 
            self.assertTrue("lookupFailure" in str(e))

    def store_two_ng_one_report(self):
        # setup for store tests

        # Warning: this is an integration test with the local semtk store.  
        # Messing only with ids starting with "semtk_test" and leave the rest of the store undisturbed
        
        # delete items from the store
        semtk3.delete_items_from_store("^semtk_test", semtk3.STORE_ITEM_TYPE_ALL)
        
        # store a nodegroup with store_nodegroup()
        construct_ng = importlib.resources.read_text(TestSemtk3.PACKAGE, "animalCountByType.json")
        semtk3.store_nodegroup(self.CONSTRUCT_ID, "testing", "PyUnit", construct_ng)
        
        # store a nodegroup using store_item()
        no_rt_ng = importlib.resources.read_text(TestSemtk3.PACKAGE, "animalSubPropsCats.json")
        semtk3.store_item(self.NO_RT_ID, "testing", "PyUnit", no_rt_ng, STORE_ITEM_TYPE_NODEGROUP)
        
        # store a report
        report = importlib.resources.read_text(TestSemtk3.PACKAGE, "animalTestReport.json")
        semtk3.store_item(self.REPORT_ID, "testing", "PyUnit", report, STORE_ITEM_TYPE_REPORT)
        
    def test_runtime_constraints(self):
        self.load_cats_and_dogs()
        
        # get name and uri of a random cat
        nodegroup_json_str =  importlib.resources.read_text(TestSemtk3.PACKAGE, "animalSubPropsCatsConstrained.json")
        tab = semtk3.query_by_nodegroup(nodegroup_json_str)
        cat = tab.get_cell(0, "Cat")
        cat_name = tab.get_cell(0, "catName")
        
        constraint = semtk3.build_constraint("Cat", semtk3.OP_MATCHES, [cat] )
        tab = semtk3.query_by_nodegroup(nodegroup_json_str, runtime_constraints= [constraint])
        self.assertEqual(1, tab.get_num_rows(), "Runtime constraint URI query did not return expected row")
        
        constraint = semtk3.build_constraint("catName", semtk3.OP_MATCHES, [cat_name] )
        tab = semtk3.query_by_nodegroup(nodegroup_json_str, runtime_constraints= [constraint])
        self.assertEqual(1, tab.get_num_rows(), "Runtime constraint string query did not return expected row")
        
        
    def test_build_connection_str(self):
        
        
        # cheating, but pull things out of the current conn_str to demonstrate
        conn_dict = json.loads(TestSemtk3.conn_str)
        name = conn_dict["name"]
        triple_store_type = conn_dict["model"][0]["type"]
        triple_store_url = conn_dict["model"][0]["url"]
        model_graph_list = [sei["graph"] for sei in conn_dict["model"]]
        data_graph = conn_dict["data"][0]["graph"]
        data_graph_list = [sei["graph"] for sei in conn_dict["data"][1:]]
        
        # use values to test build_connection_str()
        new_conn_str = semtk3.build_connection_str(
            name,
            triple_store_type,
            triple_store_url,
            model_graph_list,
            data_graph,
            data_graph_list
            )
        
        # mess up current connection
        semtk3.set_connection_override(TestSemtk3.conn_str.replace("http:", "junk:"))
        
        # use the new build_connection() version
        semtk3.set_connection_override(new_conn_str)
        
        # make sure a basic query still works
        self.clear_graph()
        self.load_cats_and_dogs()
        res = semtk3.query_by_id("semtk_test_animalSubPropsDogs") 
        self.assertTrue("No rows returned from query with new connection", res.get_num_rows() > 0)
        
    def test_store(self):
        # test functions for directly storing and retrieving items
        
        #setup
        self.store_two_ng_one_report()
        
        # store again with overwrite error
        no_rt_ng = importlib.resources.read_text(TestSemtk3.PACKAGE, "animalSubPropsCats.json")
        try:
            semtk3.store_item(self.NO_RT_ID, "testing", "PyUnit", no_rt_ng, STORE_ITEM_TYPE_NODEGROUP)
            self.assertTrue(False, "Missing overwrite exception")
        except:
            pass
        
        # store again with overwrite_flag = True
        semtk3.store_item(self.NO_RT_ID, "testing", "PyUnit", no_rt_ng, STORE_ITEM_TYPE_NODEGROUP, overwrite_flag=True)
            
        
        # retrieve nodegroup data and count
        table = semtk3.get_nodegroup_store_data()
        self.assertEqual(2, len(table.get_matching_rows("ID", "^semtk_test")), "Wrong number of nodegroups returned from store")
        
        # retrieve report table and count
        table = semtk3.get_store_table(STORE_ITEM_TYPE_REPORT)
        self.assertEqual(1, len(table.get_matching_rows("ID", "^semtk_test")),  "Wrong number of reports returned from store")
        
        # retrieve both and count
        table = semtk3.get_store_table(STORE_ITEM_TYPE_ALL)
        self.assertEqual(3, len(table.get_matching_rows("ID", "^semtk_test")),  "Wrong number of reports returned from store")
        
        # retrieve nodegroup and run queries and check
        no_rt_ng_retrieved = semtk3.get_nodegroup_by_id(self.NO_RT_ID)
           
        t1 = semtk3.query_by_nodegroup(no_rt_ng_retrieved)
        t2 = semtk3.query_by_id(self.NO_RT_ID)
        self.assertEqual(t1.get_csv_string(), t2.get_csv_string(), "Running count query by id and by nodegroup returned different results")
        
        # retrieve report : running report in python is not implemented
        report_retrieved = semtk3.get_store_item(self.REPORT_ID, STORE_ITEM_TYPE_REPORT)
        self.assertTrue(report_retrieved and len(report_retrieved) > 0, "Retrieved report is empty")
    
    def test_store_to_from_disk(self):  
        # Test functions for storing to and from disk
        
        #setup
        self.store_two_ng_one_report()
         
        # set up a temp DIR
        DIR = "/tmp/semtk_pyunit"
        shutil.rmtree(DIR, ignore_errors=True)
        os.mkdir(DIR)
        
        # retrieve 2 nodegroups to disk
        semtk3.retrieve_nodegroups_from_store("^semtk_test", DIR)
        
        # delete with delete_nodegroup_from_store() and delete_item_from_store()
        semtk3.delete_nodegroup_from_store(self.NO_RT_ID)   
        semtk3.delete_item_from_store(self.CONSTRUCT_ID, STORE_ITEM_TYPE_NODEGROUP)  
        
        # verify deletion
        table = semtk3.get_store_table(STORE_ITEM_TYPE_NODEGROUP)
        self.assertEqual(0, len(table.get_matching_rows("ID", "^semtk_test")),  "Nodegroups were not deleted")
        
        # store multiple nodegroups from disk
        semtk3.store_nodegroups(DIR)
        
        # verify restoration
        table = semtk3.get_store_table(STORE_ITEM_TYPE_NODEGROUP)
        self.assertEqual(2, len(table.get_matching_rows("ID", "^semtk_test")),  "Nodegroups were not stored")
        
        # clear disk
        shutil.rmtree(DIR, ignore_errors=True)
        os.mkdir(DIR)
        
        # retrieve 3 mixed items: nodegroups + report
        semtk3.retrieve_items_from_store("^semtk_test", DIR)
        
        # delete with delete_nodegroup_from_store() and delete_item_from_store()
        semtk3.delete_nodegroup_from_store(self.NO_RT_ID)   
        semtk3.delete_item_from_store(self.CONSTRUCT_ID, STORE_ITEM_TYPE_NODEGROUP)  
        semtk3.delete_item_from_store(self.REPORT_ID, STORE_ITEM_TYPE_REPORT)  
        
        # verify deletion
        table = semtk3.get_store_table(STORE_ITEM_TYPE_NODEGROUP)
        self.assertEqual(0, len(table.get_matching_rows("ID", "^semtk_test")),  "Nodegroups and report were not deleted")
        
        # store multiple mixed items from disk
        semtk3.store_nodegroups(DIR)
        
        # verify restoration
        table = semtk3.get_store_table(STORE_ITEM_TYPE_ALL)
        self.assertEqual(3, len(table.get_matching_rows("ID", "^semtk_test")),  "Nodegroups were not stored")
    
        # clear disk
        shutil.rmtree(DIR, ignore_errors=True)
        os.mkdir(DIR)
        
        # retrieve one report that contains two nodegroups
        semtk3.retrieve_reports_from_store("^semtk_test", DIR)
        
        # delete with delete_nodegroup_from_store() and delete_item_from_store()
        semtk3.delete_nodegroup_from_store(self.NO_RT_ID)   
        semtk3.delete_item_from_store(self.CONSTRUCT_ID, STORE_ITEM_TYPE_NODEGROUP)  
        semtk3.delete_item_from_store(self.REPORT_ID, STORE_ITEM_TYPE_REPORT)  
    
        # store multiple mixed items from disk
        semtk3.store_nodegroups(DIR)
        
        # verify restoration
        table = semtk3.get_store_table(STORE_ITEM_TYPE_ALL)
        self.assertEqual(3, len(table.get_matching_rows("ID", "^semtk_test")),  "Report and 2 nodegroups were not stored")
    
    def test_store_errors(self):  
        # Test store error handling
        #setup
        self.store_two_ng_one_report()
         
        # set up a temp DIR
        DIR = "/tmp/semtk_pyunit"
        shutil.rmtree(DIR, ignore_errors=True)
        os.mkdir(DIR)
        
        # delete_nodegroup_from_store()
        try:
            # delete nonexistent nodegroup
            semtk3.delete_nodegroup_from_store("semtk_test_does_not_exist")
            self.assertTrue(False, "Missing expected error deleting non-existent nodegroup")
        except Exception as e: 
            self.assertTrue(" exists " in str(e))
        
        # verify nothing deleted
        table = semtk3.get_store_table(STORE_ITEM_TYPE_ALL)
        self.assertEqual(3, len(table.get_matching_rows("ID", "^semtk_test")),  "Items were unexpectedly deleted")
         
        # delete_item_from_store()
        try:
            # delete with wrong type
            semtk3.delete_item_from_store(self.CONSTRUCT_ID, STORE_ITEM_TYPE_REPORT)
            self.assertTrue(False, "Missing expected error deleting item with wrong type")
        except Exception as e: 
            self.assertTrue(" exists " in str(e))
        
        # verify nothing deleted
        table = semtk3.get_store_table(STORE_ITEM_TYPE_ALL)
        self.assertEqual(3, len(table.get_matching_rows("ID", "^semtk_test")),  "Items were unexpectedly deleted")
        
        # delete_items_from_store()  with wrong type: no error. no action
        semtk3.delete_items_from_store(self.CONSTRUCT_ID, STORE_ITEM_TYPE_REPORT)
        table = semtk3.get_store_table(STORE_ITEM_TYPE_ALL)
        self.assertEqual(3, len(table.get_matching_rows("ID", "^semtk_test")),  "Items were unexpectedly deleted")
        
        # delete_items_from_store() try a bad regex: no error. no action
        semtk3.delete_items_from_store("^semtk_test_nothing", STORE_ITEM_TYPE_ALL)
        table = semtk3.get_store_table(STORE_ITEM_TYPE_ALL)
        self.assertEqual(3, len(table.get_matching_rows("ID", "^semtk_test")),  "Items were unexpectedly deleted")
         
        # retrieve_items_from_store() with bad regex
        semtk3.retrieve_items_from_store("^semtk_test_nothing", DIR, STORE_ITEM_TYPE_ALL)
        files = os.listdir(DIR);
        self.assertTrue(len(files) == 1 and files[0]=="store_data.csv", "Retrieval did not retrieve nothing as expected")
        with open(DIR + "/store_data.csv", "r") as f:
            self.assertEquals(len(f.readlines()), 1, "store_data.csv is not empty as expected")
                              
        # get_store_item() with wrong type
        try:
            # delete with wrong type
            semtk3.get_store_item(self.CONSTRUCT_ID, STORE_ITEM_TYPE_REPORT)
            self.assertTrue(False, "Missing expected error getting item with wrong type")
        except Exception as e: 
            self.assertTrue(" not find " in str(e))
            
        # get_store_item() with wrong name
        try:
            # delete with wrong type
            semtk3.get_store_item("^semtk_test_nothing", STORE_ITEM_TYPE_REPORT)
            self.assertTrue(False, "Missing expected error getting item with wrong type")
        except Exception as e: 
            self.assertTrue(" not find " in str(e))
          
    def test_combine_entities(self):
        # Basic test of REST endpoint
        # Real testing is don
        self.clear_graph()
        
        with importlib.resources.path(TestSemtk3.PACKAGE, "AnimalSubProps.owl") as owl_path:
            semtk3.upload_owl(owl_path, TestSemtk3.conn_str)
        
        with importlib.resources.path(TestSemtk3.PACKAGE, "AnimalsToCombineData.owl") as owl_path:
            semtk3.upload_owl(owl_path, TestSemtk3.conn_str, model_or_data="data")
        
        semtk3.combine_entities("http://AnimalsToCombineData#auntyEm", "http://AnimalsToCombineData#auntyEmDuplicate", None, ["http://AnimalSubProps#name"])
        
        nodegroup_json_str =  importlib.resources.read_text(TestSemtk3.PACKAGE, "animalsToCombineTigerTree.json")
        semtk3.store_nodegroup("semtk_test_animalToCombineTigerTree", "comments", "semtk python test", nodegroup_json_str, True)
        
        tab = semtk3.select_by_id("semtk_test_animalToCombineTigerTree")
        self.assertEqual(tab.get_num_rows(), 16, "Wrong number of rows returned")
        self.assertTrue("AUNTY_EM" not in tab.get_csv_string(), "Duplicate name was not removed")
    
    def test_get_oinfo(self):
        oinfo = semtk3.get_oinfo()  
        class_list = oinfo.get_class_list()
        self.assertEqual(len(class_list), 4, "Wrong number of classes returned")
        for c in ['http://AnimalSubProps#Animal', 'http://AnimalSubProps#Cat', 'http://AnimalSubProps#Dog', 'http://AnimalSubProps#Tiger']:
            self.assertTrue(c in class_list, "Did not find class " + c )
         
    def test_classes_and_templates(self):
        # get all classes
        classes = semtk3.get_class_names()
        
        # should be 4, with Animal in position [0]
        self.assertEqual(len(classes), 4)   
        self.assertEqual(classes[0], 'http://AnimalSubProps#Animal')
        
        # get Animal's template information
        (ng, col_names, col_types) = semtk3.get_class_template_and_csv(classes[0], id_regex="name")
        
        # make sure nodegroup runs without error
        semtk3.query_by_nodegroup(ng)
        
        # column names are comma separated with \n on the end.  split into a list
        col_name_list = col_names.strip().split(",")
        self.assertEqual(col_name_list[0], "name")
        self.assertEqual(col_name_list[1], "Child_name")
        
        # column types are comma separated with \n on the end. split into a list
        col_type_list = col_types.strip().split(",")
        # check that first column type is a simple type.  If it weren't you might need to expect "string"
        self.assertEqual(len(col_type_list[0].split(" ")), 1, "First column's type is complex, expected 'string' " + col_type_list[0]);
        self.assertEqual(col_type_list[0], "string")
        
    def test_download_and_upload(self):
        #
        # Warning: this tests valid download and upload capabilities using smaller graphs
        #          but copying a graph this way is more prone to timeouts and performance issues.
        #
        # To see the best way to copy a graph, see 
        #           test_copy_graph()
  
        # load a fresh graph
        self.clear_graph()
        self.load_cats_and_dogs()
        
        # dump the DATA graph
        semtk3.download_owl("/tmp/whatever.owl", TestSemtk3.conn_str, model_or_data=semtk3.SEMTK3_CONN_DATA, conn_index = 0)   
        
        # clear and upload to DATA-COPY graph
        conn2_str = TestSemtk3.conn_str.replace("http://semtk-python-test/data", "http://semtk-python-test/data-copy")
        semtk3.clear_graph(conn2_str, model_or_data=semtk3.SEMTK3_CONN_DATA, index=0)
        semtk3.upload_owl("/tmp/whatever.owl", conn2_str, model_or_data=semtk3.SEMTK3_CONN_DATA, conn_index=0)
        
        # query the new graph
        semtk3.set_connection_override(conn2_str)
        res = semtk3.query_by_id("semtk_test_animalSubPropsDogs")
        self.assertEqual(type(res), semtk3.semtktable.SemtkTable, "query_by_id() on copied graph did not return a Table")
        self.assertEqual(res.get_num_rows(), 2, "query_by_id() on copied graph returned wrong number of table rows")
        
        # reset the default graph for other tests
        semtk3.set_connection_override(TestSemtk3.conn_str)
        
    def test_get_instance_dict(self):
        # load a fresh graph
        self.clear_graph()
        self.load_cats_and_dogs()
        
        tab = semtk3.get_instance_dictionary(max_words=2, specificity_limit=2)
        self.assertEqual(10, tab.get_num_rows(), "instance dict table has wrong number of rows")
        
    def test_sparqlgraph_url(self):
        self.clear_graph()
        self.load_cats_and_dogs()
        
        # load the extra constraints nodegroup
        nodegroup_json_str =  importlib.resources.read_text(TestSemtk3.PACKAGE, "animalSubPropsConstructConstrained.json")
        CONSTRAINT_NG = "semtk_test_animalSubPropsConstructConstrained"
        semtk3.store_nodegroup(CONSTRAINT_NG, "comments", "semtk python test", nodegroup_json_str)
        
        # generate some urls.
        # you'll have to put them into a browser for full testing
        
        # plain URL
        print(semtk3.get_sparqlgraph_url("http://localhost:8080"))
        
        # override connection
        print(semtk3.get_sparqlgraph_url("http://localhost:8080", conn_json_str=TestSemtk3.conn_str))
        
        # default connection
        print(semtk3.get_sparqlgraph_url("http://localhost:8080", conn_json_str=semtk3.build_default_connection_str("default", "fuseki", "http://localhost:3030/RACK")))

        # run a nodegroup with override
        print(semtk3.get_sparqlgraph_url("http://localhost:8080", conn_json_str=TestSemtk3.conn_str, nodegroup_id="semtk_test_animalSubPropsCats"))
        
        # load a nodegroup with override, but don't run it
        print(semtk3.get_sparqlgraph_url("http://localhost:8080", conn_json_str=TestSemtk3.conn_str, nodegroup_id="semtk_test_animalSubPropsCats", run_flag="False"))
        
        # run a (construct) query with two constraints
        rt_constraints=[
            semtk3.build_constraint("catName", semtk3.OP_MATCHES, ["fluffymom"] ),
            semtk3.build_constraint("demonName", semtk3.OP_MATCHES, ["beelz"]   )
            ]
        print(semtk3.get_sparqlgraph_url("http://localhost:8080", conn_json_str=TestSemtk3.conn_str, nodegroup_id=CONSTRAINT_NG, runtime_constraints=rt_constraints))

        # run a report
        print(semtk3.get_sparqlgraph_url("http://localhost:8080", conn_json_str=TestSemtk3.conn_str, report_id="report data verification"))
        
    def test_copy_graph(self):
        # set up from_graph
        self.clear_graph()
        self.load_cats_and_dogs()
        
        # clear to_graph
        semtk3.clear_graph(TestSemtk3.conn2_str, "data", 0)
        
        # perform the copy
        from_graph = sparqlconnection.SparqlConnection(TestSemtk3.conn_str).get_graph("data", 0)
        to_graph = sparqlconnection.SparqlConnection(TestSemtk3.conn2_str).get_graph("data", 0)
        status = semtk3.copy_graph(from_graph, to_graph)
        print(status)
        
        # test the results
        tab = semtk3.query("select ?x ?y ?z from <" + to_graph +"> where { ?x ?y ?z }", TestSemtk3.conn2_str)
        self.assertEqual(tab.get_num_rows(), 26, "Unexpected number of rows were copied")
        

        
        
if __name__ == '__main__':
    unittest.main()