import unittest
import importlib.resources
import os 
import shutil

import semtk3

from semtk3 import STORE_ITEM_TYPE_REPORT, STORE_ITEM_TYPE_ALL, STORE_ITEM_TYPE_NODEGROUP

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
        msg = semtk3.ingest_using_class_template("http://AnimalSubProps#Cat", "name,Kitties_name\nkitty,null\nmom,kitty\n", TestSemtk3.conn_str, "name")
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
        
    def test_store(self):
        # test functions for directly storing and retrieving items
        
        #setup
        self.store_two_ng_one_report()
        
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
          
        
if __name__ == '__main__':
    unittest.main()