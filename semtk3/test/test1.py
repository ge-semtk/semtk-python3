import unittest
import importlib.resources
import semtk3
import requests
import json

class TestSemtk3(unittest.TestCase):

    PACKAGE = "semtk3.test"
    
    @classmethod
    def setUpClass(cls):
        '''
        Run once for class
        Exceptions here will stop the class' tests from being run
        '''
                
        # set up the default connection
        TestSemtk3.conn_str = importlib.resources.read_text(TestSemtk3.PACKAGE, "conn.json")
        semtk3.SEMTK3_CONN_OVERRIDE = TestSemtk3.conn_str
        
        # check services
        all_ok = semtk3.check_services();
        if not all_ok: 
            raise Exception("Semtk services are not properly running on localhost")
        
        # check triplestore
        triplestore = json.loads(TestSemtk3.conn_str)['model'][0]['url']
        response = requests.request("GET", triplestore)
        if not response.ok:
            raise Exception("Problem connecting to triplestore url: " + triplestore + '\n' + str(response.content))
        
        
        
    def clear_graph(self): 
        # clear graph
        semtk3.clear_graph(TestSemtk3.conn_str, 'model', 0)
        semtk3.clear_graph(TestSemtk3.conn_str, 'data', 0)
        

    def load_cats_and_dogs(self):
        # load some owl
        with importlib.resources.path(TestSemtk3.PACKAGE, "AnimalSubProps.owl") as owl_path:
            semtk3.upload_owl(owl_path, TestSemtk3.conn_str)
            
        # store a nodegroups
        semtk3.delete_nodegroups_from_store("semtk_test_*")
        
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
        
        # ingest
        msg = semtk3.ingest_using_class_template("http://AnimalSubProps#Cat", "name,Kitties_name\nkitty,lookupFailure\n", TestSemtk3.conn_str, "name")
        self.assertTrue(msg and "2" in msg)  # inserted 2 records


if __name__ == '__main__':
    unittest.main()