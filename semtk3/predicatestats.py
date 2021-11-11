import json
from . import semtktable

class PredicateStats():
    
    def __init__(self, stats_json):
        ''' 
            Create a SemtkTable from the @table results from a Semtk REST service
        '''
        self.json = stats_json
        
    #
    # Return a dictionary[class]=count
    #
    def get_class_count(self):
        ''' see java or javascript, but:
               exactTab keys are stringified json OntologyPaths
               exactTab values are counts
        '''
        ret = {}
        exactTab = self.json["exactTab"]
        for k in exactTab:
            key_json = json.loads(k)
            triples = key_json["triples"]
            if len(triples) == 1 and triples[0]["p"].endswith("type"):
                ret[triples[0]["s"]] = exactTab[k]
            
        return ret
    
    def get_class_count_table(self):
         class_dict = self.get_class_count()
         count_arr = [[k,v] for k, v in sorted(class_dict.items(), key=lambda item: item[1])]
         count_arr.reverse()
         table_dict = semtktable.SemtkTable.create_table_dict(["class", "count"], ["string", "int"], count_arr)
         table = semtktable.SemtkTable(table_dict)

         return table