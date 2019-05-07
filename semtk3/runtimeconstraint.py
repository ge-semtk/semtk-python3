import json

class RuntimeConstraint():
    OP_MATCHES = "MATCHES"
    
    def __init__(self, sparqlId, operator, operand_list):
        ''' 
            Create a SemtkTable from the @table results from a Semtk REST service
        '''
        self.sparqlId = sparqlId
        self.operator = operator
        self.operand_list = operand_list
    
    def to_json(self):
        
        jdict = {}
        jdict["SparqlID"] = self.sparqlId
        jdict["Operator"] = self.operator
        jdict["Operands"] = self.operand_list
        
        return json.dumps(jdict)