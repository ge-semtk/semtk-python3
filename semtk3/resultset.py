class ResultSet():
    def __init__(self):
        self.dict = {}
        self.set_status(True)
        self.set_message("")
        
        
    def set_status(self, status_bool):
        self.dict["status"] = "success" if status_bool else "failure"
        
    def set_message(self, msg):
        self.dict["message"] = msg
        
    def set_rationale(self, rat):
        self.dict["rationale"] = rat
        
    def set_table(self, table):
        self.dict["table"] = { "@table": table.to_dict()}
        
    def set_json_field(self, field_name, json_val):
        
        if not ("simpleresults" in self.dict):
            self.dict["simpleresults"] = {}
            
        self.dict["simpleresults"][field_name] = json_val
        
    def to_dict(self):
        
        return self.dict;