import json
import sys
from . import restclient
from . import semtktable

# 
# SETUP NOTES
#    - Inside GE, will fail with Captcha if Windows has HTTP_PROXY and maybe HTTPS_PROXY environment variables set
#           unless NO_PROXY is set up on your endpoint
#



class SemTkClient(restclient.RestClient):
    
    def __check_status(self, content):
        ''' check content is a dict, has status=="success"
        '''
        if not isinstance(content, dict):
            self.raise_exception("Can't process content from rest service")
        
        if "status" not in content.keys():
            self.raise_exception("Can't find status in content from rest service")
        
        if content["status"] != "success":
            self.raise_exception("Rest service call did not succeed ")

    def __check_simple(self, content):
        ''' perform all checks on content through checking for simpleresults 
        '''
        self.__check_status(content)
        
        if "simpleresults" not in content.keys():
            self.raise_exception("Rest service did not return simpleresults")
            
    def __check_table(self, content):
        ''' perform all checks on content through checking for table 
        '''
        self.__check_status(content)
        
        if  "table" not in content.keys():
            self.raise_exception("Rest service did not return table")
        
        if "@table" not in content["table"].keys():
            self.raise_exception("Rest service table does not contain @table")
    
    def __check_record_process(self, content):
        ''' perform all checks on content through checking for table 
        '''
        try:
            self.__check_status(content)
        except Exception as e:
            print >> sys.stderr, content["recordProcessResults"]["errorTable"]
            raise e
            
        
        if  "recordProcessResults" not in content.keys():
            self.raise_exception("Rest service did not record process results")
    
    
    def get_simple_field(self, simple_res, field):
        ''' get a simple field with REST error handling
        '''
        if field not in simple_res.keys():
            self.raise_exception("Rest service did not return simple result " + field)
        
        return simple_res[field]
    
    def get_simple_field_int(self, simple_res, field):
        ''' get integer simple results field
            returns int
            raises RestException on type or missing field
        '''
        try:
            f = self.get_simple_field(simple_res, field)
            return int(f)
        
        except ValueError:
            self.raise_exception("Simple results field " + field + " expecting integer, found " + f)
            
    def get_simple_field_str(self, simple_res, field):
        ''' get string from simple result
            returns string
            raises RestException on type or missing field
        '''
        f = self.get_simple_field(simple_res, field)
        return str(f)
    
    
        
    def post_to_simple(self, endpoint, dataObj={}):
        ''' 
            returns dict - the simple results
                           which can be used as a regular dict, or with error-handling get_simple_field*() methods
        '''
        content_str = self.post(endpoint, dataObj)
        content = json.loads(content_str)
        self.__check_simple(content)
        return content["simpleresults"]
    
    def post_to_table(self, endpoint, dataObj={}):
        ''' 
            returns dict - the table 
            raises RestException
        '''
        content_str = self.post(endpoint, dataObj)
        content = json.loads(content_str)

        self.__check_table(content)
        
        table = semtktable.SemtkTable(content["table"]["@table"])
        return table
    
    def post_to_record_process(self, endpoint, dataObj={}):
        ''' 
            returns dict - the table 
            raises RestException
        '''
        content_str = self.post(endpoint, dataObj)
        content = json.loads(content_str)

        self.__check_record_process(content)
        
        record_process = content["recordProcessResults"]
        return record_process
        
        
    