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

#
# based on the true story of java: com.ge.research.semtk.resultSet.Table
#

import csv
import io
import json
from dateutil import parser      # python-dateutil and six
import re

JSON_KEY_COL_NAMES = "col_names"
JSON_KEY_COL_TYPES = "col_type"
JSON_KEY_ROWS = "rows"
JSON_KEY_ROW_COUNT = "row_count"
JSON_KEY_COL_COUNT = "col_count"
JSON_TYPE = "type"

class SemtkTable():
    
    def __init__(self, table_dict):
        ''' 
            Create a SemtkTable from the @table results from a Semtk REST service
        '''
        self.dict = table_dict
        
    # create a table with no checking so far
    #
    # use it like this:   
    #
    #     from semtk3 import semtktable
    #     table = semtktable(SemtkTable.create_table_dict(["col1", "col2"], ["string", "string"], [["a1", "a2"]["b1","b2]])
    #
    @staticmethod
    def create_table_dict(col_names, col_types, rows):
        d = {}
        d[JSON_KEY_ROW_COUNT] = len(rows)
        d[JSON_KEY_COL_COUNT] = len(col_names)
        d[JSON_KEY_COL_NAMES] = col_names
        d[JSON_KEY_COL_TYPES] = col_types
        d[JSON_KEY_ROWS] = rows
        return d
      
    def delete_column(self, col_name):
        i = self.get_column_index(col_name)
        self.dict[JSON_KEY_COL_COUNT] -= 1
        
        del self.dict[JSON_KEY_COL_NAMES][i]
        del self.dict[JSON_KEY_COL_TYPES][i]
        for r in range(self.get_num_rows()):
            del self.dict[JSON_KEY_ROWS][r][i] 
            
    def get_num_rows(self):
        return self.dict[JSON_KEY_ROW_COUNT]
    
    def get_num_columns(self):
        return self.dict[JSON_KEY_COL_COUNT]
    
    def get_pandas_data(self):
        data = {}
        for c in range(self.get_num_columns()):
            row = [];
            for r in range(self.get_num_rows()):
                row.append(self.get_cell_typed(r,c))
            data[self.get_column_names()[c]] = row
        
        return data
        
    def get_column_names(self):
        return self.dict[JSON_KEY_COL_NAMES]
    
    def get_column_types(self):
        return self.dict[JSON_KEY_COL_TYPES]
    
    def get_column_type(self, col_name):
        ''' 
            raises ValueError on bad col_name
        '''
        return self.get_column_types() [ self.get_column_index(col_name) ]

    def get_column_index(self, col_name):
        ''' get column index
            raises ValueError
        '''
        return self.get_column_names().index(col_name)
    
    def has_column(self, col_name):
        try:
            self.get_column_index(col_name)
            return True
        except ValueError:
            return False
    
    def get_column(self, col):
        ret = []
        c = self.get_column_index(col) if isinstance(col, str) else col
        for row in self.get_rows():
            ret.append(row[c])
        return ret     
         
    def set_cell(self, row, col, val):
        c = self.get_column_index(col) if isinstance(col, str) else col
        self.dict[JSON_KEY_ROWS][row][c] = str(val)
    
    def get_cell(self, row, col):
        c = self.get_column_index(col) if isinstance(col, str) else col
        return self.dict[JSON_KEY_ROWS][row][c]
    
    def get_cell_as_string(self, row, col):
        return self.get_cell(row, col)
    
    def get_cell_as_int(self, row, col):
        cell_str = self.get_cell(row, col)
        if (cell_str == ""):
            return None
        else:
            return int(cell_str)
    
    def get_cell_as_float(self, row, col):
        cell_str = self.get_cell(row, col)
        if (cell_str == ""):
            return None
        else:
            return float(cell_str)
    
    def get_cell_as_date(self, row, col):
        cell_str = self.get_cell(row, col)
        if (cell_str == ""):
            return None
        else:
            return parser.parse(cell_str)
    
    def get_cell_typed(self, row, col):
        ''' PEC TODO
            Full types list :    especially Time
            (see list in ImportSpecHandler.java)
        '''
        cname = self.get_column_types()[col]

        if cname.endswith("XMLSchema#int"):
            return self.get_cell_as_int(row, col)
        
        elif cname.endswith("XMLSchema#float") or cname.endswith("XMLSchema#double"):
            return self.get_cell_as_float(row, col)
        
        elif cname.endswith("XMLSchema#date") or cname.endswith("XMLSchema#dateTime"):
            return self.get_cell_as_date(row, col)
        
        else:
            return self.get_cell_as_string(row, col)
        
    def get_csv_string(self):
    
        si = io.StringIO()
        cw = csv.writer(si)
        
        cw.writerow(self.get_column_names())
        
        for row in range(self.get_num_rows()):
            cw.writerow(self.dict[JSON_KEY_ROWS][row])

        return re.sub("[\r\n]+", "\n", si.getvalue())  # ghetto improper use of io.StringIO
    
    def get_rows(self):
        ''' returns array of arrays '''
        ret = []
        for r in range(self.get_num_rows()): 
            row = []
            for c in range(self.get_num_columns()):
                row.append(self.get_cell_typed(r, c))
                 
            ret.append(row)
                  
        return ret
    
    def get_matching_rows(self, col_name, regex_str):
        # get store data
        regex = re.compile(regex_str)
        ret = []
        for r in range(self.get_num_rows()):
            if (regex.search(self.get_cell(r,col_name))):
                row = []
                for c in range(self.get_num_columns()):
                    row.append(self.get_cell_typed(r, c))
                ret.append(row)
                
        return ret
    
    def get_matching_row_nums(self, col_name, regex_str):
        # get store data
        regex = re.compile(regex_str)
        ret = []
        for r in range(self.get_num_rows()):
            if (regex.search(self.get_cell(r,col_name))):
                row = []
                for c in range(self.get_num_columns()):
                    row.append(self.get_cell_typed(r, c))
                ret.append(r)
                
        return ret
                
    def to_dict(self):
        return self.dict
    
    def to_json_str(self):
        return json.dumps(self.dict, indent=4, sort_keys=True)
