# semtk-python3
This package contains python3 clients to the SemTK services.
Tested on Python 3.7.5.  May be incompatible with Python 3.7.6+

## Build whl file
```
pip install wheel
python3 setup.py sdist bdist_wheel
```
Alternatively, use the wheel file provided in `semtk-python3/dist/`

## Install whl file
```
python -m pip install /path/to/whl/file
```
## Sample code 

To execute a query via a stored nodegroup:
```
```

To execute a query against the SemTK Hive Service:
```
import semtk3
import numpy as np

semtk3.set_host("http://semtk-service-host")

hive_query=""
table=semtk3.query_hive("hive-server-host.crd.ge.com", "10000", "databasename", hive_query)
   
# convert to numpy array
npa = np.array(table.get_rows())
```
