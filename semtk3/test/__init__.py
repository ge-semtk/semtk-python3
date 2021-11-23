#
# this file exists so that test_resources registers as a sub-module
# and semtk3 tests can use
#      importlib.resources.read_text("semtk3.test_resources", filename)
#
# Note the other two steps:
#    MANIFEST.in file has include pattern for this dir:  include semtk3/test/*
#    setup.py has: include_package_data=True,
#
# reminder that, after you pip install semtk3,
# this command will run tests:
#   python -m unittest discover semtk3
#
