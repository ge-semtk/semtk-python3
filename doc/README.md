# Documentation

API Documentation is auto-generated and available online at:
[readthedocs.io](https://semtk-python3.readthedocs.io/en/latest/api/semtk3.html).
The API documentation for semtk-python3 is build with
[Sphinx](https://www.sphinx-doc.org/en/master/index.html).

## Building the Documentation Locally

To build a local copy of the documentation,
```
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
mkdir _build
sphinx-build -b html . _build
```
(Note: on Windows, `venv/bin/activate` should instead be
`venv/Scripts/activate.bat`.) You should now have HTML documentation available
in `_build/`.

## Adding New Python Modules to the Documentation

This section is for developers of semtk-python3.

If you add a new module (Python file), you'll need to update the documentation
to include it like so:
```
sphinx-apidoc -Me -H "semtk-python3 API Reference" -d 0 -o api ../semtk3
```
