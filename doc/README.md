# Documentation

This documentation is built with
[Sphinx](https://www.sphinx-doc.org/en/master/index.html). To get started,
```
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
mkdir _build
sphinx-build -b html . _build
```
You should now have HTML documentation available in `_build/`.

To update the API documentation to include new modules:
```
sphinx-apidoc -Me -H "semtk-python3 API Reference" -d 0 -o api ../semtk3
```
