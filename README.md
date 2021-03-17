# semtk-python3
This package contains python3 clients to the SemTK services.
Tested on Python 3.7.5.  May be incompatible with Python 3.7.6+

## Documentation

API Documentation is auto-generated and available online at:
[readthedocs.io](https://semtk-python3.readthedocs.io/en/latest/api/semtk3.html)

For more info, see [./doc](./doc/README.md).

## pip install

Shown with a sample commit tag.  You'll want to choose the latest or one that works for you.
```
pip install git+https://github.com/ge-semtk/semtk-python3@d44a45941d06ec6466bbe61bfd81cd667d6a5137
```

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
## Sample usage

For sample usage, see demo.py
