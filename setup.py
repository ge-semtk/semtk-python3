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
import setuptools

#with open("README.md", "r") as fh:
#    long_description = fh.read()

setuptools.setup(
    name="semtk-python3", # Replace with your own username
    version="0.0.1",
    author="Paul Cuddihy",
    author_email="cuddihy@ge.com",
    description="Python3 client for semtk",
    #long_description=long_description,
    #long_description_content_type="text/markdown",
    url="https://github.build.ge.com/semtk",
    packages=setuptools.find_packages(),
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        #"License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        'requests',
        'python-dateutil'
    ],
    python_requires='>=3.6',
     entry_points={
        'console_scripts': [
            'semtk = semtk3:main',
        ]
    }
)
