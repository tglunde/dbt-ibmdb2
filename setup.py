#!/usr/bin/env python
from setuptools import find_packages
from setuptools import setup

package_name = "dbt-ibmdb2"
package_version = "0.0.1"
description = """The IBM DB2 adpter plugin for dbt (data build tool)"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author='Torsten Glunde',
    author_email='torsten@glunde.de',
    url='github.com/tglunde/dbt-ibmdb2',
    packages=find_packages(),
    package_data={
        'dbt': [
            'include/dbt-ibmdb2/dbt_project.yml',
            'include/dbt-ibmdb2/macros/*.sql',
        ]
    },
    install_requires=[
       'dbt-core>={}'.format('0.15.0')
    ]
)
