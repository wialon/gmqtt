#!/usr/bin/env python
import sys
import gmqtt

from setuptools import setup, find_packages


extra = {}
if sys.version_info >= (3, 4):
    extra['use_2to3'] = False
    extra['convert_2to3_doctests'] = ['README.md']

CLASSIFIERS = [
    'Development Status :: 5 - Production/Stable',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: MIT License',
    'Natural Language :: English',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Topic :: Software Development :: Libraries :: Python Modules'
    ]

KEYWORDS = 'Gurtam MQTT client.'

setup(name='gmqtt',
      version=gmqtt.__version__,
      description="""Client for MQTT protocol""",
      long_description=open('README.md').read(),
      author=gmqtt.__author__,
      author_email=gmqtt.__email__,
      url='https://github.com/wialon/gmqtt',
      packages=find_packages(),
      download_url='https://github.com/wialon/gmqtt',
      classifiers=CLASSIFIERS,
      keywords=KEYWORDS,
      zip_safe=True,
      install_requires=[]
)
