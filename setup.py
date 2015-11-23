try:
  from setuptools import setup
except ImportError:
  from distutils.core import setup

MODULE_NAME = "sodatap"

sdict = {}

execfile(MODULE_NAME + "/version.py", {}, sdict)

def findRequirements():
  """
  Read the requirements.txt file and parse into requirements for setup"s
  install_requirements option.
  """
  return [
    line.strip()
    for line in open("requirements.txt").readlines()
    if not line.startswith("#")
  ]

sdict.update({
    "name" : MODULE_NAME,
    "description" : "Socrata data viewer.",
    "url": "http://github.com/rhyolight/" + MODULE_NAME,
    "author" : "Matthew Taylor",
    "author_email" : "rhyolight@gmail.com",
    "keywords" : ["soda", "socrata", "big data", "data"],
    "license" : "MIT",
    "install_requires": findRequirements(),
    # "test_suite": "tests.unit",
    "packages" : [MODULE_NAME],
    "classifiers" : [
        "Development Status :: 5 - Production/Stable",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python"],
})

setup(**sdict)