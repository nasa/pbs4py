 [![pipeline status](https://gitlab.larc.nasa.gov/kejacob1/pbs4py/badges/main/pipeline.svg)](https://gitlab.larc.nasa.gov/kejacob1/pbs4py/-/commits/main)


# Description

A python module for scripting PBS jobs

# Set up

## Installation with setup.py

setup.py in the root directory is a setuptools script for installing pyrefine.
It is run with `python setup.py install`.
Typical command line arguments to this script are `--user` to install in ~/.local or `--prefix={path/to/install}`.

## In place usage of pyrefine using pip
From this root directory do `pip install -e .` to do a "developer install". This allows you to edit pbs4py without
having to reinstall to get new changes.

# Documentation
[LaRC gitlab page](https://sketch-to-solution.gitlab-pages.larc.nasa.gov/pyrefine/)


The pyrefine documentation is generated from the source code with Sphinx.
If you do not already have sphinx installed, you can use `pip` or `conda` to install it.
Once you have installed or added pyrefine to your PYTHONPATH, the documentation is built by running `make html` in the docs directory.
The generated documentation will be in `docs/build/html`.


# Quick Start

After installation,

```python
from pbs4py import PBS
pbs = PBS.k4()
pbs.launch('example_job',['echo "Hello World"'])
```
