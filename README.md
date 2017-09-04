MONROE Analysis
===============

[![Build Status](https://travis-ci.org/biolab/monroe-anal.svg?branch=master)](https://travis-ci.org/biolab/monroe-anal)

A Python 3 utility library for querying, analyzing, and visualizing
[MONROE] data.

[MONROE]: https://www.monroe-project.eu/

Installation
------------

Run the following command line in the Python 3 environment of choice:

    pip install git+https://github.com/biolab/monroe-anal

As the project API is as of yet considered unstable, we recommend installing
in development mode:
    
    git clone https://github.com/biolab/monroe-anal
    cd monroe-anal
    pip install -e .
    
    # Run tests to confirm the package is installed correctly
    python setup.py test

Then issue `git pull` within _monroe-anal_ directory every now and
then. The alternative is to re-run above `pip install git+...` command
whenever updating is desired.

Usage
-----
For help on [how to query data], see the relevant Jupyter notebook.
In general, see Jupyter notebooks in the [_notebooks_ directory] for
usage examples.

[how to query data]: notebooks/data.ipynb
[_notebooks_ directory]: notebooks/
