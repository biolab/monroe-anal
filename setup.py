#!/usr/bin/env python

from setuptools import setup, find_packages


VERSION = '0.1.0'


if __name__ == '__main__':

    setup(
        name="monroe-anal",
        description="Utility library for querying, analyzing, and visualizing MONROE data",
        version=VERSION,
        author='UL FRI, Biolab',
        url='https://github.com/biolab/monroe-anal',
        keywords=(
        ),
        test_suite='monroe_anal.tests',
        packages=find_packages(),
        package_data={
        },
        include_package_data=True,
        setup_requires=[
            'setuptools_git >= 0.3',
        ],
        install_requires=[
            'numpy',
            'pandas >= 0.19.0',
            'ipython',
            'influxdb',
        ],
        entry_points={
            'orange3.addon': ('monroe = monroe_anal',),
            'orange.widgets': ('MONROE = monroe_anal.orange_widgets',),
        },
        classifiers=[
            'Framework :: IPython',
            'Framework :: Jupyter',
            'Programming Language :: Python :: 3 :: Only',
            'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
            'Operating System :: OS Independent',
            'Topic :: Scientific/Engineering :: Visualization',
            'Topic :: Scientific/Engineering :: Information Analysis',
            'Topic :: Scientific/Engineering :: Artificial Intelligence',
            'Intended Audience :: Science/Research',
        ],
        zip_safe=False,
    )
