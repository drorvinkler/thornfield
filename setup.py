import setuptools
from setuptools import find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="thornfield",
    version="1.5.0",
    author="Dror A. Vinkler",
    description="Advanced caching in python",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/drorvinkler/thornfield",
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
    license='MIT',
)
