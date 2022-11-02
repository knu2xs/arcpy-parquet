from setuptools import find_packages, setup

with open('README.md', 'r') as readme:
    long_description = readme.read()

setup(
    name='arcpy_parquet',
    package_dir={"": "src"},
    packages=find_packages('src'),
    version='0.0.0',
    description='Tooling to import and export from and to Parquet.',
    long_description=long_description,
    author='Joel McCune',
    license='Apache 2.0',
)
