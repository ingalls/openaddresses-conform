openaddresses-conform
=====================

![TravisCI status](https://travis-ci.org/openaddresses/openaddresses-conform.svg)

A utility to download the openaddresses cache and wrangle them into a standardized format. Work in progress.

## Installation

- Install [GDAL](http://www.gdal.org/).
- Run `npm install` in project directory

## Run

    node index.js <source-director> <working-directory> <options>
                    <source.json>   <working-directory> <options>

Where

`<source-directory>` contains the directory of the openaddresses source files

`<source.json>` is a single openaddresses source file.

`<options>` If AWS credentials are found automatically uploads to this named s3 bucket.

`<working-directory>` is an empty directory to do processing


##Usage

Conform data can be found in the source files of the openaddresses project. Conform data is used to convert the data to a single standardized format.

In order to prevent fragmention accross processing libraries, all documentation about `conform` objects can be found in the main [CONTRIBUTING.md](https://github.com/openaddresses/openaddresses/blob/master/CONTRIBUTING.md) doc

