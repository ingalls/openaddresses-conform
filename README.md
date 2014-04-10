openaddresses-conform
=====================

A utility to download the openaddresses cache and wrangle them into a standardized format. Work in progress.

## Installation

- Install [GDAL](http://www.gdal.org/).
- Run `npm install` in project directory

## Run

    node index.js <source-director> <working-directory>
    
Where

`<source-directory>` contains the directory of the openaddresses source files or a single openaddress source file ending in .json. 

`<working-directory>` is an empty directory to do processing


##Usage

Conform data can be found in the source files of the openaddresses project. Conform data is used to convert the data to a single standardized format.


`"conform"={}` The envelope for all conform data. All of the following properties are children to the `conform` object.

`type` The type properties stores the format. It can currently be either `shapefile`, `shapefile-polygon`, or `geojson`.

`merge=["one", "two", ..., "nth"]` The merge tag will merge several columns together. This is typically soemthing along the lines of street-name and street-type columns. The merged column will be named `auto_street`

`split` Some databases give the number and street in one column. The split tag will split the number and the street. The resulting columns will be `auto_street` and `auto_number`.

`lon` The longitude column. Due to the way the conversion scripts work this is currently always going to be `x`.

`lat` The latitude column. Due to the way the conversion script work this is currently always going to be `y`.

`number` The name of the number columm. This will either be the name of the column or `auto_number` if the split tool was used.

`street` The name of the street column. This will either be the name of the column or `auto_street` if the split or merge tools were used.

`file` The majority of zips contain a single shapefile. Sometimes zips will contain multiple files, or the shapefile that is needed is located in a folder
hierarchy in the zip. Since the program only supports determining single shapefiles not in a subfolder, file can be used to point the program to an exact file.
The proper syntax would be `"file": "addresspoints/address.shp"` if the file was under a single subdirectory called `addresspoints`. Note there is no preceding forward slash.

It should be noted that during the conversion, all column names are lowercased and stripped of newline characters and commas are converted to spaces.

The last tag is used for testing only and must not be pushed to the git repo as the conversion scripts will stop on its detection.

`"test": true` will stop the program as soon as the conversion to CSV has taken place. This is extremly useful as it allows the user to open the CSV in a texteditor or spreadsheet program to obtain the header names. After the header names are obtained, the `test` tag should be removed and the processing script run to ensure it runs properly.
