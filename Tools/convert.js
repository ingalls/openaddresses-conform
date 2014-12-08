var fs = require('fs'),
    async = require('async'),
    transform = require('stream-transform'),
    parse = require('csv-parse'),
    stringify = require('csv-stringify'),
    expat = require('node-expat'),
    fileTypeExtensions = require('./filetype-extensions.json');

exports.polyshp2csv = function polyshp2csv(dir, shp, s_srs, callback) {
    var debug = require('debug')('conform:polyshp2csv');

    var sh = require('execSync'),
        geojson = require('geojson-stream');

    if (!shp) {
        debug('Detecting Shapefile');
        var tmp = fs.readdirSync(dir);
        var shp;

        for(var i = 0; i < tmp.length; i++){
            if (tmp[i].indexOf(".shp") != -1){
                shp = tmp[i];
                debug('Found: ' + shp);
                break;
            }
        }
    }

    debug('Converting ' + shp);
    if (s_srs)
        sh.run('ogr2ogr -s_srs ' + s_srs + ' -t_srs EPSG:4326 -f GeoJSON ' + dir + 'tmp.json ' + dir + shp + ' -lco GEOMETRY=AS_XYZ');
    else
        sh.run('ogr2ogr -t_srs EPSG:4326 -f GeoJSON ' + dir + 'tmp.json ' + dir + shp + ' -lco GEOMETRY=AS_XYZ');

    var start = true;
    var stream = fs.createReadStream(dir + 'tmp.json').pipe(geojson.parse());
    var headers = "X,Y,";

    stream.on('data', function(data){
        if (start) { //Headers
            headers = headers + Object.keys(data.properties).join(',');
            start = false;

            fs.writeFileSync(dir + "/out.csv", headers + "\n");
            headers = headers.split(',');
        } else { //Data
            var row = [];
            try {
                if (data.geometry.coordinates[0][0]) { //Handle Polygons
                    var center = function(arr) {
                        var minX, maxX, minY, maxY;
                        for (var i=0; i< arr.length; i++) {
                            minX = (arr[i][0] < minX || minX == null) ? arr[i][0] : minX;
                            maxX = (arr[i][0] > maxX || maxX == null) ? arr[i][0] : maxX;
                            minY = (arr[i][1] < minY || minY == null) ? arr[i][1] : minY;
                            maxY = (arr[i][1] > maxY || maxY == null) ? arr[i][1] : maxY;
                        }
                        return [(minX + maxX) /2, (minY + maxY) /2];
                    }

                    row = center(data.geometry.coordinates[0]);
                } else { //Handle Points
                    row[0] = data.geometry.coordinates[0];
                    row[1] = data.geometry.coordinates[1];
                }

                for (var elem in data.properties) {
                    if (headers.indexOf(elem) != -1 && data.properties[elem])
                        row[headers.indexOf(elem)] = data.properties[elem].toString().replace(/\s*,\s*/g, ' ').replace(/(\r\n|\n|\r)/gm,"");
                    else
                        row[headers.indexOf(elem)] = "";
                }
                fs.appendFileSync(dir + "/out.csv", row + "\n");
            } catch (err) {
                debug("Malformed data package");
            }
        }
    });

    stream.on('close', function(){
        fs.unlinkSync(dir + 'tmp.json');
        callback();
    });
}

exports.shp2csv = function shp2csv(dir, shp, s_srs, callback) {
    var sh = require('execSync');
    var debug = require('debug')('conform:shp2csv');

    if (!shp) {
        debug('Detecting Shapefile');        
        var tmp = fs.readdirSync(dir);
        var shp;

        for(var i = 0; i < tmp.length; i++){
            if (tmp[i].indexOf(".shp") != -1){
                shp = tmp[i];
                debug('  Found: ' + shp);
                break;
            }
        }
    }

    debug('Converting ' + shp);

    debug('directory: ' + dir);
    
    if (s_srs)
        sh.run('ogr2ogr -s_srs ' + s_srs + ' -t_srs EPSG:4326 -f CSV ' + dir + 'out.csv ' + dir + shp + ' -lco GEOMETRY=AS_XYZ');
    else
        sh.run('ogr2ogr -t_srs EPSG:4326 -f CSV ' + dir + 'out.csv ' + dir + shp + ' -lco GEOMETRY=AS_XYZ');

    callback();
}

exports.json2csv = function json2csv(file, callback) {
    var debug = require('debug')('conform:json2csv'),
        geojson = require('geojson-stream');

    var start = true;

    var dir = file.split("/"),
        sourceDir = file.replace(dir[dir.length-1],"");

    var stream = fs.createReadStream(file).pipe(geojson.parse());

    var headers = "X,Y,";

    stream.on('data', function(data){
        if (data.properties.x) delete data.properties.x;
        if (data.properties.y) delete data.properties.y;

        if (start) { //Headers
            headers = headers + Object.keys(data.properties).join(',');
            start = false;
            try {
                fs.mkdirSync(file.replace(".json",""));
            } catch(err) {
                debug("Folder Exists");
            }
            fs.writeFileSync(file.replace(".json","") + "/out.csv", headers + "\n");
            headers = headers.split(',');
        } else { //Data
            var row = [];
            try {
                if (data.geometry.coordinates[0][0]) { //Handle Polygons
                    var center = function(arr) {
                        var minX, maxX, minY, maxY;
                        for (var i=0; i< arr.length; i++) {
                            minX = (arr[i][0] < minX || minX == null) ? arr[i][0] : minX;
                            maxX = (arr[i][0] > maxX || maxX == null) ? arr[i][0] : maxX;
                            minY = (arr[i][1] < minY || minY == null) ? arr[i][1] : minY;
                            maxY = (arr[i][1] > maxY || maxY == null) ? arr[i][1] : maxY;
                        }
                        return [(minX + maxX) /2, (minY + maxY) /2];
                    }

                    row = center(data.geometry.coordinates[0]);
                } else { //Handle Points
                    row[0] = data.geometry.coordinates[0];
                    row[1] = data.geometry.coordinates[1];
                }

                for (var elem in data.properties) {
                    if (headers.indexOf(elem) != -1 && data.properties[elem])
                        row[headers.indexOf(elem)] = data.properties[elem].toString().replace(/\s*,\s*/g, ' ').replace(/(\r\n|\n|\r)/gm,"");
                    else
                        row[headers.indexOf(elem)] = "";
                }
                fs.appendFileSync(file.replace(".json","") + "/out.csv", row + "\n");
            } catch (err) {
                debug("Malformed data package");
            }
        }
    });

    stream.on('close', function(){
        callback();
    });
}

exports.csv = function(source, cachedir, callback) {    
    var debug = require('debug')('conform:csv');

    debug('Cleaning up CSV');

    if(!fs.existsSync(cachedir + source._id + '/')) fs.mkdirSync(cachedir + source._id);

    var filename = cachedir + source._id + '.' + fileTypeExtensions[source.conform.type]; // eg /tmp/openaddresses/us-va-arlington.csv
    var outFilename = cachedir + source._id + '/out.csv';
    
    var instream = fs.createReadStream(filename);
    var outstream = fs.createWriteStream('./tmp.csv');

    var stringifier = stringify();
    var parseropts = {relax: true};
    var numheaders = 0;
    if (source.conform.csvsplit) parseropts.delimiter = source.conform.csvsplit;
    var parser = parse(parseropts);
    parser.on('error', function(err) {
        debug(err);    
    });

    var linenum = 0;
    var mergeIndices = [];

    var transformer = transform(function(data) {
        linenum++;

        if (linenum === source.conform.headers) {            
            numheaders = data.length;
            return data;
        }
        else if(linenum > source.conform.skiplines) {            
            return data;
        }
        else
            return null;
    });
    


    outstream.on('close', function() {
        
        var finishUp = function(err) {
            fs.rename('./tmp.csv', outFilename, function(err){
                callback(err);
            });
        };

        // if noheaders was specified, build some damn headers
        if (source.conform._noheaders) {
            debug('adding headers to CSV');

            var headers = [];
            for(var i=1;i<=numheaders;i++)
                headers.push('COLUMN' + i);
            fs.writeFileSync('./tmp2.csv', headers.join(',') + '\n');
            var outstream2 = fs.createWriteStream('./tmp2.csv', { flags: 'a' });
            outstream2.on('close', function(err) {
                fs.rename('./tmp2.csv', './tmp.csv', finishUp);
            });
            fs.createReadStream('./tmp.csv').pipe(outstream2);
        }
        else
            finishUp(null);


    });

    instream
        .pipe(parser)
        .pipe(transformer)
        .pipe(stringifier)
        .pipe(outstream); 
}

exports.xml = function(source, cachedir, callback) {
    var sh = require('execSync');
    var debug = require('debug')('conform:convert:xml');

    debug('extracting CSV data from XML');

    if (source.conform.srs) 
        sh.run('ogr2ogr -s_srs ' + source.conform.srs + ' -t_srs EPSG:4326 -f CSV ' + cachedir + source.id + '/out.csv ' + cachedir + source.id + '.' + fileTypeExtensions[source.conform.type] + ' -lco GEOMETRY=AS_XYZ');
    else      
        sh.run('ogr2ogr -t_srs EPSG:4326 -f CSV ' + cachedir + source.id + '/out.csv ' + cachedir + source.id + '.' + fileTypeExtensions[source.conform.type] + ' -lco GEOMETRY=AS_XYZ');

    callback();
}



