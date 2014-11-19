var fs = require('fs');
var async = require('async');

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
                console.log('  Found: ' + shp);
                break;
            }
        }
    }

    debug('Converting ' + shp);
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

exports.csv = function csv(source, cachedir, extension, callback) {
    var debug = require('debug')('conform:csv');

    var directory = cachedir + source.id; // eg /tmp/openaddresses/us-va-arlington
    filename = directory + '.' + extension; // eg /tmp/openaddresses/us-va-arlington.csv

    async.series([        
        function(cb) {
            fs.mkdir(directory, cb);
        },
        function(cb) {
            fs.unlink(directory + '/out.csv', cb);
        }
    ], function(err, results) {
        var stream = fs.createReadStream(filename);
        stream.on('close', callback);
        stream.pipe(fs.createWriteStream(directory + '/out.csv'));
    });
};

