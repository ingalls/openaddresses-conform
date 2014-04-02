exports.shp2csv = function shp2csv(dir, callback) {
    var sh = require('execSync'),
        fs = require('fs');
    
    console.log('  Detecting Shapefile');
    var tmp = fs.readdirSync(dir);
    var shp;
    
    for(var i = 0; i < tmp.length; i++){
        if (tmp[i].indexOf(".shp") != -1){
            shp = tmp[i];
            console.log('  Found: ' + shp);
            break;
        }
    }
    
    console.log('  Converting ' + shp);
    sh.run('ogr2ogr -t_srs EPSG:4326 -f CSV ' + dir + 'out.csv ' + dir + shp + ' -lco GEOMETRY=AS_XYZ');
    
    callback();
}

exports.json2csv = function json2csv(file, callback) {
    var geojson = require('geojson-stream'),
        fs = require('fs');

    var start = true;

    var dir = file.split("/"),
        sourceDir = file.replace(dir[dir.length-1],"");

    //fs.writeFileSync(sourceDir + "out.csv", ,

    var stream = fs.createReadStream(file).pipe(geojson.parse());

    var headers = "X, Y, ";

    stream.on('data', function(data){
        
        if (start) {
            headers = headers + Object.keys(data.properties).join(', ');
            start = false;
            console.log(headers);
        } else {
            for (var elem in data.property) {
                process.exit(0);
            }
        }
    });
}
