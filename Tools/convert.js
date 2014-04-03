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

    var stream = fs.createReadStream(file).pipe(geojson.parse());

    var headers = "X,Y,";

    stream.on('data', function(data){
        if (start) {
            headers = headers + Object.keys(data.properties).join(',');
            start = false;
            try {
                fs.mkdirSync(file.replace(".json",""));
            } catch(err) {
                console.log("  Folder Exists");
            }
            fs.writeFileSync(file.replace(".json","") + "/out.csv", headers + "\n");
            headers = headers.split(',');
        } else {
            var row = [];
            row[0] = data.geometry.coordinates[0];
            row[1] = data.geometry.coordinates[1];
            
            for (var elem in data.properties) {
                if (headers.indexOf(elem) != -1 && data.properties[elem]) {
                    var value = data.properties[elem].toString().replace(/\s*,\s*/g, ' ').replace(/(\r\n|\n|\r)/gm,"")
                    console.log(value);
                    row[headers.indexOf(elem)] = value;
                } else
                    row[headers.indexOf(elem)] = "";
            }
            fs.appendFileSync(file.replace(".json","") + "/out.csv", row + "\n");
            process.exit(0);
        }
    });

    stream.on('close', function(){
        callback();
    });
}
