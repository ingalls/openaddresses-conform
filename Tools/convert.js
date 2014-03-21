exports.shp2csv = function shp2csv(dir, callback){
    var sh = require('execSync');
    var fs = require('fs');
    
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
