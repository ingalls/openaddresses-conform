var fs = require('fs'),
    stream = require('stream'),
    transform = require('stream-transform'),
    parse = require('csv-parse'),
    stringify = require('csv-stringify');

exports.dropCol = function dropCol(source, cachedir, callback){
    var debug = require('debug')('conform:csv:dropCol');

    debug("Dropping Unnecessary Data");

    var keep = [source.conform.lon, source.conform.lat, source.conform.number, source.conform.street];
    var loc = cachedir + source.id + "/out.csv";

    if (fs.exists('./tmp.csv'))
        fs.unlinkSync('./tmp.csv');

    var instream = fs.createReadStream(loc);
    var outstream = fs.createWriteStream('./tmp.csv');

    var stringifier = stringify();
    var parser = parse({ relax: true });

    var keepCols = {
        lon: null,
        lat: null,
        str: null,
        num: null
    };

    var linenum = 0;

    var transformer = transform(function(data) {
        linenum++;

        if(linenum === 1) {
            keep = keep.map(function(x) { return x.toLowerCase(); });
            data = data.map(function(x) { return x.toLowerCase(); });
            for (var i = 0; i < data.length; i++){            
                if (data[i] === keep[0])
                    keepCols.lon = i;
                else if (data[i] === keep[1])
                    keepCols.lat = i;                    
                else if (data[i] === keep[2])
                    keepCols.num = i;                
                else if (data[i] === keep[3])
                    keepCols.str = i;
            }

            return ['LON','LAT','NUMBER','STREET'];
        }
        else {
            return [
                data[keepCols.lon],
                data[keepCols.lat],
                data[keepCols.num],
                data[keepCols.str]
            ];
        }        
    });    

    outstream.on('close', function() {
        fs.rename('./tmp.csv', loc, function(err) {
            callback(err);
        });
    });

    instream
        .pipe(parser)
        .pipe(transformer)
        .pipe(stringifier)
        .pipe(outstream);    
};

//Joins Columns in order that they are given in array into 'auto_street' column
exports.mergeStreetName = function mergeStreetName(source, cachedir, callback){
    var debug = require('debug')('conform:csv:mergeStreetName');
    debug("Merging Columns");

    var cols = source.conform.merge.slice(0);
    var loc = cachedir + source.id + "/out.csv";

    if (fs.exists('./tmp.csv'))
        fs.unlinkSync('./tmp.csv');

    var instream = fs.createReadStream(loc);
    var outstream = fs.createWriteStream('./tmp.csv');

    var stringifier = stringify();
    var parser = parse({ relax: true });
    parser.on('error', function(err) {
        debug(err);    
    });

    var linenum = 0;
    var mergeIndices = [];

    var transformer = transform(function(data) {
        linenum++;

        if (linenum === 1) {
            lowerData = data.map(function(x) { return x.toLowerCase(); } );            
            cols.forEach(function(name, i) {
                mergeIndices.push(lowerData.indexOf(name.toLowerCase()));
            });
            data.push('auto_street');
            return data;
        }
        else {
            var pieces = [];
            mergeIndices.forEach(function(index) {
                pieces.push(data[index]);
            });
            data.push(pieces.join(' '));
            return data;
        }
    });
    
    outstream.on('close', function() {
        fs.rename('./tmp.csv', loc, function(err){
            callback(err);
        });
    });

    instream
        .pipe(parser)
        .pipe(transformer)
        .pipe(stringifier)
        .pipe(outstream); 
};

// joins arbitrary number of columns into new ones
exports.advancedMerge = function mergeStreetName(source, cachedir, callback){
    var debug = require('debug')('conform:csv:advancedMerge');
    debug("Advanced-merging Columns");

    var loc = cachedir + source.id + "/out.csv";

    if (fs.exists('./tmp.csv'))
        fs.unlinkSync('./tmp.csv');

    var instream = fs.createReadStream(loc);
    var outstream = fs.createWriteStream('./tmp.csv');

    var stringifier = stringify();
    var parser = parse({ relax: true });
    parser.on('error', function(err) {
        debug(err);    
    });

    var linenum = 0;
    var merges = []; // list of tuples: [outField, separator, [fields], order]

    var transformer = transform(function(data) {
        linenum++;

        if (linenum === 1) {

            lowerData = data.map(function(x) { return x.toLowerCase(); } );    

            Object.keys(source.conform.advanced_merge).forEach(function(outField) {
                var newMerge = [
                    outField,                    
                    typeof source.conform.advanced_merge[outField].separator !== 'undefined' ? source.conform.advanced_merge[outField].separator : ' ',
                    []
                    //typeof source.conform.advanced_merge[outField].order !== 'undefined' ? parseInt(source.conform.advanced_merge[outField].order) : 0,
                ];

                source.conform.advanced_merge[outField].fields.forEach(function(inField) {
                    var foundIndex = lowerData.indexOf(inField.toLowerCase());
                    if (foundIndex > -1)
                        newMerge[2].push(foundIndex);
                });

                merges.push(newMerge);
            });

            /*
            // @TODO: enable sorting by optional 'order' field. 
            // This will allow fields to be built from 
            // each other -- but will require recursion.
            merges.sort(function(a, b) { 
                if (a[1] > b[1]) return 1;
                if (a[1] < b[1]) return -1;
                return 0;
            });
            */

            // push out headers in the order we arrived at
            merges.forEach(function(merge) {
                data.push(merge[0]);
            });

            return data;
        }
        else {
            merges.forEach(function(merge) {
                var pieces = [];
                merge[2].forEach(function(inFieldIndex) {
                    pieces.push(data[inFieldIndex]);
                });                
                data.push(pieces.join(merge[1]));
            });
            
            return data;
        } 
    });
    
    outstream.on('close', function() {
        fs.rename('./tmp.csv', loc, function(err){
            callback(err);
        });
    });

    instream
        .pipe(parser)
        .pipe(transformer)
        .pipe(stringifier)
        .pipe(outstream); 
};


// this function is @ingalls' code -- I haven't touched it because it's magic --@sbma44
// do substitutions 
function _expandElements(elements) {
    expand = require('./expand.json');

    elements[3] = elements[3].toLowerCase();
    elements[3] = elements[3].replace(/\./g,'');
    elements[2] = elements[2].trim();

    for (var i = 0; i < expand.abbr.length; i++) {
        var key = expand.abbr[i].k;
        var value = expand.abbr[i].v;
        var tokenized = elements[3].split(' ');

        for(var e = 0; e < tokenized.length; e++) {
            if (tokenized[e] == key)
            tokenized[e] = value;
        }

        elements[3] = tokenized.join(' ');
    }

    //Take care of the pesky st vs saint
    var tokenized = elements[3].split(' ');
    var length = tokenized.length;

    //Only converts to street if in the last half of the words
    for (var i = 0; length - i >= Math.floor(length/2); i++) {
        if (tokenized[length - i] === "st")
            tokenized[length - i] = "street";
    }
    
    //Only converts to saint if in the last half of the words
    for (var i = 0; i <= Math.ceil(length/2); i++) {
        if (tokenized[i] === "st")
            tokenized[i] = "saint";
    }

    elements[3] = tokenized.join(" ");
    elements[3] = elements[3].toLowerCase().replace( /(^|\s)([a-z])/g , function(m,p1,p2){ return p1+p2.toUpperCase(); } );
    elements[3] = elements[3].trim();

    return elements;
}

exports.expand = function expand(source, cachedir, callback) {
    var debug = require('debug')('conform:csv:expand');
           
    var loc = cachedir + source.id + "/out.csv";

    var instream = fs.createReadStream(loc);
    var outstream = fs.createWriteStream('./tmp.csv');
    
    var stringifier = stringify();
    var parser = parse({ relax: true });
    parser.on('error', function(err) {
        debug(err);    
    });

    var linenum = 0;

    var transformer = transform(function(data) {
        linenum++;

        if (linenum % 10000 === 0)
            debug('Processed Addresses: ' + linenum);

        return (linenum === 1) ? data : _expandElements(data);
    });

    outstream.on('close', function() {
        fs.rename('./tmp.csv', loc, function(err) {
            callback(err);
        });
    });

    instream
        .pipe(parser)
        .pipe(transformer)
        .pipe(stringifier)
        .pipe(outstream);        
};


//If the address is given as one field
//This will take a given number of fields as the number address (numFields)
//numFields defaults to 1
//and use the remainder as the street address
//Creates two new columns called auto_num & auto_str
exports.splitAddress = function splitAddress(source, cachedir, callback){
    var debug = require('debug')('conform:csv:splitAddress');

    debug('Splitting Address Column');

    var col = source.conform.split.toLowerCase();
    var numFields = 1;
    var loc = cachedir + source.id + "/out.csv";

    var instream = fs.createReadStream(loc);
    var outstream = fs.createWriteStream('./tmp.csv');

    var stringifier = stringify();
    var parser = parse({ relax: true });
    parser.on('error', function(err) {
        debug(err);    
    });

    var linenum = 0,
        elementToSplit, //Stores the element # to split
        length; //Stores start position of auto_num

    var transformer = transform(function(data) {
        linenum++;
        if (linenum === 1) {
            elementToSplit = data.map(function(x) { return x.toLowerCase() }).indexOf(col);

            length = data.length + 1;

            data.push('auto_number');
            data.push('auto_street');            

            return data;
        }
        else {
            if(data[elementToSplit]) {
                var token = data[elementToSplit].split(' ');
                var street = token[numFields];
                var number = token[0];
        
                for (var num = 1; num < numFields; num++){
                    number = number + " " + token[num];
                }
          
                for (var str = numFields+1; str < token.length; str++){
                    street = street + " " + token[str];
                }
          
                data[length - 1] = number;
                data[length] = street;
                return data;                
            }
        }
    });

    outstream.on('close', function() {
        fs.rename('./tmp.csv', loc, function(err) {
            callback(err);
        });        
    });

    instream
        .pipe(parser)
        .pipe(transformer)
        .pipe(stringifier)
        .pipe(outstream);
};

exports.reproject = function(source, cachedir, callback) {
    var sh = require('execSync');
    var debug = require('debug')('conform:csv:reproject');

    debug('reprojecting CSV from ' + source.conform.srs);

    var dir = cachedir + source.id + '/';

    // move input csv
    if(!fs.existsSync(dir + 'tmp'))
        fs.mkdirSync(dir + 'tmp');
    fs.renameSync(dir + 'out.csv', dir + 'tmp/out.csv');
    
    // write VRT
    var vrt = '<OGRVRTDataSource>\
    <OGRVRTLayer name="out">\
        <SrcDataSource>' + dir + 'tmp/out.csv</SrcDataSource>\
        <GeometryType>wkbPoint</GeometryType>\
        <LayerSRS>' + source.conform.srs + '</LayerSRS>\
        <GeometryField encoding="PointFromColumns" x="LON" y="LAT"/>\
    </OGRVRTLayer>\
</OGRVRTDataSource>'
    fs.writeFileSync(dir + 'out.vrt', vrt);

    // reproject
    sh.run('ogr2ogr -f CSV ' + dir + 'out.csv ' + dir + 'out.vrt -lco GEOMETRY=AS_XY -t_srs EPSG:4326 -overwrite');

    // clean up files
    fs.unlinkSync(dir + 'out.vrt');
    fs.unlinkSync(dir + 'tmp/out.csv');
    fs.rmdirSync(dir + 'tmp');

    // drop old columns
    var tmpSource = JSON.parse(JSON.stringify(source));
    tmpSource.conform.lon = 'X';
    tmpSource.conform.lat = 'Y';
    tmpSource.conform.number = 'NUMBER';
    tmpSource.conform.street = 'STREET';
    exports.dropCol(tmpSource, cachedir, callback);    
};
