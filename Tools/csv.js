var fs = require('fs'),
    stream = require('stream'),
    transform = require('stream-transform'),
    parse = require('csv-parse'),
    stringify = require('csv-stringify');

exports.dropCol = function dropCol(keep, loc, callback){
    var debug = require('debug')('conform:dropCol');

    debug("Dropping Unnecessary Data");

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
exports.mergeStreetName = function mergeStreetName(cols, loc, callback){
    var debug = require('debug')('conform:mergeStreetName');
    debug("Merging Columns");

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

exports.expand = function expand(loc, callback) {
    var debug = require('debug')('conform:expand');
            
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
exports.splitAddress = function splitAddress(col, numFields, loc, callback){
    var debug = require('debug')('conform:splitAddress');

    debug('Splitting Address Column');

    col = col.toLowerCase();

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

